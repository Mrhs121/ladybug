//! A native CSR leaf and an Iceberg scan in one typed DataFusion plan.

use std::any::Any;
use std::ffi::{CStr, CString, c_char, c_void};
use std::fmt;
use std::ptr;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use arrow::array::{Array, Int64Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ffi::FFI_ArrowArray;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, DataFusionError, Result as DfResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{JoinType, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, collect, displayable,
};
use datafusion::prelude::{Expr, SessionConfig, SessionContext, lit};
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::table::StaticTable;
use iceberg_datafusion::IcebergStaticTableProvider;

type EmitKeys = extern "C" fn(*mut c_void, *const i64, usize);
type ScanKeys = extern "C" fn(*mut c_void, *mut c_void, EmitKeys) -> bool;
type EmitBatch = extern "C" fn(*mut c_void, *mut FFI_ArrowArray) -> bool;

// Mirrors DfQuery in datafusion_plan.cpp. All pointers are borrowed for one synchronous call.
#[repr(C)]
pub struct DfQuery {
    metadata: *const c_char,
    key: *const c_char,
    endpoint: *const c_char,
    filter_column: *const c_char,
    projection: *const *const c_char,
    columns: usize,
    anchor: i64,
    threshold: i64,
    limit: u64,
    ascending: bool,
    forward: bool,
}

#[derive(Debug, Clone)]
struct NativeCsr {
    properties: Arc<PlanProperties>,
    // The callback's generation is pinned by C++ until this query-local runtime is dropped.
    context: usize,
    scan: ScanKeys,
    forward: bool,
}

impl DisplayAs for NativeCsr {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LbugCsrExpandExec: direction={}, locator=lake_key",
            if self.forward { "FWD" } else { "BWD" }
        )
    }
}

impl ExecutionPlan for NativeCsr {
    fn name(&self) -> &str {
        "LbugCsrExpandExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan("CSR expansion is a leaf".into()));
        }
        Ok(self)
    }
    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution("Invalid CSR partition".into()));
        }
        extern "C" fn append(out: *mut c_void, keys: *const i64, count: usize) {
            let keys = unsafe { std::slice::from_raw_parts(keys, count) };
            unsafe { &mut *out.cast::<Vec<i64>>() }.extend_from_slice(keys);
        }
        // One bound vertex per query. Materialize its degree-many keys for this join.
        let mut keys: Vec<i64> = vec![];
        if !(self.scan)(
            self.context as *mut _,
            ptr::from_mut(&mut keys).cast(),
            append,
        ) {
            return Err(DataFusionError::Execution(
                "Native lbug CSR scan failed".into(),
            ));
        }
        if std::env::var_os("LBUG_ICEBERG_DF_TRACE").is_some() {
            eprintln!("[iceberg_df] CSR candidate edges={}", keys.len());
        }
        let schema = ExecutionPlan::schema(self);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(keys))])?;
        Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
    }
}

#[async_trait]
impl TableProvider for NativeCsr {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        ExecutionPlan::schema(self)
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        _: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if projection.is_some_and(|p| p.as_slice() != [0]) {
            return Err(DataFusionError::Plan(
                "CSR edge-key projection required".into(),
            ));
        }
        Ok(Arc::new(self.clone()))
    }
}

async fn execute(
    query: &DfQuery,
    context: *mut c_void,
    scan: ScanKeys,
    emit: EmitBatch,
) -> Result<()> {
    let string = |p| unsafe { CStr::from_ptr(p) }.to_str();
    let metadata = string(query.metadata)?;
    let table = StaticTable::from_metadata_file(
        metadata,
        TableIdent::from_strs(["mapped", "edges"])?,
        FileIO::new_with_fs(),
    )
    .await?
    .into_table();
    let snapshot = table.metadata().current_snapshot_id();
    let lake = IcebergStaticTableProvider::try_new_from_table(table).await?;
    let session = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    session.register_table("lake", Arc::new(lake))?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "edge_key",
        DataType::Int64,
        false,
    )]));
    let properties = PlanProperties::new(
        EquivalenceProperties::new(schema),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    );
    session.register_table(
        "csr",
        Arc::new(NativeCsr {
            properties: Arc::new(properties),
            context: context as usize,
            scan,
            forward: query.forward,
        }),
    )?;

    let column = |name: &str| Expr::Column(Column::new(Some("lake"), name));
    let key = column(string(query.key)?);
    let mut edges = session
        .table("lake")
        .await?
        .filter(column(string(query.endpoint)?).eq(lit(query.anchor)))?;
    if !query.filter_column.is_null() {
        edges = edges.filter(column(string(query.filter_column)?).lt(lit(query.threshold)))?;
    }
    let candidates = session.table("csr").await?;
    let join_key = Expr::Column(Column::new(Some("csr"), "edge_key"));
    let mut result = edges
        .join_on(candidates, JoinType::Inner, [key.clone().eq(join_key)])?
        .sort(vec![key.sort(query.ascending, false)])?;
    if query.limit != u64::MAX {
        result = result.limit(0, Some(usize::try_from(query.limit)?))?;
    }
    let names = unsafe { std::slice::from_raw_parts(query.projection, query.columns) };
    let projection = names
        .iter()
        .map(|p| string(*p).map(column))
        .collect::<Result<Vec<_>, _>>()?;
    let result = result.select(projection)?;
    ensure!(
        result
            .schema()
            .fields()
            .iter()
            .all(|f| f.data_type() == &DataType::Int64),
        "Mapped result must be INT64"
    );
    let trace = std::env::var_os("LBUG_ICEBERG_DF_TRACE").is_some();
    if trace {
        eprintln!(
            "[iceberg_df] snapshot={snapshot:?} metadata={metadata}\n{}",
            result.logical_plan().display_indent()
        );
    }
    let physical = result.create_physical_plan().await?;
    if trace {
        eprintln!(
            "[iceberg_df] physical\n{}",
            displayable(physical.as_ref()).indent(false)
        );
    }
    for batch in collect(physical, session.task_ctx()).await? {
        if batch.num_rows() == 0 {
            continue;
        }
        let mut array = FFI_ArrowArray::new(&StructArray::from(batch).to_data());
        if !emit(context, &mut array) {
            bail!("Could not hand Arrow batch to lbug");
        }
    }
    Ok(())
}

/// # Safety
/// The caller keeps the query strings and callback context alive until return. Callbacks do not
/// unwind. The output callback takes ownership of an Arrow array by clearing its release field.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iceberg_df_query(
    query: &DfQuery,
    context: *mut c_void,
    scan: ScanKeys,
    emit: EmitBatch,
) -> *mut c_char {
    let run = || -> Result<()> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(execute(query, context, scan, emit))
    };
    let error = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
        Ok(Ok(())) => return ptr::null_mut(),
        Ok(Err(e)) => format!("{e:#}"),
        Err(_) => "Panic executing DataFusion mapping".to_owned(),
    };
    CString::new(error.replace('\0', " ")).unwrap().into_raw()
}

/// # Safety
/// Accepts a non-null error pointer returned by iceberg_df_query, exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn iceberg_df_free_error(error: *mut c_char) {
    drop(unsafe { CString::from_raw(error) });
}
