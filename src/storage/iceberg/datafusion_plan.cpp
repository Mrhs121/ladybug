// Translate a bound, directed one-hop mapping query without parsing or generating SQL.
#include <cstring>
#include <set>

#include "binder/expression/literal_expression.h"
#include "binder/expression/scalar_function_expression.h"
#include "binder/query/bound_regular_query.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/arrow/arrow.h"
#include "expression_evaluator/expression_evaluator.h"
#include "extension/planner_extension.h"
#include "main/client_context.h"
#include "main/database_manager.h"
#include "native_csr.h"
#include "planner/operator/logical_table_function_call.h"
#include "processor/expression_mapper.h"
#include "storage/storage_manager.h"

namespace lbug::iceberg {
using namespace lbug::binder;
using namespace lbug::catalog;
using namespace lbug::common;
using namespace lbug::function;
using namespace lbug::main;
using namespace lbug::planner;
using lbug::transaction::Transaction;

extern "C" {
struct DfQuery {
    const char *metadata, *key, *endpoint, *filterColumn;
    const char* const* projection;
    size_t columns;
    int64_t anchor, threshold;
    uint64_t limit;
    bool ascending, forward;
};
using EmitKeys = void (*)(void*, const int64_t*, size_t);
char* iceberg_df_query(const DfQuery*, void*, bool (*scan)(void*, void*, EmitKeys),
    bool (*output)(void*, ArrowArray*));
void iceberg_df_free_error(char*);
}

struct MappedPlan final : TableFuncBindData {
    MappedPlan(const RelGroupCatalogEntry& rel, const NodeTableCatalogEntry& src,
        const NodeTableCatalogEntry& dst)
        : relID{rel.getRelEntryInfos()[0].oid}, edge{rel.getStorage()},
          from{src.getStorage(), src.getPrimaryKeyName()},
          to{dst.getStorage(), dst.getPrimaryKeyName()} {}
    bool stable = false;
    table_id_t relID;
    std::string dbName;
    EdgeMapping edge;
    Source from, to;
    std::shared_ptr<Expression> anchor, threshold;
    std::string filterColumn;
    std::vector<std::string> projection;
    uint64_t limit = UINT64_MAX;
    bool ascending = true, forward = true;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<MappedPlan>(*this);
    }
    std::string getDescription() const override {
        return std::string{"DATAFUSION_ICEBERG_MATCH (native CSR "} + (forward ? "FWD" : "BWD") +
               ", " + edge.source.table + ")";
    }
};

static const PropertyExpression* intProperty(const std::shared_ptr<Expression>& expr) {
    if (expr->expressionType != ExpressionType::PROPERTY ||
        expr->getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        return nullptr;
    }
    const auto& p = expr->constCast<PropertyExpression>();
    return p.isSingleLabel() && !p.isInternalID() ? &p : nullptr;
}

// Python and other clients can bind a narrower integer; the Cypher binder inserts this cast.
static bool isIntArgument(const Expression& expr) {
    if (expr.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
        return false;
    }
    if (expr.expressionType == ExpressionType::LITERAL ||
        expr.expressionType == ExpressionType::PARAMETER) {
        return true;
    }
    if (expr.expressionType != ExpressionType::FUNCTION || expr.getNumChildren() != 1 ||
        !expr.constCast<ScalarFunctionExpression>().isCast()) {
        return false;
    }
    const auto child = expr.getChild(0);
    auto type = child->getDataType().getLogicalTypeID();
    return child->expressionType == ExpressionType::PARAMETER &&
           (type == LogicalTypeID::INT8 || type == LogicalTypeID::INT16 ||
               type == LogicalTypeID::INT32);
}

static bool bindPredicate(MappedPlan& plan, const RelExpression& rel, const Expression& pred) {
    if (pred.getNumChildren() != 2) {
        return false;
    }
    auto lhs = pred.getChild(0), rhs = pred.getChild(1);
    if (pred.expressionType == ExpressionType::EQUALS && !intProperty(lhs)) {
        std::swap(lhs, rhs);
    }
    auto prop = intProperty(lhs);
    if (!prop || !isIntArgument(*rhs)) {
        return false;
    }
    if (prop->getVariableName() == rel.getUniqueName() &&
        pred.expressionType == ExpressionType::LESS_THAN && !plan.threshold) {
        plan.filterColumn = prop->getPropertyName();
        plan.threshold = rhs;
        return true;
    }
    if (plan.anchor || pred.expressionType != ExpressionType::EQUALS || !prop->isPrimaryKey()) {
        return false;
    }
    plan.forward = prop->getVariableName() == rel.getSrcNodeName();
    if (!plan.forward && prop->getVariableName() != rel.getDstNodeName()) {
        return false;
    }
    plan.anchor = rhs;
    return true;
}

static std::unique_ptr<MappedPlan> translate(const BoundRegularQuery& query,
    ClientContext& context) {
    if (query.getNumSingleQueries() != 1 || query.getSingleQuery(0)->getNumQueryParts() != 1) {
        return nullptr;
    }
    const auto* part = query.getSingleQuery(0)->getQueryPart(0);
    if (part->getNumReadingClause() != 1 || part->hasUpdatingClause() ||
        !part->hasProjectionBody() || part->hasProjectionBodyPredicate() ||
        part->getReadingClause(0)->getClauseType() != ClauseType::MATCH) {
        return nullptr;
    }
    const auto& match = part->getReadingClause(0)->constCast<BoundMatchClause>();
    const auto* graphs = match.getQueryGraphCollection();
    if (match.hasHint() || match.getMatchClauseType() != MatchClauseType::MATCH ||
        graphs->getNumQueryGraphs() != 1) {
        return nullptr;
    }
    const auto* graph = graphs->getQueryGraph(0);
    if (graph->getNumQueryNodes() != 2 || graph->getNumQueryRels() != 1) {
        return nullptr;
    }
    const auto rel = graph->getQueryRel(0);
    if (rel->isRecursive() || rel->getDirectionType() != RelDirectionType::SINGLE ||
        rel->getNumEntries() != 1 || rel->getSrcNode()->getNumEntries() != 1 ||
        rel->getDstNode()->getNumEntries() != 1) {
        return nullptr;
    }
    const auto& r = rel->getEntry(0)->constCast<RelGroupCatalogEntry>();
    const auto& src = rel->getSrcNode()->getEntry(0)->constCast<NodeTableCatalogEntry>();
    const auto& dst = rel->getDstNode()->getEntry(0)->constCast<NodeTableCatalogEntry>();
    if ((r.getStorageFormatName() != "iceberg_csr" &&
            r.getStorageFormatName() != "iceberg_stable_csr") ||
        src.getStorageFormatName() != r.getStorageFormatName() ||
        dst.getStorageFormatName() != r.getStorageFormatName() ||
        r.getRelEntryInfos().size() != 1) {
        return nullptr;
    }
    const auto& pair = r.getRelEntryInfos()[0].nodePair;
    const auto dbName = rel->getDbName(rel->getEntry(0));
    auto* catalog =
        DatabaseManager::resolveTableStorage(context, r.getRelEntryInfos()[0].oid, dbName).first;
    auto* txn = Transaction::Get(context);
    if (pair.srcTableID != src.getTableID() || pair.dstTableID != dst.getTableID() ||
        rel->getSrcNode()->getDbName(rel->getSrcNode()->getEntry(0)) != dbName ||
        rel->getDstNode()->getDbName(rel->getDstNode()->getEntry(0)) != dbName ||
        !catalog->containsTable(txn, r.getName()) ||
        catalog->getTableCatalogEntry(txn, r.getName()) != &r) {
        return nullptr;
    }

    auto plan = std::make_unique<MappedPlan>(r, src, dst);
    plan->dbName = dbName;
    plan->stable = r.getStorageFormatName() == "iceberg_stable_csr";
    for (const auto& pred : match.getConjunctivePredicates()) {
        if (!bindPredicate(*plan, *rel, *pred)) {
            return nullptr;
        }
    }
    const auto* body = part->getProjectionBody();
    if (!plan->anchor || body->isDistinct() || body->hasAggregateExpressions() || body->hasSkip() ||
        body->getOrderByExpressions().size() != 1) {
        return nullptr;
    }
    auto order = intProperty(body->getOrderByExpressions()[0]);
    if (!order || order->getVariableName() != rel->getUniqueName() ||
        order->getPropertyName() != plan->edge.source.key) {
        return nullptr;
    }
    plan->ascending = body->getSortingOrders()[0];
    if (body->hasLimit()) {
        auto limit = body->getLimitNumber();
        if (limit->expressionType != ExpressionType::LITERAL ||
            limit->getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
            return nullptr;
        }
        const auto& value = limit->constCast<LiteralExpression>().value;
        if (value.isNull() || value.getValue<int64_t>() < 0) {
            return nullptr;
        }
        plan->limit = static_cast<uint64_t>(value.getValue<int64_t>());
    }
    std::set<std::string> projected;
    for (const auto& expr : body->getProjectionExpressions()) {
        const auto* p = intProperty(expr);
        if (!p || p->getVariableName() != rel->getUniqueName() ||
            !projected.insert(p->getPropertyName()).second) {
            return nullptr;
        }
        plan->projection.push_back(p->getPropertyName());
        plan->columns.push_back(expr);
    }
    return plan->columns.empty() ? nullptr : std::move(plan);
}

struct QueryOutput final : TableFuncLocalState {
    std::vector<ArrowArrayWrapper> batches;
    size_t batch = 0, offset = 0;
};

struct ScanContext {
    std::shared_ptr<NativeGeneration> generation;
    size_t slot;
    RelDataDirection direction;
    QueryOutput& output;
    std::exception_ptr error;
};

static bool scanKeys(void* raw, void* consumer, EmitKeys emit) noexcept {
    auto& scan = *static_cast<ScanContext*>(raw);
    try {
        NativeCursor cursor(scan.generation, "lake_key");
        cursor.seek(scan.slot, scan.direction);
        std::vector<int64_t> keys;
        while (cursor.readChunk([&](auto, int64_t key) { keys.push_back(key); })) {
            if (!keys.empty()) {
                emit(consumer, keys.data(), keys.size());
            }
            keys.clear();
        }
        return true;
    } catch (...) {
        scan.error = std::current_exception();
        return false;
    }
}

static bool receiveBatch(void* raw, ArrowArray* batch) noexcept {
    auto& scan = *static_cast<ScanContext*>(raw);
    try {
        auto& out = scan.output.batches.emplace_back();
        static_cast<ArrowArray&>(out) = *batch;
        batch->release = nullptr;
        return true;
    } catch (...) {
        scan.error = std::current_exception();
        return false;
    }
}

static std::optional<int64_t> argument(const std::shared_ptr<Expression>& expr,
    ClientContext* context) {
    Schema emptySchema;
    lbug::processor::ExpressionMapper mapper(&emptySchema);
    auto evaluator = mapper.getEvaluator(expr);
    lbug::processor::ResultSet emptyResult(0);
    evaluator->init(emptyResult, context);
    evaluator->evaluate();
    const auto& vec = *evaluator->resultVector;
    auto pos = vec.state->getSelVector()[0];
    return vec.isNull(pos) ? std::nullopt : std::optional{vec.getValue<int64_t>(pos)};
}

static std::unique_ptr<TableFuncLocalState> runPlan(const TableFuncInitLocalStateInput& input) {
    const auto& p = input.bindData.cast<MappedPlan>();
    auto output = std::make_unique<QueryOutput>();
    auto anchor = argument(p.anchor, input.clientContext);
    auto threshold =
        p.threshold ? argument(p.threshold, input.clientContext) : std::optional<int64_t>{0};
    if (!anchor || !threshold || p.limit == 0) {
        return output;
    }
    auto& lake = LakeRead::get(Transaction::Get(*input.clientContext));
    auto* storage =
        DatabaseManager::resolveTableStorage(*input.clientContext, p.relID, p.dbName).second;
    auto store = NativeStore::forDatabase(storage->getDatabasePath());
    std::shared_ptr<NativeGeneration> generation;
    std::optional<size_t> slot;
    auto& node = p.forward ? p.from : p.to;
    if (p.stable) {
        generation = lake.stableGeneration(p.relID, p.edge, p.from, p.to, *store);
        slot = lake.stableNodes(node, *store)->slot(*anchor);
    } else {
        slot = lake.nodeOffset(node, *anchor, store.get());
        if (slot) {
            generation = lake.nativeGeneration(p.relID, p.edge, p.from, p.to, *store);
        }
    }
    if (!slot) {
        return output;
    }
    ScanContext scan{std::move(generation), *slot,
        p.forward ? RelDataDirection::FWD : RelDataDirection::BWD, *output, {}};
    std::vector<const char*> columns;
    for (const auto& col : p.projection) {
        columns.push_back(col.c_str());
    }
    const DfQuery query{lake.metadataFile(p.edge.source).c_str(), p.edge.source.key.c_str(),
        (p.forward ? p.edge.from : p.edge.to).c_str(),
        p.threshold ? p.filterColumn.c_str() : nullptr, columns.data(), columns.size(), *anchor,
        *threshold, p.limit, p.ascending, p.forward};
    std::unique_ptr<char, decltype(&iceberg_df_free_error)> error(
        iceberg_df_query(&query, &scan, scanKeys, receiveBatch), iceberg_df_free_error);
    if (scan.error) {
        std::rethrow_exception(scan.error);
    }
    if (error) {
        throw std::runtime_error(error.get());
    }
    return output;
}

static offset_t nextBatch(const TableFuncInput& input, TableFuncOutput& out) {
    auto& state = *input.localState->ptrCast<QueryOutput>();
    if (state.batch == state.batches.size()) {
        return 0;
    }
    const auto& batch = state.batches[state.batch];
    auto count = std::min<size_t>(DEFAULT_VECTOR_CAPACITY, batch.length - state.offset);
    for (int64_t column = 0; column < batch.n_children; ++column) {
        auto& vec = out.dataChunk.getValueVectorMutable(column);
        const auto& child = *batch.children[column];
        size_t start = state.offset + batch.offset + child.offset;
        auto* values = static_cast<const int64_t*>(child.buffers[1]);
        std::memcpy(vec.getData(), values + start, count * sizeof(int64_t));
        auto* valid = static_cast<const uint8_t*>(child.buffers[0]);
        vec.setAllNonNull();
        if (valid) {
            for (size_t i = 0; i < count; ++i) {
                vec.setNull(i, (valid[(start + i) / 8] & (1u << ((start + i) % 8))) == 0);
            }
        }
    }
    state.offset += count;
    if (state.offset == static_cast<size_t>(batch.length)) {
        ++state.batch;
        state.offset = 0;
    }
    return count;
}

class DataFusionPlanner final : public lbug::extension::QueryPlannerExtension {
    std::shared_ptr<LogicalOperator> tryPlanQuery(const BoundRegularQuery& query,
        ClientContext& context) override {
        if (!context.getCurrentSetting("iceberg_poc_datafusion").getValue<bool>()) {
            return nullptr;
        }
        auto plan = translate(query, context);
        if (!plan) {
            return nullptr;
        }
        TableFunction scan("DATAFUSION_ICEBERG_MATCH", {});
        scan.canParallelFunc = [] { return false; };
        scan.initSharedStateFunc = TableFunction::initEmptySharedState;
        scan.initLocalStateFunc = runPlan;
        scan.tableFunc = nextBatch;
        return std::make_shared<LogicalTableFunctionCall>(std::move(scan), std::move(plan));
    }
};

void registerDataFusionPlanner(Database& db) {
    db.addExtensionOption("iceberg_poc_datafusion", LogicalTypeID::BOOL, Value(true));
    db.addPlannerExtension(std::make_unique<DataFusionPlanner>());
}
} // namespace lbug::iceberg
