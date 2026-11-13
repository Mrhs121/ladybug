#include "lake.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

#include "duckdb.hpp"
#include "main/client_context.h"
#include "native_csr.h"
#include <sqlite3.h>

namespace lbug::iceberg {
using lbug::common::LogicalType;
using lbug::common::Value;
using lbug::transaction::Transaction;

EdgeMapping::EdgeMapping(const std::string& storage) {
    std::istringstream input(storage);
    std::string key, src, dst, extra;
    if (!(input >> source.table >> key >> source.key >> src >> from >> dst >> to) || key != "KEY" ||
        src != "FROM" || dst != "TO" || input >> extra) {
        throw std::runtime_error(
            "Expected storage = 'namespace.table KEY edge_id FROM src TO dst'");
    }
}

static std::string sqlQuote(const std::string& text, char quote) {
    std::string result(1, quote);
    for (char c : text) {
        result += c;
        if (c == quote) {
            result += c;
        }
    }
    return result + quote;
}

using DuckResult = decltype(std::declval<duckdb::Connection&>().Query(std::declval<std::string>()));

struct LakeRows {
    explicit LakeRows(DuckResult result)
        : result{std::move(result)}, columns{this->result->Collection()}, rows{columns} {}

    duckdb::Value getValue(size_t column, size_t row) const { return rows.GetValue(column, row); }

    DuckResult result;
    duckdb::ColumnDataCollection& columns;
    duckdb::ColumnDataRowCollection rows;
};

struct LakeRead::Reader {
    static duckdb::DuckDB openDatabase(int32_t scanThreads) {
        duckdb::DBConfig config;
        config.SetOptionByName("threads", duckdb::Value::BIGINT(scanThreads));
        return duckdb::DuckDB(nullptr, &config);
    }

    duckdb::DuckDB db;
    duckdb::Connection sql{db};
    std::map<std::string, std::string> metadata;

    Reader(const std::string& catalog, int32_t scanThreads) : db{openDatabase(scanThreads)} {
        // One read-only SQLite SELECT pins all metadata locations at the same catalog read.
        sqlite3* raw = nullptr;
        auto rc = sqlite3_open_v2(catalog.c_str(), &raw, SQLITE_OPEN_READONLY, nullptr);
        std::unique_ptr<sqlite3, decltype(&sqlite3_close)> handle(raw, sqlite3_close);
        if (rc != SQLITE_OK) {
            throw std::runtime_error(
                "Opening Iceberg SQL catalog: " + std::string(sqlite3_errstr(rc)));
        }
        sqlite3_stmt* stmt = nullptr;
        rc = sqlite3_prepare_v2(raw,
            "SELECT table_namespace, table_name, metadata_location "
            "FROM iceberg_tables WHERE catalog_name = 'poc' AND metadata_location IS NOT NULL",
            -1, &stmt, nullptr);
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> statement(stmt,
            sqlite3_finalize);
        if (rc != SQLITE_OK) {
            throw std::runtime_error(sqlite3_errmsg(raw));
        }
        auto text = [&](int i) {
            return reinterpret_cast<const char*>(sqlite3_column_text(stmt, i));
        };
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            metadata.emplace(std::string(text(0)) + "." + text(1), text(2));
        }
        if (rc != SQLITE_DONE) {
            throw std::runtime_error(sqlite3_errmsg(raw));
        }
        query("LOAD iceberg");
        if (std::getenv("LBUG_ICEBERG_POC_PROFILE")) {
            std::clog << "[iceberg_query_csr] duckdb_threads=" << db.NumberOfThreads() << '\n';
        }
    }
    DuckResult query(const std::string& sqlText) {
        auto result = sql.Query(sqlText);
        if (result->HasError()) {
            throw std::runtime_error(result->GetError());
        }
        return result;
    }
    void replaceLookupKeys(const std::vector<int64_t>& keys) {
        // Private to this reader and replaced under LakeRead::mutex before every semi join.
        query("CREATE OR REPLACE TEMP TABLE csr_lookup_keys(candidate_key BIGINT)");
        duckdb::Appender appender(sql, "temp", "main", "csr_lookup_keys");
        for (auto key : keys) {
            appender.AppendRow(key);
        }
        appender.Close();
    }
    LakeRows queryRows(const std::string& sqlText) { return LakeRows(query(sqlText)); }
};

static constexpr auto cacheKey = "iceberg_query_csr/pinned_read";

using Clock = std::chrono::steady_clock;

static Clock::time_point tracePhase(const char* phase, Clock::time_point start) {
    auto end = Clock::now();
    static const bool enabled = std::getenv("LBUG_ICEBERG_POC_PROFILE") != nullptr;
    if (enabled) {
        std::clog << "[iceberg_query_csr] phase=" << phase
                  << " ms=" << std::chrono::duration<double, std::milli>(end - start).count()
                  << '\n';
    }
    return end;
}

LakeRead::LakeRead(const std::string& catalog, int32_t scanThreads)
    : LocalCacheObject{cacheKey}, reader{std::make_unique<Reader>(catalog, scanThreads)} {}
LakeRead::~LakeRead() = default;

LakeRead& LakeRead::get(const Transaction* txn) {
    if (!txn || !txn->getClientContext()) {
        throw std::runtime_error("Iceberg reads require an active lbug transaction");
    }
    auto& cache = const_cast<Transaction*>(txn)->getLocalCacheManager();
    if (!cache.contains(cacheKey)) {
        auto start = Clock::now();
        const auto& context = *txn->getClientContext();
        auto path = context.getCurrentSetting("iceberg_poc_catalog").toString();
        cache.put(std::make_unique<LakeRead>(path, context.getClientConfig()->icebergScanThreads));
        tracePhase("open_reader", start);
    }
    return *cache.at(cacheKey).cast<LakeRead>();
}

static void appendValue(LakeRead::Column& out, const duckdb::Value& value,
    duckdb::LogicalTypeId type) {
    if (value.IsNull()) {
        out.push_back(Value::createNullValue(
            type == duckdb::LogicalTypeId::BIGINT ? LogicalType::INT64() : LogicalType::STRING()));
    } else if (type == duckdb::LogicalTypeId::BIGINT) {
        out.emplace_back(value.GetValue<int64_t>());
    } else {
        out.emplace_back(value.GetValue<std::string>());
    }
}

void LakeRead::fetch(const Source& source, const std::vector<std::string>& requested) {
    auto& data = columns[source];
    std::vector<std::string> missing;
    std::string projection;
    for (auto& name : requested) {
        if (!data.contains(name) &&
            std::find(missing.begin(), missing.end(), name) == missing.end()) {
            projection += (projection.empty() ? "" : ", ") + sqlQuote(name, '"');
            missing.push_back(name);
        }
    }
    if (missing.empty()) {
        return;
    }
    const auto it = reader->metadata.find(source.table);
    if (it == reader->metadata.end()) {
        throw std::runtime_error("No pinned metadata for " + source.table);
    }
    // All projected columns use the same immutable metadata and the same unique sort key.
    auto start = Clock::now();
    auto result =
        reader->queryRows("SELECT " + projection + " FROM iceberg_scan(" +
                          sqlQuote(it->second, '\'') + ") ORDER BY " + sqlQuote(source.key, '"'));
    auto scanned = tracePhase("scan_and_sort", start);
    std::map<std::string, Column> loaded;
    for (size_t col = 0; col < missing.size(); ++col) {
        const auto type = result.columns.Types()[col].id();
        const bool isKey = missing[col] == source.key;
        if (isKey && type != duckdb::LogicalTypeId::BIGINT) {
            throw std::runtime_error("Expected BIGINT key: " + source.table + "." + source.key);
        }
        if (type != duckdb::LogicalTypeId::BIGINT && type != duckdb::LogicalTypeId::VARCHAR) {
            throw std::runtime_error(
                "Iceberg mappings support BIGINT and VARCHAR: " + missing[col]);
        }
        auto& values = loaded[missing[col]];
        values.reserve(result.columns.Count());
        for (size_t row = 0; row < result.columns.Count(); ++row) {
            auto value = result.getValue(col, row);
            appendValue(values, value, type);
            if (isKey && (value.IsNull() || (row && values[row - 1].getValue<int64_t>() ==
                                                        values[row].getValue<int64_t>()))) {
                throw std::runtime_error(
                    "Expected unique, non-null BIGINT key: " + source.table + "." + source.key);
            }
        }
    }
    data.merge(loaded);
    tracePhase("convert_columns", scanned);
}

const LakeRead::Column& LakeRead::column(const Source& source, const std::string& name) {
    std::lock_guard lock(mutex);
    fetch(source, {source.key, name});
    return columns.at(source).at(name);
}

LakeRead::KeyedRows LakeRead::lookup(const Source& source, std::vector<int64_t> keys,
    const std::vector<LookupColumn>& properties, const std::string& filter, int64_t upper) {
    KeyedRows rows;
    if (keys.empty()) {
        return rows;
    }
    std::ranges::sort(keys);
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    const auto key = sqlQuote(source.key, '"');
    std::string projection = key;
    for (const auto& prop : properties) {
        projection += ", " + sqlQuote(prop.name, '"');
    }
    auto sql = "SELECT " + projection + " FROM iceberg_scan(" +
               sqlQuote(metadataFile(source), '\'') + ") AS lake";
    const bool useKeyTable = keys.size() > lbug::common::DEFAULT_VECTOR_CAPACITY;
    std::lock_guard lock(mutex);
    if (useKeyTable) {
        reader->replaceLookupKeys(keys);
        sql += " SEMI JOIN temp.main.csr_lookup_keys AS k ON lake." + key + " = k.candidate_key";
    } else {
        sql += " WHERE " + key + " IN (";
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i) {
                sql += ',';
            }
            sql += std::to_string(keys[i]);
        }
        sql += ")";
    }
    if (!filter.empty()) {
        sql += (useKeyTable ? " WHERE " : " AND ") + sqlQuote(filter, '"') + " < " +
               std::to_string(upper);
    }
    auto result = reader->query(sql);
    if (result->types[0].id() != duckdb::LogicalTypeId::BIGINT) {
        throw std::runtime_error("Expected BIGINT lookup key: " + source.key);
    }
    for (size_t i = 0; i < properties.size(); ++i) {
        const auto expected = properties[i].type == lbug::common::LogicalTypeID::INT64 ?
                                  duckdb::LogicalTypeId::BIGINT :
                                  duckdb::LogicalTypeId::VARCHAR;
        if (result->types[i + 1].id() != expected) {
            throw std::runtime_error("Mapped property type mismatch: " + properties[i].name);
        }
    }
    rows.reserve(result->RowCount());
    while (auto chunk = result->Fetch()) {
        const auto* resultKeys = duckdb::FlatVector::GetData<int64_t>(chunk->data[0]);
        for (size_t row = 0; row < chunk->size(); ++row) {
            auto [it, inserted] = rows.try_emplace(resultKeys[row]);
            if (!inserted) {
                throw std::runtime_error("Duplicate lookup key in " + source.table);
            }
            it->second.reserve(properties.size());
            for (size_t i = 0; i < properties.size(); ++i) {
                appendValue(it->second, chunk->GetValue(i + 1, row), result->types[i + 1].id());
            }
        }
    }
    if (filter.empty() && rows.size() != keys.size()) {
        throw std::runtime_error("CSR key missing from the pinned snapshot of " + source.table);
    }
    if (std::getenv("LBUG_ICEBERG_POC_PROFILE")) {
        std::clog << "[iceberg_key_batch] table=" << source.table << " keys=" << keys.size()
                  << " rows=" << rows.size() << " sql=" << sql << '\n';
    }
    return rows;
}

std::shared_ptr<const NodeKeys> LakeRead::orderedNodeKeys(const Source& source,
    NativeStore& store) {
    std::lock_guard lock(mutex);
    return nodeKeys(source, store);
}

static std::optional<size_t> slotOf(const NodeKeys& keys, int64_t key) {
    auto it = std::lower_bound(keys.begin(), keys.end(), key);
    if (it == keys.end() || *it != key) {
        return std::nullopt;
    }
    return it - keys.begin();
}

std::shared_ptr<const NodeKeys> LakeRead::nodeKeys(const Source& source, NativeStore& store) {
    const auto& metadata = metadataFile(source);
    return store.nodeKeys(source, metadata, [&] {
        auto key = sqlQuote(source.key, '"');
        auto result = reader->query(
            "SELECT " + key + " FROM iceberg_scan(" + sqlQuote(metadata, '\'') + ") ORDER BY 1");
        if (result->Collection().Types()[0].id() != duckdb::LogicalTypeId::BIGINT) {
            throw std::runtime_error("Expected BIGINT key: " + source.table + "." + source.key);
        }
        NodeKeys keys;
        keys.reserve(result->RowCount());
        while (auto chunk = result->Fetch()) {
            auto& col = chunk->data[0];
            auto& valid = duckdb::FlatVector::Validity(col);
            if (valid.CountValid(chunk->size()) != chunk->size()) {
                throw std::runtime_error(
                    "Expected unique, non-null BIGINT key: " + source.table + "." + source.key);
            }
            auto* values = duckdb::FlatVector::GetData<int64_t>(col);
            keys.insert(keys.end(), values, values + chunk->size());
        }
        if (std::adjacent_find(keys.begin(), keys.end()) != keys.end()) {
            throw std::runtime_error(
                "Expected unique, non-null BIGINT key: " + source.table + "." + source.key);
        }
        if (std::getenv("LBUG_ICEBERG_POC_PROFILE")) {
            std::clog << "[iceberg_csr] loaded endpoint keys " << source.table
                      << " rows=" << keys.size() << '\n';
        }
        return keys;
    });
}

std::optional<size_t> LakeRead::nodeOffset(const Source& source, int64_t key, NativeStore* store) {
    if (store) {
        std::lock_guard lock(mutex);
        return slotOf(*nodeKeys(source, *store), key);
    }
    const auto& keys = column(source, source.key);
    auto it = std::lower_bound(keys.begin(), keys.end(), key,
        [](const Value& v, int64_t k) { return v.getValue<int64_t>() < k; });
    if (it == keys.end() || it->getValue<int64_t>() != key) {
        return std::nullopt;
    }
    return it - keys.begin();
}

const std::string& LakeRead::metadataFile(const Source& source) const {
    return reader->metadata.at(source.table);
}

using KeyMap = std::unordered_map<int64_t, size_t>;

static KeyMap offsets(const LakeRead::Column& keys) {
    KeyMap result;
    result.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        result.emplace(keys[i].getValue<int64_t>(), i);
    }
    return result;
}

TopologyRows LakeRead::mapEndpoints(const EdgeMapping& edge, const Source& from, const Source& to) {
    fetch(from, {from.key});
    fetch(to, {to.key});
    fetch(edge.source, {edge.source.key, edge.from, edge.to});
    auto start = Clock::now();
    auto srcOffsets = offsets(columns.at(from).at(from.key));
    auto dstOffsets = offsets(columns.at(to).at(to.key));
    auto& src = columns.at(edge.source).at(edge.from);
    auto& dst = columns.at(edge.source).at(edge.to);
    const auto& keys = columns.at(edge.source).at(edge.source.key);
    TopologyRows rows{srcOffsets.size(), dstOffsets.size(), {}, keys};
    rows.edges.reserve(src.size());
    for (size_t row = 0; row < src.size(); ++row) {
        if (src[row].isNull() || dst[row].isNull()) {
            continue;
        }
        if (src[row].getDataType() != LogicalType::INT64() ||
            dst[row].getDataType() != LogicalType::INT64()) {
            throw std::runtime_error("Mapped endpoints must be BIGINT");
        }
        auto a = srcOffsets.find(src[row].getValue<int64_t>());
        auto b = dstOffsets.find(dst[row].getValue<int64_t>());
        if (a == srcOffsets.end() || b == dstOffsets.end()) {
            continue; // Same endpoint membership as an inner join.
        }
        rows.edges.push_back({static_cast<int64_t>(a->second), static_cast<int64_t>(b->second),
            static_cast<int64_t>(row)});
    }
    tracePhase("map_endpoints", start);
    return rows;
}

const Adjacency& LakeRead::adjacency(lbug::common::table_id_t relID, const EdgeMapping& edge,
    const Source& from, const Source& to) {
    std::lock_guard lock(mutex);
    if (auto it = indexes.find(relID); it != indexes.end()) {
        return it->second;
    }
    const auto rows = mapEndpoints(edge, from, to);
    auto start = Clock::now();
    Adjacency index;
    index.forward.indptr.resize(rows.sources + 1);
    index.backward.indptr.resize(rows.destinations + 1);
    for (const auto& r : rows.edges) {
        ++index.forward.indptr[r[0] + 1];
        ++index.backward.indptr[r[1] + 1];
    }
    for (auto* csr : {&index.forward, &index.backward}) {
        std::partial_sum(csr->indptr.begin(), csr->indptr.end(), csr->indptr.begin());
        csr->arcs.resize(rows.edges.size());
    }
    auto fwd = index.forward.indptr, bwd = index.backward.indptr;
    for (auto [a, b, row] : rows.edges) {
        index.forward.arcs[fwd[a]++] = {static_cast<size_t>(b), static_cast<size_t>(row)};
        index.backward.arcs[bwd[b]++] = {static_cast<size_t>(a), static_cast<size_t>(row)};
    }
    tracePhase("build_adjacency", start);
    std::clog << "[iceberg_query_csr] built " << edge.source.table << " edges=" << rows.edges.size()
              << "\n  src " << reader->metadata.at(from.table) << "\n  rel "
              << reader->metadata.at(edge.source.table) << "\n  dst "
              << reader->metadata.at(to.table) << '\n';
    return indexes.emplace(relID, std::move(index)).first->second;
}

// Only metadata is read for the parent. Comparing live file paths also handles rewritten manifests
// whose EXISTING entries refer to files already indexed in the parent.
std::optional<AddedRows> LakeRead::readAdded(const Source& source, const AppendStep& step,
    const std::vector<std::string>& projection, bool checkBounds) {
    if (step.parentRows < 0 || step.rows < step.parentRows) {
        return std::nullopt;
    }
    const auto expectedRows = step.rows - step.parentRows;
    const auto metadata = sqlQuote(metadataFile(source), '\'');
    auto scan = [&](const std::string& function, int64_t snapshot) {
        return function + "(" + metadata + ", snapshot_from_id=" + std::to_string(snapshot) + ")";
    };
    auto live = [&](int64_t snapshot) {
        if (snapshot == -1) {
            return std::string{
                "SELECT NULL::VARCHAR AS file_path,NULL::BIGINT AS record_count,NULL::VARCHAR AS "
                "file_format,NULL::VARCHAR AS manifest_content WHERE false"};
        }
        return "SELECT file_path, record_count, file_format, manifest_content FROM " +
               scan("iceberg_metadata", snapshot) + " WHERE status <> 'DELETED'";
    };
    auto diff = reader->queryRows(
        "WITH p AS (" + live(step.parentSnapshot) + "), n AS (" + live(step.snapshot) +
        ") SELECT n.file_path,n.record_count,n.file_format,n.manifest_content "
        "FROM p FULL JOIN n USING(file_path) WHERE p.file_path IS NULL OR n.file_path IS NULL");
    AddedRows delta;
    std::string files;
    int64_t records = 0;
    for (size_t i = 0; i < diff.columns.Count(); ++i) {
        if (diff.getValue(0, i).IsNull() || diff.getValue(2, i).ToString() != "PARQUET" ||
            diff.getValue(3, i).ToString() != "DATA") {
            return std::nullopt;
        }
        files += (files.empty() ? "" : ",") + sqlQuote(diff.getValue(0, i).ToString(), '\'');
        const auto fileRows = diff.getValue(1, i).GetValue<int64_t>();
        if (fileRows < 0 || fileRows > expectedRows - records) {
            return std::nullopt;
        }
        records += fileRows;
    }
    if (records != expectedRows) {
        return std::nullopt;
    }
    delta.files = diff.columns.Count();
    if (files.empty()) {
        delta.aboveParent = true;
        return delta;
    }
    // Iceberg names can differ from Parquet names. Select the three topology columns by field ID.
    std::set<std::string> required(projection.begin(), projection.end());
    std::string selected;
    for (const auto& name : projection) {
        selected += (selected.empty() ? "" : ",") + sqlQuote(name, '"');
    }
    auto fields =
        reader->queryRows("SELECT (f.value->>'id')::INTEGER,f.value->>'name',f.value->>'type' "
                          "FROM read_text(" +
                          metadata +
                          ") m, json_each(m.content::JSON,'$.schemas') s, "
                          "json_each(s.value,'$.fields') f WHERE "
                          "(s.value->>'schema-id')=(m.content::JSON->>'current-schema-id')");
    std::string schema;
    for (size_t i = 0; i < fields.columns.Count(); ++i) {
        auto name = fields.getValue(1, i).ToString();
        if (!required.erase(name)) {
            continue;
        }
        if (fields.getValue(2, i).ToString() != "long") {
            return std::nullopt;
        }
        schema += (schema.empty() ? "" : ",") + fields.getValue(0, i).ToString() +
                  ":{name:" + sqlQuote(name, '\'') + ",type:'BIGINT',default_value:NULL}";
    }
    if (!required.empty()) {
        return std::nullopt;
    }
    // Files that depend on an Iceberg name mapping need the ordinary Iceberg reader.
    auto tagged =
        reader->queryRows("SELECT count(*) FROM parquet_schema([" + files +
                          "]) WHERE type IS NOT NULL AND (field_id IS NULL OR field_id < 1)");
    if (tagged.getValue(0, 0).GetValue<int64_t>() != 0) {
        return std::nullopt;
    }
    auto rows = reader->queryRows("SELECT " + selected + " FROM read_parquet([" + files +
                                  "],schema=MAP{" + schema + "}) ORDER BY 1");
    if (rows.columns.Count() != static_cast<uint64_t>(records)) {
        throw std::runtime_error("Appended Parquet row count differs from Iceberg manifests");
    }
    for (size_t i = 0; i < rows.columns.Count(); ++i) {
        std::array<std::optional<int64_t>, 3> row{};
        for (size_t col = 0; col < projection.size(); ++col) {
            auto value = rows.getValue(col, i);
            if (!value.IsNull()) {
                row[col] = value.GetValue<int64_t>();
            }
        }
        if (!row[0] || (!delta.rows.empty() && row[0] == delta.rows.back()[0])) {
            throw std::runtime_error("Expected unique, non-null BIGINT key: " + source.table);
        }
        delta.rows.push_back(row);
    }
    if (!checkBounds || !step.parentRows || delta.rows.empty()) {
        return delta;
    }
    auto bound = reader->queryRows(
        "WITH p AS (" + live(step.parentSnapshot) +
        "), s AS (SELECT file_path,try_cast(upper_bound AS BIGINT) AS hi FROM " +
        scan("iceberg_column_stats", step.parentSnapshot) +
        " WHERE status <> 'DELETED' "
        "AND column_name=" +
        sqlQuote(source.key, '\'') +
        ") SELECT "
        "CASE WHEN count(*)=count(hi) THEN max(hi) END FROM p LEFT JOIN s USING(file_path) "
        "WHERE p.manifest_content='DATA' AND p.record_count>0");
    auto high = bound.getValue(0, 0);
    delta.aboveParent = !high.IsNull() && *delta.rows.front()[0] > high.GetValue<int64_t>();
    return delta;
}

std::optional<AppendedOrders> LakeRead::readAppend(const AppendStep& step, const EdgeMapping& edge,
    const Source& from, const Source& to, NativeStore& store) {
    auto added = readAdded(edge.source, step, {edge.source.key, edge.from, edge.to}, true);
    if (!added) {
        return std::nullopt;
    }
    AppendedOrders out;
    out.files = added->files;
    out.aboveParent = added->aboveParent;
    if (added->rows.empty()) {
        return out;
    }
    auto src = nodeKeys(from, store), dst = nodeKeys(to, store);
    for (const auto& r : added->rows) {
        auto ordinal = step.parentRows + static_cast<int64_t>(out.keys.size());
        out.keys.push_back(*r[0]);
        auto a = r[1] ? slotOf(*src, *r[1]) : std::nullopt;
        auto b = r[2] ? slotOf(*dst, *r[2]) : std::nullopt;
        if (a && b) {
            out.edges.push_back(
                {static_cast<int64_t>(*a), static_cast<int64_t>(*b), ordinal, *r[0]});
        }
    }
    return out;
}

std::string LakeRead::sortedVersion(const EdgeMapping& edge, const Source& from, const Source& to,
    std::optional<int64_t> parent) {
    std::string parts =
        "'sorted-key-v2-edge-key'," + sqlQuote(edge.from, '\'') + "," + sqlQuote(edge.to, '\'');
    for (const auto* source : {&from, &edge.source, &to}) {
        auto snapshot = source == &edge.source && parent ?
                            sqlQuote(std::to_string(*parent), '\'') :
                            "json_extract_string(content, '$.\"current-snapshot-id\"')";
        parts += "," + sqlQuote(source->table, '\'') + "," + sqlQuote(source->key, '\'') +
                 ",(SELECT to_json([json_extract_string(content, '$.\"table-uuid\"')," + snapshot +
                 ",json_extract_string(content, '$.\"current-schema-id\"')])::VARCHAR FROM "
                 "read_text(" +
                 sqlQuote(metadataFile(*source), '\'') + "))";
    }
    return "IceEdges_" +
           reader->queryRows("SELECT sha256(to_json([" + parts + "]))").getValue(0, 0).ToString();
}

std::string LakeRead::stableTag(const EdgeMapping& edge, const Source& from, const Source& to) {
    return reader
        ->queryRows("SELECT sha256(to_json([" + sqlQuote(history(from).identity, '\'') + "," +
                    sqlQuote(history(edge.source).identity, '\'') + "," +
                    sqlQuote(history(to).identity, '\'') + "," + sqlQuote(edge.from, '\'') + "," +
                    sqlQuote(edge.to, '\'') + "]))")
        .getValue(0, 0)
        .ToString();
}

const LakeHistory& LakeRead::history(const Source& source) {
    if (auto it = histories.find(source); it != histories.end()) {
        return *it->second;
    }
    auto result = std::make_shared<LakeHistory>();
    auto metadata = sqlQuote(metadataFile(source), '\'');
    auto info = reader->queryRows(
        "SELECT sha256(to_json([" + sqlQuote(source.table, '\'') + "," +
        sqlQuote(source.key, '\'') +
        ", "
        "j->>'table-uuid',j->>'current-schema-id'])),coalesce((j->>'current-snapshot-id')::BIGINT,-"
        "1),"
        "(j->>'format-version')::BIGINT FROM (SELECT content::JSON AS j FROM read_text(" +
        metadata + "))");
    if (info.getValue(2, 0).GetValue<int64_t>() != 2) {
        throw std::runtime_error("Stable CSR requires Iceberg v2");
    }
    result->identity = info.getValue(0, 0).ToString();
    result->current = info.getValue(1, 0).GetValue<int64_t>();
    auto snaps = reader->queryRows(
        "SELECT "
        "(s.value->>'snapshot-id')::BIGINT,coalesce((s.value->>'parent-snapshot-id')::BIGINT,-1),"
        "(s.value->'summary'->>'total-records')::BIGINT, "
        "(s.value->'summary'->>'operation')='append' AND "
        "(s.value->'summary'->>'total-delete-files')='0' AND "
        "(s.value->>'schema-id')=(m.content::JSON->>'current-schema-id') "
        "FROM read_text(" +
        metadata + ") m,json_each(m.content::JSON,'$.snapshots') s");
    for (size_t i = 0; i < snaps.columns.Count(); ++i) {
        if (snaps.getValue(2, i).IsNull()) {
            continue;
        }
        auto ok = snaps.getValue(3, i);
        result->snapshots.emplace(snaps.getValue(0, i).GetValue<int64_t>(),
            LakeSnapshot{snaps.getValue(1, i).GetValue<int64_t>(),
                snaps.getValue(2, i).GetValue<int64_t>(), !ok.IsNull() && ok.GetValue<bool>()});
    }
    histories.emplace(source, result);
    return *result;
}

std::shared_ptr<const StableNodes> LakeRead::stableNodesLocked(const Source& source,
    NativeStore& store) {
    auto& view = vertexViews[{store.databasePath(), source}];
    if (!view) {
        view = store.stableNodes(*this, source);
    }
    return view;
}

std::shared_ptr<const StableNodes> LakeRead::stableNodes(const Source& source, NativeStore& store) {
    std::lock_guard lock(mutex);
    return stableNodesLocked(source, store);
}

std::shared_ptr<NativeGeneration> LakeRead::stableGeneration(lbug::common::table_id_t relID,
    const EdgeMapping& edge, const Source& from, const Source& to, NativeStore& store,
    std::optional<int64_t> target) {
    std::lock_guard lock(mutex);
    const auto graphTable = std::pair{store.databasePath(), relID};
    if (auto it = generations.find(graphTable); !target && it != generations.end()) {
        return it->second;
    }
    auto& orders = history(edge.source);
    const auto snapshot = target.value_or(orders.current);
    // Reject an oversized snapshot tuple before advancing either vertex dictionary.
    orders.rows(snapshot);
    for (auto* source : {&from, &to}) {
        auto& vertices = history(*source);
        vertices.rows(vertices.current);
    }
    auto u = stableNodesLocked(from, store), p = stableNodesLocked(to, store);
    auto generation = store.stablePin(*this, edge, from, to, *u, *p, snapshot, target.has_value());
    if (!target) {
        generations.emplace(graphTable, generation);
    }
    return generation;
}

std::shared_ptr<NativeGeneration> LakeRead::nativeGeneration(lbug::common::table_id_t relID,
    const EdgeMapping& edge, const Source& from, const Source& to, NativeStore& store,
    std::optional<int64_t> refreshTarget) {
    std::lock_guard lock(mutex);
    const auto graphTable = std::pair{store.databasePath(), relID};
    if (auto it = generations.find(graphTable); !refreshTarget && it != generations.end()) {
        return it->second;
    }
    // Only substitute the edge snapshot. A changed node snapshot, mapping, schema or UUID
    // therefore cannot match a parent generated with a different endpoint numbering.
    auto version = [&](std::optional<int64_t> id) { return sortedVersion(edge, from, to, id); };
    if (refreshTarget) {
        auto result =
            reader->queryRows(R"(WITH RECURSIVE m AS (SELECT content::JSON AS j FROM read_text()" +
                              sqlQuote(metadataFile(edge.source), '\'') + R"()), snaps AS (
                SELECT (s.value->>'snapshot-id')::BIGINT AS id,
                       (s.value->>'parent-snapshot-id')::BIGINT AS parent
                FROM m,json_each(j,'$.snapshots') s),
            history(id) AS (SELECT (j->>'current-snapshot-id')::BIGINT FROM m UNION
                SELECT parent FROM snaps JOIN history USING (id) WHERE parent IS NOT NULL)
            SELECT count(*) FROM history JOIN snaps USING(id) WHERE id=)" +
                              std::to_string(*refreshTarget));
        if (!result.getValue(0, 0).GetValue<int64_t>()) {
            throw std::runtime_error(
                "Requested snapshot is outside the retained current Iceberg lineage");
        }
    }
    auto name = version(refreshTarget);
    auto lineage = [&](const FindIndexedParent& indexed) {
        std::vector<AppendStep> steps;
        auto snapshots = reader->queryRows(
            "SELECT (s.value->>'snapshot-id')::BIGINT, "
            "(s.value->>'parent-snapshot-id')::BIGINT,(s.value->'summary'->>'total-records')::"
            "BIGINT,"
            "s.value->'summary'->>'operation', (m.content::JSON->>'current-snapshot-id')::BIGINT "
            "FROM read_text(" +
            sqlQuote(metadataFile(edge.source), '\'') +
            ") m, json_each(m.content::JSON,'$.snapshots') s WHERE "
            "(m.content::JSON->>'format-version')='2' "
            "AND (s.value->>'schema-id')=(m.content::JSON->>'current-schema-id') "
            "AND (s.value->'summary'->>'total-delete-files')='0' AND "
            "(s.value->'summary'->>'total-records') IS NOT NULL");
        if (!snapshots.columns.Count()) {
            return steps;
        }
        std::map<int64_t, size_t> byID;
        for (size_t i = 0; i < snapshots.columns.Count(); ++i) {
            byID.emplace(snapshots.getValue(0, i).GetValue<int64_t>(), i);
        }
        auto id = refreshTarget.value_or(snapshots.getValue(4, 0).GetValue<int64_t>());
        auto child = name;
        while (byID.contains(id)) {
            auto row = byID.at(id);
            auto parent = snapshots.getValue(1, row);
            if (parent.IsNull() || snapshots.getValue(3, row).ToString() != "append") {
                break;
            }
            auto pid = parent.GetValue<int64_t>();
            if (!byID.contains(pid)) {
                break;
            }
            auto ancestor = version(pid);
            steps.push_back(
                {child, ancestor, id, pid, snapshots.getValue(2, row).GetValue<int64_t>(),
                    snapshots.getValue(2, byID.at(pid)).GetValue<int64_t>()});
            if (indexed(ancestor)) {
                break;
            }
            child = ancestor;
            id = pid;
        }
        return steps;
    };
    auto generation = store.pin(
        name, lineage,
        [&](const AppendStep& step) { return readAppend(step, edge, from, to, store); },
        [&] {
            if (refreshTarget) {
                throw std::runtime_error("Full CSR rebuild required for requested snapshot; "
                                         "incremental refresh was not applied");
            }
            return mapEndpoints(edge, from, to);
        });
    if (refreshTarget) {
        return generation;
    }
    return generations.emplace(graphTable, std::move(generation)).first->second;
}
} // namespace lbug::iceberg
