#include "native_csr.h"

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>

#include "binder/binder.h"
#include "catalog/catalog.h"
#include "function/table/scan_replacement.h"
#include "function/table/simple_table_function.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "processor/result/flat_tuple.h"
#include "storage/storage_manager.h"

namespace lbug::iceberg {
using namespace lbug::common;
using namespace lbug::function;
using namespace lbug::graph;
using namespace lbug::main;
using lbug::transaction::Transaction;

std::unique_ptr<QueryResult> nativeQuery(Connection& conn, const std::string& sql) {
    auto result = conn.query(sql);
    if (!result->isSuccess()) {
        throw std::runtime_error(result->getErrorMessage());
    }
    return result;
}

// Feed endpoints and locators into native COPY on the short-lived build connection.
void copyTopology(Connection& writer, const std::string& name, const TopologyRows& rows) {
    static uint8_t inputHandle;
    writer.getClientContext()->addScanReplace(ScanReplacement{
        [](const std::string& object) {
            return object == "TopologyInput" ? std::vector<scan_replace_handle_t>{&inputHandle} :
                                               std::vector<scan_replace_handle_t>{};
        },
        [&rows](std::span<scan_replace_handle_t>) {
            auto replacement = std::make_unique<ScanReplacementData>();
            auto& f = replacement->func;
            f = TableFunction("scan_topology_input", {});
            f.bindFunc = [&rows](ClientContext*, const TableFuncBindInput* in) {
                std::vector<LogicalType> types;
                for (auto i = 0; i < 4; ++i) {
                    types.push_back(LogicalType::INT64());
                }
                auto columns =
                    in->binder->createVariables({"src", "dst", "lake_row", "lake_key"}, types);
                return std::make_unique<TableFuncBindData>(std::move(columns), rows.edges.size());
            };
            f.initSharedStateFunc = SimpleTableFunc::initSharedState;
            f.initLocalStateFunc = TableFunction::initEmptyLocalState;
            f.tableFunc = SimpleTableFunc::getTableFunc(
                [&rows](const TableFuncMorsel& part, const TableFuncInput&, DataChunk& out) {
                    for (size_t i = 0; i < part.getMorselSize(); ++i) {
                        const auto& edge = rows.edges[part.startOffset + i];
                        for (size_t col = 0; col < edge.size(); ++col) {
                            out.getValueVectorMutable(col).setValue(i, edge[col]);
                        }
                        out.getValueVectorMutable(3).setValue(i,
                            rows.edgeKeys[edge[2]].getValue<int64_t>());
                    }
                    return part.getMorselSize();
                });
            return replacement;
        }});
    nativeQuery(writer, "COPY " + name + " FROM TopologyInput");
}

std::shared_ptr<NativeStore> NativeStore::forDatabase(const std::string& mainPath) {
    static std::mutex registryMutex;
    static std::map<std::string, std::weak_ptr<NativeStore>> stores;
    std::lock_guard lock(registryMutex);
    std::erase_if(stores, [](const auto& it) { return it.second.expired(); });
    const auto path = DBConfig::isDBPathInMemory(mainPath) ?
                          mainPath :
                          std::filesystem::weakly_canonical(mainPath).string();
    auto& ref = stores[path];
    if (auto store = ref.lock()) {
        return store;
    }
    auto store = std::shared_ptr<NativeStore>(new NativeStore(path));
    ref = store;
    return store;
}

std::shared_ptr<const NodeKeys> NativeStore::nodeKeys(const Source& source,
    const std::string& metadata, const std::function<NodeKeys()>& load) {
    std::lock_guard lock(keyMutex);
    auto& cached = nodeKeyCache[source];
    if (cached.first != metadata || !cached.second) {
        cached = {metadata, std::make_shared<const NodeKeys>(load())};
    }
    return cached.second;
}

struct StoredVersion {
    std::string physical;
    int64_t rows, edges;
    bool sorted, head;
};

static std::optional<StoredVersion> findVersion(Connection& reader, const std::string& name) {
    auto& context = *reader.getClientContext();
    auto* catalog = context.getDatabase()->getCatalog();
    auto* txn = Transaction::Get(context);
    if (catalog->containsTable(txn, "CsrVersions")) {
        auto result = nativeQuery(reader,
            "MATCH (v:CsrVersions {id:'" + name +
                "'}) RETURN v.physical,v.row_end,v.edge_count,v.sorted_rows,v.is_head");
        if (result->hasNext()) {
            auto row = result->getNext();
            return StoredVersion{row->getValue(0)->getValue<std::string>(),
                row->getValue(1)->getValue<int64_t>(), row->getValue(2)->getValue<int64_t>(),
                row->getValue(3)->getValue<bool>(), row->getValue(4)->getValue<bool>()};
        }
    }
    if (catalog->containsTable(txn, name)) {
        return StoredVersion{name, -1, -1, true, true}; // Unmodified full-build sidecar.
    }
    return std::nullopt;
}

static void saveVersion(Connection& writer, const std::string& name, const StoredVersion& v) {
    nativeQuery(writer, "CREATE (:CsrVersions {id:'" + name + "', physical:'" + v.physical +
                            "',row_end:" + std::to_string(v.rows) +
                            ",edge_count:" + std::to_string(v.edges) +
                            ",sorted_rows:" + (v.sorted ? "true" : "false") + ",is_head:true})");
}

void writeEdges(Connection& writer, const std::string& physical, const AppendedOrders& delta) {
    if (delta.edges.empty()) {
        return;
    }
    // The normal relationship CREATE appends to both native directions in this writer transaction.
    // Bind the endpoint PKs for point lookups even when the existing table has millions of slots.
    auto statement = writer.prepare("MATCH (s:CsrSlots {pos:$src}),(d:CsrSlots {pos:$dst}) "
                                    "CREATE (s)-[:" +
                                    physical + " {lake_row:$ord,lake_key:$key}]->(d) RETURN 1");
    if (!statement->isSuccess()) {
        throw std::runtime_error(statement->getErrorMessage());
    }
    for (const auto& row : delta.edges) {
        auto arg = [&](const char* name, size_t col) {
            return std::pair<std::string, int64_t>{name, row[col]};
        };
        auto result = writer.execute(statement.get(), arg("src", 0), arg("dst", 1), arg("ord", 2),
            arg("key", 3));
        if (!result->isSuccess()) {
            throw std::runtime_error(result->getErrorMessage());
        }
        if (!result->hasNext()) {
            throw std::runtime_error("Missing native endpoint slot during CSR append");
        }
    }
}

static bool appendVersions(Connection& writer, const AppendHistory& lineage,
    const std::function<std::optional<AppendedOrders>(const AppendStep&)>& readAppend) {
    std::optional<StoredVersion> parent;
    auto steps = lineage([&](const std::string& version) {
        parent = findVersion(writer, version);
        return parent.has_value();
    });
    size_t count = steps.size();
    if (!parent || !parent->head) {
        return false; // A divergent history needs its own base.
    }
    nativeQuery(writer,
        "CREATE NODE TABLE IF NOT EXISTS CsrVersions(id STRING PRIMARY KEY, physical STRING,"
        "row_end INT64, edge_count INT64, sorted_rows BOOL, is_head BOOL)");
    if (parent->rows == -1) {
        const auto& step = steps[count - 1];
        parent->rows = step.parentRows;
        auto& ctx = *writer.getClientContext();
        auto* entry = ctx.getDatabase()
                          ->getCatalog()
                          ->getTableCatalogEntry(Transaction::Get(ctx), parent->physical)
                          ->ptrCast<lbug::catalog::RelGroupCatalogEntry>();
        parent->edges = ctx.getDatabase()
                            ->getStorageManager()
                            ->getTable(entry->getRelEntryInfos()[0].oid)
                            ->ptrCast<lbug::storage::RelTable>()
                            ->getNumTotalRows(Transaction::Get(ctx));
        saveVersion(writer, step.parent, *parent);
    }
    while (count) {
        const auto& step = steps[--count];
        if (parent->rows != step.parentRows) {
            return false;
        }
        auto delta = readAppend(step);
        if (!delta) {
            return false;
        }
        if (!delta->aboveParent) {
            // For interleaved keys, verify uniqueness against the existing native topology.
            // If dangling lake rows were excluded, its keys cannot prove lake-wide uniqueness.
            if (parent->rows != parent->edges) {
                return false;
            }
            for (size_t i = 0; i < delta->keys.size();) {
                std::string list;
                auto end = i + std::min<size_t>(256, delta->keys.size() - i);
                for (; i < end; ++i) {
                    list += (list.empty() ? "" : ",") + std::to_string(delta->keys[i]);
                }
                if (nativeQuery(writer, "MATCH ()-[e:" + parent->physical +
                                            "]->() WHERE e.lake_key IN [" + list +
                                            "] RETURN e.lake_key LIMIT 1")
                        ->hasNext()) {
                    throw std::runtime_error(
                        "Expected unique, non-null BIGINT key for appended orders");
                }
            }
        }
        writeEdges(writer, parent->physical, *delta);
        nativeQuery(writer, "MATCH (v:CsrVersions {id:'" + step.parent + "'}) SET v.is_head=false");
        parent->rows = step.rows;
        parent->edges += delta->edges.size();
        parent->sorted &= delta->aboveParent;
        saveVersion(writer, step.version, *parent);
        if (std::getenv("LBUG_ICEBERG_POC_PROFILE")) {
            std::clog << "[iceberg_csr] append snapshot=" << step.snapshot
                      << " parent=" << step.parentSnapshot << " data_files=" << delta->files
                      << " order_rows=" << delta->keys.size()
                      << " native_edges=" << delta->edges.size() << " physical=" << parent->physical
                      << '\n';
        }
    }
    return true;
}

void NativeStore::open(uint64_t bytes) {
    if (!db) {
        if (DBConfig::isDBPathInMemory(mainPath)) {
            throw std::runtime_error("The iceberg_csr format requires a file-backed main database");
        }
        SystemConfig config(bytes, 1);
        config.throwOnWalReplayFailure = true;
        db = std::make_shared<Database>(mainPath + ".csr.db", config);
    }
}

void ensureSlots(Connection& writer, size_t needed) {
    nativeQuery(writer, "CREATE NODE TABLE IF NOT EXISTS CsrSlots(pos INT64 PRIMARY KEY)");
    const auto slots = nativeQuery(writer, "MATCH (s:CsrSlots) RETURN count(*)")
                           ->getNext()
                           ->getValue(0)
                           ->getValue<int64_t>();
    for (auto first = static_cast<size_t>(slots); first < needed;) {
        const auto end = first + std::min<size_t>(needed - first, StorageConfig::NODE_GROUP_SIZE);
        nativeQuery(writer, "COPY CsrSlots FROM (UNWIND range(" + std::to_string(first) + "," +
                                std::to_string(end - 1) + ") AS i RETURN i)");
        first = end;
    }
}

std::shared_ptr<NativeGeneration> NativeStore::openGeneration(const std::string& physical) {
    auto g = std::make_shared<NativeGeneration>();
    g->owner = db;
    g->reader = std::make_unique<Connection>(db.get());
    nativeQuery(*g->reader, "BEGIN TRANSACTION READ ONLY");
    g->txn = Transaction::Get(*g->reader->getClientContext());
    auto* catalog = db->getCatalog();
    g->slots = catalog->getTableCatalogEntry(g->txn, "CsrSlots");
    g->entry = catalog->getTableCatalogEntry(g->txn, physical)
                   ->ptrCast<lbug::catalog::RelGroupCatalogEntry>();
    g->table = db->getStorageManager()
                   ->getTable(g->entry->getRelEntryInfos()[0].oid)
                   ->ptrCast<lbug::storage::RelTable>();
    g->edgeCount = g->table->getNumTotalRows(g->txn);
    return g;
}

std::shared_ptr<NativeGeneration> NativeStore::pin(const std::string& name,
    const AppendHistory& lineage,
    const std::function<std::optional<AppendedOrders>(const AppendStep&)>& readAppend,
    const std::function<TopologyRows()>& build) {
    std::lock_guard lock(mutex);
    open();
    auto generation = std::make_shared<NativeGeneration>();
    generation->owner = db;
    generation->reader = std::make_unique<Connection>(db.get());
    auto& reader = *generation->reader;
    auto* catalog = db->getCatalog();
    nativeQuery(reader, "BEGIN TRANSACTION READ ONLY");
    auto version = findVersion(reader, name);
    if (!version) {
        auto* txn = Transaction::Get(*reader.getClientContext());
        if (!catalog->containsTable(txn, "CsrSlots") && !catalog->getTableEntries(txn).empty()) {
            throw std::runtime_error(
                "Expected a dedicated Iceberg CSR sidecar at " + mainPath + ".csr.db");
        }
        bool haveSlots = catalog->containsTable(txn, "CsrSlots");
        nativeQuery(reader, "COMMIT");
        Connection writer(db.get());
        auto started = std::chrono::steady_clock::now();
        nativeQuery(writer, "BEGIN TRANSACTION");
        if (haveSlots && appendVersions(writer, lineage, readAppend)) {
            nativeQuery(writer, "COMMIT");
            std::clog << "[iceberg_csr] published append " << name << " ms="
                      << std::chrono::duration<double, std::milli>(
                             std::chrono::steady_clock::now() - started)
                             .count()
                      << '\n';
        } else {
            nativeQuery(writer, "ROLLBACK");
            if (std::getenv("LBUG_ICEBERG_POC_PROFILE")) {
                std::clog << "[iceberg_csr] cache miss, starting full build " << name << '\n';
            }
            const auto rows = build();
            nativeQuery(writer, "BEGIN TRANSACTION");
            ensureSlots(writer, std::max(rows.sources, rows.destinations));
            nativeQuery(writer, "CREATE REL TABLE " + name +
                                    "(FROM CsrSlots TO CsrSlots, lake_row INT64, lake_key INT64)");
            if (!rows.edges.empty()) {
                copyTopology(writer, name, rows);
            }
            nativeQuery(writer, "COMMIT");
            std::clog << "[iceberg_csr] built " << name << " edges=" << rows.edges.size() << '\n';
        }
        nativeQuery(reader, "BEGIN TRANSACTION READ ONLY");
        version = findVersion(reader, name);
        if (!version) {
            throw std::runtime_error("Published CSR version is missing");
        }
    } else {
        std::clog << "[iceberg_csr] reused " << name << '\n';
    }
    auto* txn = Transaction::Get(*reader.getClientContext());
    generation->txn = txn;
    if (!version->head) {
        generation->rowEnd = version->rows;
    }
    generation->sortedRows = version->sorted;
    generation->slots = catalog->getTableCatalogEntry(txn, "CsrSlots");
    generation->entry = catalog->getTableCatalogEntry(txn, version->physical)
                            ->ptrCast<lbug::catalog::RelGroupCatalogEntry>();
    generation->table = db->getStorageManager()
                            ->getTable(generation->entry->getRelEntryInfos()[0].oid)
                            ->ptrCast<lbug::storage::RelTable>();
    // Native table statistics are mutable; pin the count together with the CSR reader.
    generation->edgeCount = version->edges >= 0 ?
                                version->edges :
                                static_cast<int64_t>(generation->table->getNumTotalRows(txn));
    return generation;
}

NativeCursor::NativeCursor(std::shared_ptr<NativeGeneration> generation, const std::string& locator,
    bool includeKey_)
    : generation{std::move(generation)},
      graph{this->generation->reader->getClientContext(),
          NativeGraphEntry({this->generation->slots}, {this->generation->entry})} {
    auto& g = *this->generation;
    std::vector<std::string> properties{locator};
    if (g.rowEnd && locator != "lake_row") {
        rowColumn = 1;
        properties.emplace_back("lake_row");
    }
    includeKey = includeKey_;
    if (includeKey && locator != "lake_key") {
        keyColumn = properties.size();
        properties.emplace_back("lake_key");
    }
    state =
        graph.prepareRelScan(*g.entry, g.table->getTableID(), g.slots->getTableID(), properties);
}

void NativeCursor::seek(size_t parent, RelDataDirection dir) {
    auto limit = dir == RelDataDirection::FWD ? generation->sourceEnd : generation->destinationEnd;
    boundVisible = !limit || parent < static_cast<size_t>(*limit);
    neighborEnd = dir == RelDataDirection::FWD ? generation->destinationEnd : generation->sourceEnd;
    const nodeID_t bound{parent, generation->slots->getTableID()};
    if (dir == RelDataDirection::FWD) {
        graph.scanFwd(bound, *state);
    } else {
        graph.scanBwd(bound, *state);
    }
}

size_t NativeCursor::degree(size_t parent, RelDataDirection dir) {
    seek(parent, dir);
    size_t n = 0;
    while (readChunk([&](auto, int64_t) { ++n; })) {}
    return n;
}

void NativeCursor::next(size_t parent, RelDataDirection dir, bool start,
    const LakeRead::Column* sortedKeys) {
    if (start) {
        seek(parent, dir);
    }
    batch.clear();
    keys.clear();
    while (batch.empty() && readChunkWithKey([&](size_t neighbor, int64_t row, int64_t key) {
        if (includeKey) {
            keys.push_back(key);
        }
        if (sortedKeys) {
            auto it = std::lower_bound(sortedKeys->begin(), sortedKeys->end(), row,
                [](const Value& v, int64_t k) { return v.getValue<int64_t>() < k; });
            if (it == sortedKeys->end() || it->getValue<int64_t>() != row) {
                throw std::runtime_error("CSR order key missing from the pinned snapshot");
            }
            row = it - sortedKeys->begin();
        }
        batch.push_back({neighbor, static_cast<size_t>(row)});
    })) {}
}
} // namespace lbug::iceberg
