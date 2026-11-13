#include <algorithm>

#include "binder/binder.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "lake.h"
#include "main/client_context.h"
#include "main/database.h"
#include "main/database_manager.h"
#include "native_csr.h"
#include "processor/execution_context.h"
#include "storage/iceberg/iceberg_mapping.h"
#include "storage/storage_manager.h"
#include "storage/table/columnar_node_table_base.h"
#include "storage/table/columnar_rel_table_base.h"
#include "storage/table/external_table_hooks.h"

namespace lbug::iceberg {
void registerKeyLookupPlanner(lbug::main::Database& db);
#ifdef WITH_DATAFUSION
void registerDataFusionPlanner(lbug::main::Database& db);
#endif
using namespace lbug::catalog;
using namespace lbug::common;
using namespace lbug::storage;
using lbug::transaction::Transaction;

using Projection = std::vector<const LakeRead::Column*>;

// Only fetch a lake payload column if the Cypher scan requested it.
static Projection project(const Transaction* txn, const Source& source,
    const TableCatalogEntry& entry, const TableScanState& state,
    const std::string* storedKey = nullptr) {
    Projection out(state.outputVectors.size(), nullptr);
    auto& lake = LakeRead::get(txn);
    for (size_t prop = 0; prop < entry.getNumProperties(); ++prop) {
        const auto& def = entry.getProperty(prop);
        if (def.getName() == "_ID" || (storedKey && def.getName() == *storedKey)) {
            continue;
        }
        for (size_t i = 0; i < out.size(); ++i) {
            if (state.columnIDs[i] == entry.getColumnID(prop)) {
                out[i] = &lake.column(source, def.getName());
                if (!out[i]->empty() && out[i]->front().getDataType() != def.getType()) {
                    throw std::runtime_error("Mapped property type mismatch: " + def.getName());
                }
            }
        }
    }
    return out;
}

static void writeProperties(TableScanState& state, const Projection& cols, size_t pos, size_t row) {
    for (size_t i = 0; i < cols.size(); ++i) {
        if (cols[i]) {
            state.outputVectors[i]->copyFromValue(pos, cols[i]->at(row));
        }
    }
}

class MappedNode final : public ColumnarNodeTableBase {
public:
    Source source;
    bool stable;
    std::shared_ptr<NativeStore> store;

    MappedNode(const StorageManager* sm, const NodeTableCatalogEntry* entry, MemoryManager* mm)
        : ColumnarNodeTableBase{sm, entry, mm, nullptr},
          source{entry->getStorage(), entry->getPrimaryKeyName()},
          stable{entry->getStorageFormatName() == "iceberg_stable_csr"} {
        if (stable || entry->getStorageFormatName() == "iceberg_csr") {
            store = NativeStore::forDatabase(sm->getDatabasePath());
        }
        if (entry->getPrimaryKeyDefinition().getType() != LogicalType::INT64()) {
            throw std::runtime_error("Mapped nodes require an INT64 primary key");
        }
    }

    std::string getColumnarFormatName() const override {
        return nodeTableCatalogEntry->getStorageFormatName();
    }
    row_idx_t getTotalRowCount(const Transaction* txn) const override {
        if (stable) {
            return LakeRead::get(txn).stableNodes(source, *store)->size();
        }
        return LakeRead::get(txn).column(source, source.key).size();
    }
    node_group_idx_t getNumBatches(const Transaction* txn) const override {
        return (getTotalRowCount(txn) + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY;
    }
    bool isVisibleNoLock(const Transaction* txn, offset_t off) const override {
        return off < getTotalRowCount(txn);
    }
    bool isVisible(const Transaction* txn, offset_t off) const override {
        return isVisibleNoLock(txn, off);
    }
    bool lookupPK(const Transaction* txn, ValueVector* key, uint64_t pos,
        offset_t& out) const override {
        if (key->isNull(pos)) {
            return false;
        }
        if (key->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID) {
            auto id = key->getValue<internalID_t>(pos);
            out = id.offset;
            return id.tableID == getTableID() && isVisible(txn, out);
        }
        if (key->dataType.getLogicalTypeID() != LogicalTypeID::INT64) {
            throw std::runtime_error("Mapped node lookup requires an INT64 key");
        }
        auto& lake = LakeRead::get(txn);
        std::optional<size_t> offset =
            stable ? std::optional<size_t>(
                         lake.stableNodes(source, *store)->slot(key->getValue<int64_t>(pos))) :
                     lake.nodeOffset(source, key->getValue<int64_t>(pos), store.get());
        if (!offset) {
            return false;
        }
        out = *offset;
        return true;
    }

    void initScanState(Transaction*, TableScanState& state, bool) const override {
        state.cast<ColumnarNodeTableScanState>().scanCompleted = false;
    }
    bool scanInternal(Transaction* txn, TableScanState& base) override {
        auto& s = base.cast<ColumnarNodeTableScanState>();
        if (s.source != TableScanSource::COMMITTED || s.scanCompleted) {
            return false;
        }
        s.scanCompleted = true;
        const auto start = s.nodeGroupIdx * DEFAULT_VECTOR_CAPACITY;
        const auto total = getTotalRowCount(txn);
        if (start >= total) {
            return false;
        }
        const auto count = std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, total - start);
        s.resetOutVectors();
        auto& sel = s.outState->getSelVectorUnsafe();
        sel.setToUnfiltered(count);
        applySemiMaskFilter(s, start, count, sel);
        auto cols = project(txn, source, *nodeTableCatalogEntry, s, stable ? &source.key : nullptr);
        auto view = stable ? LakeRead::get(txn).stableNodes(source, *store) : nullptr;
        bool hasPayload = std::ranges::any_of(cols, [](auto col) { return col != nullptr; });
        auto keyCol = nodeTableCatalogEntry->getColumnID(source.key);
        for (auto pos : sel.getSelectedPositions()) {
            s.nodeIDVector->setValue<nodeID_t>(pos, {start + pos, getTableID()});
            if (view) {
                for (size_t col = 0; col < s.columnIDs.size(); ++col) {
                    if (s.columnIDs[col] == keyCol) {
                        s.outputVectors[col]->setValue<int64_t>(pos, view->key(start + pos));
                    }
                }
            }
            if (hasPayload) {
                writeProperties(s, cols, pos, view ? view->rank(start + pos) : start + pos);
            }
        }
        return sel.getSelSize() > 0;
    }
};

struct EdgeScan : ColumnarRelTableScanState {
    using ColumnarRelTableScanState::ColumnarRelTableScanState;
    size_t parent = 0, within = 0;
    std::unique_ptr<NativeCursor> native;
    const LakeRead::Column* sortedKeys = nullptr;

    void setToTable(const Transaction* txn, Table* table, std::vector<column_id_t> ids,
        std::vector<ColumnPredicateSet> predicates, RelDataDirection direction) override {
        native.reset();
        sortedKeys = nullptr;
        ColumnarRelTableScanState::setToTable(txn, table, std::move(ids), std::move(predicates),
            direction);
    }
};

class MappedEdge final : public ColumnarRelTableBase {
public:
    MappedEdge(RelGroupCatalogEntry* entry, table_id_t src, table_id_t dst,
        const StorageManager* sm, MemoryManager* mm)
        : ColumnarRelTableBase{entry, src, dst, sm, mm}, edge{entry->getStorage()},
          stable{entry->getStorageFormatName() == "iceberg_stable_csr"} {
        if (stable || entry->getStorageFormatName() == "iceberg_csr") {
            store = NativeStore::forDatabase(sm->getDatabasePath());
        }
    }

    std::string getColumnarFormatName() const override {
        return relGroupEntry->getStorageFormatName();
    }
    const Source& nodeSource(table_id_t id) const {
        auto& node = storageManager->getTable(id)->cast<MappedNode>();
        if (node.stable != stable) {
            throw std::runtime_error(
                "A stable CSR relationship requires stable numbering on both endpoints");
        }
        return node.source;
    }
    std::shared_ptr<NativeGeneration> generation(const Transaction* txn,
        std::optional<int64_t> target = std::nullopt) const {
        if (stable) {
            return LakeRead::get(txn).stableGeneration(getTableID(), edge,
                nodeSource(getFromNodeTableID()), nodeSource(getToNodeTableID()), *store, target);
        }
        return LakeRead::get(txn).nativeGeneration(getTableID(), edge,
            nodeSource(getFromNodeTableID()), nodeSource(getToNodeTableID()), *store, target);
    }
    const CSR& csr(const Transaction* txn, RelDataDirection dir) const {
        const auto& index = LakeRead::get(txn).adjacency(getTableID(), edge,
            nodeSource(getFromNodeTableID()), nodeSource(getToNodeTableID()));
        return dir == RelDataDirection::FWD ? index.forward : index.backward;
    }

    void initScanState(Transaction*, TableScanState& base, bool reset) const override {
        auto& s = base.cast<EdgeScan>();
        s.parent = s.within = 0;
        s.source = TableScanSource::COMMITTED;
        if (reset) {
            const auto& sel = s.nodeIDVector->state->getSelVector();
            s.cachedBoundNodeSelVector.setToFiltered(sel.getSelSize());
            std::ranges::copy(sel.getSelectedPositions(),
                s.cachedBoundNodeSelVector.getMutableBuffer().begin());
        }
    }
    bool scanInternal(Transaction* txn, TableScanState& base) override {
        auto& s = base.cast<EdgeScan>();
        if (store && !s.native) {
            auto g = generation(txn);
            if (!g->sortedRows) {
                s.sortedKeys = &LakeRead::get(txn).column(edge.source, edge.source.key);
            }
            s.native =
                std::make_unique<NativeCursor>(g, s.sortedKeys ? "lake_key" : "lake_row", stable);
        }
        const auto* lists = store ? nullptr : &csr(txn, s.direction);
        while (s.parent < s.cachedBoundNodeSelVector.getSelSize()) {
            const auto parentPos = s.cachedBoundNodeSelVector[s.parent];
            const auto parent = s.nodeIDVector->readNodeOffset(parentPos);
            std::span<const Arc> batch;
            if (s.native) {
                s.native->next(parent, s.direction, s.within == 0, s.sortedKeys);
                batch = s.native->batch;
            } else {
                if (parent >= lists->indptr.size() - 1) {
                    throw std::runtime_error("Node offset outside pinned CSR");
                }
                auto start = lists->indptr[parent] + s.within;
                batch = std::span(lists->arcs)
                            .subspan(start, std::min<size_t>(lists->indptr[parent + 1] - start,
                                                DEFAULT_VECTOR_CAPACITY));
            }
            if (batch.empty()) {
                ++s.parent;
                s.within = 0;
                continue;
            }
            s.resetOutVectors();
            const auto cols =
                project(txn, edge.source, *relGroupEntry, s, stable ? &edge.source.key : nullptr);
            auto nbrID =
                s.direction == RelDataDirection::FWD ? getToNodeTableID() : getFromNodeTableID();
            for (size_t pos = 0; pos < batch.size(); ++pos) {
                const auto [neighbor, row] = batch[pos];
                s.outputVectors[0]->setValue<internalID_t>(pos, {neighbor, nbrID});
                for (size_t col = 1; col < s.columnIDs.size(); ++col) {
                    if (stable && s.columnIDs[col] == relGroupEntry->getColumnID(edge.source.key)) {
                        s.outputVectors[col]->setValue<int64_t>(pos, s.native->keys[pos]);
                    }
                    if (s.columnIDs[col] == REL_ID_COLUMN_ID) {
                        s.outputVectors[col]->setValue<internalID_t>(pos, {row, getTableID()});
                    }
                }
                writeProperties(s, cols, pos, row);
            }
            s.within += batch.size();
            s.outState->getSelVectorUnsafe().setToUnfiltered(batch.size());
            s.setNodeIDVectorToFlat(parentPos);
            return true;
        }
        return false;
    }
    row_idx_t getTotalRowCount(const Transaction* txn) const override {
        if (store) {
            auto g = generation(txn);
            return g->edgeCount;
        }
        return csr(txn, RelDataDirection::FWD).arcs.size();
    }
    row_idx_t getDegreeForOffsetInternal(const Transaction* txn, RelDataDirection dir,
        offset_t node) const override {
        if (store) {
            auto g = generation(txn);
            if (g->rowEnd) {
                return NativeCursor(g).degree(node, dir);
            }
            return g->table->getDegreeForOffset(g->txn, dir, node);
        }
        const auto& p = csr(txn, dir).indptr;
        return node < p.size() - 1 ? p[node + 1] - p[node] : 0;
    }
    using Degrees = std::vector<std::pair<offset_t, row_idx_t>>;
    Degrees getAllDegreeEntries(const Transaction* txn, RelDataDirection dir) const override {
        if (store) {
            auto g = generation(txn);
            auto entries = g->table->getDegreeEntries(g->txn, dir);
            if (g->rowEnd) {
                NativeCursor cursor(g);
                for (auto& [offset, n] : entries) {
                    n = cursor.degree(offset, dir);
                }
                std::erase_if(entries, [](const auto& e) { return !e.second; });
            }
            return entries;
        }
        const auto& p = csr(txn, dir).indptr;
        Degrees result;
        for (size_t i = 0; i + 1 < p.size(); ++i) {
            if (p[i + 1] > p[i]) {
                result.emplace_back(i, p[i + 1] - p[i]);
            }
        }
        return result;
    }
    row_idx_t getActiveBoundNodeCount(const Transaction* txn, RelDataDirection dir) const override {
        if (store) {
            auto g = generation(txn);
            if (!g->rowEnd) {
                return g->table->getNumActiveBoundNodes(g->txn, dir);
            }
        }
        return getAllDegreeEntries(txn, dir).size();
    }
    Degrees getTopKDegreeEntries(const Transaction* txn, RelDataDirection dir,
        idx_t k) const override {
        if (store) {
            auto g = generation(txn);
            if (!g->rowEnd) {
                return g->table->getTopKDegrees(g->txn, dir, k);
            }
        }
        auto degrees = getAllDegreeEntries(txn, dir);
        auto end = degrees.begin() + std::min<size_t>(k, degrees.size());
        std::partial_sort(degrees.begin(), end, degrees.end(), [](auto& a, auto& b) {
            return a.second != b.second ? a.second > b.second : a.first < b.first;
        });
        degrees.erase(end, degrees.end());
        return degrees;
    }

private:
    EdgeMapping edge;
    bool stable;
    std::shared_ptr<NativeStore> store;
};
// A CDC consumer can retry the same committed snapshot on a connection to the open database.
struct RefreshBindData final : lbug::function::TableFuncBindData {
    table_id_t relID;
    std::string dbName;
    int64_t target;
    RefreshBindData(lbug::binder::expression_vector cols, table_id_t id, std::string dbName,
        int64_t target)
        : TableFuncBindData{std::move(cols), 1}, relID{id}, dbName{std::move(dbName)},
          target{target} {}
    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<RefreshBindData>(*this);
    }
};

function::function_set RefreshCSR::getFunctionSet() {
    using namespace lbug::function;
    auto f = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::INT64});
    f->bindFunc = [](lbug::main::ClientContext* ctx, const TableFuncBindInput* input) {
        auto table = input->getLiteralVal<std::string>(0);
        auto* catalog = Catalog::Get(*ctx);
        std::string dbName;
        auto* manager = main::DatabaseManager::Get(*ctx);
        if (!manager->getDefaultGraphCatalog() && manager->hasDefaultDatabase()) {
            auto* attached = manager->getAttachedDatabase(manager->getDefaultDatabase());
            if (attached->getDBType() == ATTACHED_LBUG_DB_TYPE) {
                catalog = attached->getCatalog();
                dbName = attached->getDBName();
            }
        }
        auto* entry = catalog->getTableCatalogEntry(Transaction::Get(*ctx), table);
        if (entry->getType() != CatalogEntryType::REL_GROUP_ENTRY ||
            (entry->cast<RelGroupCatalogEntry>().getStorageFormatName() != "iceberg_csr" &&
                entry->cast<RelGroupCatalogEntry>().getStorageFormatName() !=
                    "iceberg_stable_csr")) {
            throw std::runtime_error("Expected a mapped iceberg_csr relationship: " + table);
        }
        auto& rel = entry->cast<RelGroupCatalogEntry>();
        if (rel.getRelEntryInfos().size() != 1) {
            throw std::runtime_error("Expected one mapped endpoint pair");
        }
        auto names = TableFunction::extractYieldVariables({"snapshot_id", "edge_count"},
            input->yieldVariables);
        std::vector<LogicalType> types;
        types.push_back(LogicalType::INT64());
        types.push_back(LogicalType::INT64());
        auto cols = input->binder->createVariables(names, types);
        return std::make_unique<RefreshBindData>(std::move(cols), rel.getRelEntryInfos()[0].oid,
            std::move(dbName), input->getLiteralVal<int64_t>(1));
    };
    f->initSharedStateFunc = SimpleTableFunc::initSharedState;
    f->initLocalStateFunc = TableFunction::initEmptyLocalState;
    f->tableFunc = SimpleTableFunc::getTableFunc(
        [](const TableFuncMorsel&, const TableFuncInput& in, DataChunk& out) {
            auto& args = in.bindData->cast<RefreshBindData>();
            auto& ctx = *in.context->clientContext;
            auto* storage =
                main::DatabaseManager::resolveTableStorage(ctx, args.relID, args.dbName).second;
            auto& rel = storage->getTable(args.relID)->cast<MappedEdge>();
            auto gen = rel.generation(Transaction::Get(ctx), args.target);
            out.getValueVectorMutable(0).setValue(0, args.target);
            out.getValueVectorMutable(1).setValue(0, gen->edgeCount);
            return 1;
        });
    function_set result;
    result.push_back(std::move(f));
    return result;
}

static void registerStorageFormats() {
    ColumnarTableHooks hooks;
    hooks.createNodeTable = [](auto sm, auto entry, auto mm, auto) {
        return std::make_unique<MappedNode>(sm, entry, mm);
    };
    hooks.createRelTable = [](auto entry, auto src, auto dst, auto sm, auto mm, auto) {
        return std::make_unique<MappedEdge>(entry, src, dst, sm, mm);
    };
    hooks.createNodeScanState = [](auto, auto ids, const auto& out, auto state, auto mm) {
        return std::make_unique<ColumnarNodeTableScanState>(*mm, ids, out, state);
    };
    hooks.getNumScanMorsels = [](auto txn, lbug::storage::NodeTable* table) {
        return table->cast<MappedNode>().getNumBatches(txn);
    };
    hooks.createRelScanState = [](auto, auto ids, const auto& out, auto state, auto mm) {
        return std::make_unique<EdgeScan>(*mm, ids, out, state);
    };
    ColumnarTableHookRegistry::registerHooks("iceberg_poc", hooks);
    ColumnarTableHookRegistry::registerHooks("iceberg_stable_csr", hooks);
    ColumnarTableHookRegistry::registerHooks("iceberg_csr", std::move(hooks));
}

void registerIcebergMapping(main::Database& db) {
    static std::once_flag formats;
    std::call_once(formats, registerStorageFormats);
    db.addExtensionOption("iceberg_poc_catalog", LogicalTypeID::STRING, Value(""));
    registerKeyLookupPlanner(db);
#ifdef WITH_DATAFUSION
    registerDataFusionPlanner(db);
#endif
}

} // namespace lbug::iceberg
