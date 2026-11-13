#pragma once

#include <functional>
#include <mutex>

#include "graph/on_disk_graph.h"
#include "main/connection.h"
#include "main/database.h"
#include "stable_csr.h"

namespace lbug::iceberg {

struct NativeGeneration {
    std::shared_ptr<lbug::main::Database> owner;
    std::unique_ptr<lbug::main::Connection> reader;
    lbug::catalog::TableCatalogEntry* slots;
    lbug::catalog::RelGroupCatalogEntry* entry;
    lbug::storage::RelTable* table;
    lbug::transaction::Transaction* txn;
    // A transaction that pins this snapshot after a newer append uses the stored prefix.
    std::optional<int64_t> rowEnd;
    std::optional<int64_t> sourceEnd, destinationEnd;
    int64_t edgeCount;
    bool sortedRows = true;
};

// Each physical scan gets its own graph iterator and vectors.
class NativeCursor {
public:
    explicit NativeCursor(std::shared_ptr<NativeGeneration> generation,
        const std::string& locator = "lake_row", bool includeKey = false);
    void seek(size_t parent, lbug::common::RelDataDirection dir);
    template<typename Visit>
    bool readChunk(Visit visit) {
        return readChunkWithKey([&](auto n, int64_t row, int64_t) { visit(n, row); });
    }
    template<typename Visit>
    bool readChunkWithKey(Visit visit) {
        if (!state->next()) {
            return false;
        }
        state->getChunk().forEach([&](auto neighbors, auto properties, auto i) {
            if (boundVisible &&
                (!generation->rowEnd ||
                    properties[rowColumn]->template getValue<int64_t>(i) < *generation->rowEnd) &&
                (!neighborEnd || neighbors[i].offset < static_cast<size_t>(*neighborEnd))) {
                visit(neighbors[i].offset, properties[0]->template getValue<int64_t>(i),
                    properties[keyColumn]->template getValue<int64_t>(i));
            }
        });
        return true;
    }
    void next(size_t parent, lbug::common::RelDataDirection dir, bool start,
        const LakeRead::Column* sortedKeys = nullptr);
    size_t degree(size_t parent, lbug::common::RelDataDirection dir);
    std::vector<Arc> batch;
    std::vector<int64_t> keys;
    const std::shared_ptr<NativeGeneration> generation;

private:
    lbug::graph::OnDiskGraph graph;
    std::unique_ptr<lbug::graph::NbrScanState> state;
    size_t rowColumn = 0, keyColumn = 0;
    bool boundVisible = true, includeKey = false;
    std::optional<int64_t> neighborEnd;
};

// Consume each native adjacency chunk once, even when a lake batch ends midway through it.
struct Neighbors {
    Neighbors(std::shared_ptr<NativeGeneration> generation,
        lbug::common::RelDataDirection direction, bool includeKey)
        : cursor{std::move(generation), "lake_row", includeKey}, direction{direction} {}
    void seek(size_t node) {
        slot = node;
        cursor.seek(node, direction);
        cursor.batch.clear();
        pos = 0;
        active = true;
    }
    bool take(size_t& neighbor, int64_t* order = nullptr) {
        if (!active) {
            return false;
        }
        if (pos == cursor.batch.size()) {
            cursor.next(slot, direction, false);
            if (cursor.batch.empty()) {
                active = false;
                return false;
            }
            pos = 0;
        }
        neighbor = cursor.batch[pos].neighbor;
        if (order) {
            *order = cursor.keys[pos];
        }
        ++pos;
        return true;
    }
    NativeCursor cursor;
    lbug::common::RelDataDirection direction;
    size_t slot = 0, pos = 0;
    bool active = false;
};

using FindIndexedParent = std::function<bool(const std::string&)>;
using AppendHistory = std::function<std::vector<AppendStep>(const FindIndexedParent&)>;

class NativeStore {
public:
    static std::shared_ptr<NativeStore> forDatabase(const std::string& mainPath);
    const std::string& databasePath() const { return mainPath; }
    std::shared_ptr<const NodeKeys> nodeKeys(const Source& source, const std::string& metadata,
        const std::function<NodeKeys()>& load);
    std::shared_ptr<NativeGeneration> pin(const std::string& name, const AppendHistory& lineage,
        const std::function<std::optional<AppendedOrders>(const AppendStep&)>& readAppend,
        const std::function<TopologyRows()>& build);

    std::shared_ptr<const StableNodes> stableNodes(LakeRead& lake, const Source& source);
    std::shared_ptr<NativeGeneration> stablePin(LakeRead& lake, const EdgeMapping& edge,
        const Source& from, const Source& to, const StableNodes& users, const StableNodes& products,
        int64_t orderSnapshot, bool refreshOnly);

private:
    void open(uint64_t bytes = 256ULL << 20);
    std::shared_ptr<NativeGeneration> openGeneration(const std::string& physical);
    std::map<std::string, std::pair<int64_t, std::shared_ptr<const StableNodes>>> stableNodeCache;
    explicit NativeStore(std::string mainPath) : mainPath{std::move(mainPath)} {}
    std::string mainPath;
    std::mutex mutex, keyMutex;
    // At most one immutable key vector per mapped node source. Older readers keep their copy.
    std::map<Source, std::pair<std::string, std::shared_ptr<const NodeKeys>>> nodeKeyCache;
    std::shared_ptr<lbug::main::Database> db;
};
} // namespace lbug::iceberg
