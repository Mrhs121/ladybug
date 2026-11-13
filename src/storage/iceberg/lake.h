#pragma once

#include <array>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types/value/value.h"
#include "transaction/transaction.h"
#include <span>

namespace lbug::iceberg {

class NativeStore;
struct NativeGeneration;
struct StableNodes;
struct LakeHistory;
struct AddedRows;

struct Source {
    std::string table, key;
    auto operator<=>(const Source&) const = default;
};

using NodeKeys = std::vector<int64_t>;

struct EdgeMapping {
    Source source;
    std::string from, to;
    explicit EdgeMapping(const std::string& storage);
};

struct Arc {
    size_t neighbor, edgeRow;
};

struct TopologyRows {
    size_t sources, destinations;
    // Source offset, destination offset, and edge row within this snapshot sorted by its key.
    std::vector<std::array<int64_t, 3>> edges;
    // Native COPY reads the keys from the pinned transaction while consuming these rows.
    std::span<const lbug::common::Value> edgeKeys;
};

struct AppendStep {
    std::string version, parent;
    int64_t snapshot, parentSnapshot, rows, parentRows;
};

struct AppendedOrders {
    std::vector<int64_t> keys;
    // Endpoint slots, append ordinal and Iceberg business key.
    std::vector<std::array<int64_t, 4>> edges;
    bool aboveParent = false;
    size_t files = 0;
};

struct CSR {
    std::vector<size_t> indptr;
    std::vector<Arc> arcs;
};

struct Adjacency {
    CSR forward, backward;
};

struct LookupColumn {
    std::string name;
    lbug::common::LogicalTypeID type;
};

// Lake columns and the selected topology generation are pinned to one main-db transaction.
class LakeRead final : public lbug::transaction::LocalCacheObject {
public:
    static LakeRead& get(const lbug::transaction::Transaction* txn);
    LakeRead(const std::string& catalog, int32_t scanThreads);
    ~LakeRead() override;

    using Column = std::vector<lbug::common::Value>;
    const Column& column(const Source& source, const std::string& name);
    using KeyedRows = std::unordered_map<int64_t, Column>;
    KeyedRows lookup(const Source& source, std::vector<int64_t> keys,
        const std::vector<LookupColumn>& properties, const std::string& filter = {},
        int64_t upper = 0);
    std::shared_ptr<const NodeKeys> orderedNodeKeys(const Source& source, NativeStore& store);
    std::optional<size_t> nodeOffset(const Source& source, int64_t key,
        NativeStore* store = nullptr);
    const std::string& metadataFile(const Source& source) const;
    const Adjacency& adjacency(lbug::common::table_id_t relID, const EdgeMapping& edge,
        const Source& from, const Source& to);
    std::shared_ptr<NativeGeneration> nativeGeneration(lbug::common::table_id_t relID,
        const EdgeMapping& edge, const Source& from, const Source& to, NativeStore& store,
        std::optional<int64_t> refreshTarget = std::nullopt);

    std::shared_ptr<const StableNodes> stableNodes(const Source& source, NativeStore& store);
    std::shared_ptr<NativeGeneration> stableGeneration(lbug::common::table_id_t relID,
        const EdgeMapping& edge, const Source& from, const Source& to, NativeStore& store,
        std::optional<int64_t> orderSnapshot = std::nullopt);

private:
    friend class NativeStore;
    std::string stableTag(const EdgeMapping& edge, const Source& from, const Source& to);
    const LakeHistory& history(const Source& source);
    std::shared_ptr<const StableNodes> stableNodesLocked(const Source& source, NativeStore& store);
    std::optional<AddedRows> readAdded(const Source& source, const AppendStep& step,
        const std::vector<std::string>& projection, bool checkBounds);
    std::string sortedVersion(const EdgeMapping& edge, const Source& from, const Source& to,
        std::optional<int64_t> orderSnapshot);
    std::map<Source, std::shared_ptr<LakeHistory>> histories;
    std::map<std::pair<std::string, Source>, std::shared_ptr<const StableNodes>> vertexViews;
    std::shared_ptr<const NodeKeys> nodeKeys(const Source& source, NativeStore& store);
    void fetch(const Source& source, const std::vector<std::string>& requested);
    TopologyRows mapEndpoints(const EdgeMapping& edge, const Source& from, const Source& to);
    std::optional<AppendedOrders> readAppend(const AppendStep& step, const EdgeMapping& edge,
        const Source& from, const Source& to, NativeStore& store);

    struct Reader;
    std::unique_ptr<Reader> reader;
    std::mutex mutex;
    std::map<Source, std::map<std::string, Column>> columns;
    std::map<lbug::common::table_id_t, Adjacency> indexes;
    // Attached graphs can have the same catalog table IDs and different stable slot assignments.
    std::map<std::pair<std::string, lbug::common::table_id_t>, std::shared_ptr<NativeGeneration>>
        generations;
};
} // namespace lbug::iceberg
