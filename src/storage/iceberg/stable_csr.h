#pragma once

#include "lake.h"
#include "main/connection.h"

namespace lbug::iceberg {
// Each append contributes one sorted run. Its keys keep the slots assigned at publication.
struct StableNodes {
    std::vector<std::shared_ptr<const NodeKeys>> runs;
    std::vector<int64_t> ends;
    int64_t size() const { return ends.empty() ? 0 : ends.back(); }
    void append(std::shared_ptr<const NodeKeys> keys);
    std::optional<int64_t> slot(int64_t key) const;
    int64_t key(int64_t slot) const;
    size_t rank(int64_t slot) const;
};

struct LakeSnapshot {
    int64_t parent, rows;
    bool append;
};
struct LakeHistory {
    std::string identity;
    int64_t current;
    std::map<int64_t, LakeSnapshot> snapshots;
    std::vector<AppendStep> after(int64_t base, int64_t target) const;
    int64_t rows(int64_t snapshot) const;
};
struct AddedRows {
    size_t files = 0;
    bool aboveParent = true;
    // The projection starts with the unique key, followed by the requested endpoints.
    std::vector<std::array<std::optional<int64_t>, 3>> rows;
};

std::unique_ptr<lbug::main::QueryResult> nativeQuery(lbug::main::Connection& c,
    const std::string& sql);
void copyTopology(lbug::main::Connection& writer, const std::string& name,
    const TopologyRows& rows);
void writeEdges(lbug::main::Connection& writer, const std::string& name,
    const AppendedOrders& delta);
void ensureSlots(lbug::main::Connection& writer, size_t needed);
} // namespace lbug::iceberg
