#include <algorithm>
#include <chrono>
#include <iostream>
#include <limits>
#include <set>
#include <stdexcept>

#include "catalog/catalog.h"
#include "common/constants.h"
#include "common/types/value/nested.h"
#include "native_csr.h"
#include "processor/result/flat_tuple.h"
#include "storage/storage_manager.h"

namespace lbug::iceberg {
using namespace lbug::common;
using namespace lbug::main;
using lbug::transaction::Transaction;

// Row counts are exclusive ends. The last slot in a full table is limit - 1.
static int64_t checkedRows(int64_t current, uint64_t added = 0) {
    constexpr auto limit = StorageConstants::MAX_NUM_ROWS_IN_TABLE;
    static_assert(limit <= std::numeric_limits<int64_t>::max());
    if (current < 0 || static_cast<uint64_t>(current) > limit ||
        added > limit - static_cast<uint64_t>(current)) {
        throw std::overflow_error(
            "Stable CSR row count outside supported capacity (current=" + std::to_string(current) +
            ", added=" + std::to_string(added) + ", limit=" + std::to_string(limit) + ")");
    }
    return current + static_cast<int64_t>(added);
}

void StableNodes::append(std::shared_ptr<const NodeKeys> keys) {
    if (!keys->empty()) {
        ends.push_back(checkedRows(size(), keys->size()));
        runs.push_back(std::move(keys));
    }
}
std::optional<int64_t> StableNodes::slot(int64_t k) const {
    int64_t start = 0;
    for (const auto& run : runs) {
        auto it = std::lower_bound(run->begin(), run->end(), k);
        if (it != run->end() && *it == k) {
            return start + (it - run->begin());
        }
        start += run->size();
    }
    return std::nullopt;
}
int64_t StableNodes::key(int64_t slot) const {
    auto i = std::upper_bound(ends.begin(), ends.end(), slot) - ends.begin();
    return runs.at(i)->at(slot - (i ? ends[i - 1] : 0));
}
size_t StableNodes::rank(int64_t slot) const {
    if (runs.size() == 1) {
        return slot;
    }
    auto k = key(slot);
    size_t rank = 0;
    for (const auto& run : runs) {
        rank += std::lower_bound(run->begin(), run->end(), k) - run->begin();
    }
    return rank;
}

int64_t LakeHistory::rows(int64_t id) const {
    if (id == -1) {
        return 0;
    }
    if (!snapshots.contains(id)) {
        throw std::runtime_error("Iceberg snapshot or record count unavailable");
    }
    return checkedRows(snapshots.at(id).rows);
}
std::vector<AppendStep> LakeHistory::after(int64_t base, int64_t target) const {
    std::vector<AppendStep> steps;
    while (target != base) {
        auto it = snapshots.find(target);
        if (it == snapshots.end() || !it->second.append || steps.size() >= snapshots.size()) {
            throw std::runtime_error("Stable CSR requires a retained append-only Iceberg lineage");
        }
        auto parent = it->second.parent;
        steps.push_back({{}, {}, target, parent, rows(target), rows(parent)});
        target = parent;
    }
    std::reverse(steps.begin(), steps.end());
    return steps;
}

static bool hasTable(Connection& c, const std::string& name) {
    auto& ctx = *c.getClientContext();
    return ctx.getDatabase()->getCatalog()->containsTable(Transaction::Get(ctx), name);
}

// Internal IDs are hashes and decimal integers. Lake names and values never enter these literals.
static std::string str(const std::string& id) {
    return "'" + id + "'";
}
static std::string num(std::optional<int64_t> value) {
    return value ? std::to_string(*value) : "NULL";
}
static std::string nodeVersion(const std::string& identity, int64_t snapshot) {
    return identity + "_" + std::to_string(snapshot);
}
static std::unique_ptr<QueryResult> byId(Connection& c, const std::string& table,
    const std::string& id, const std::string& columns) {
    return nativeQuery(c, "MATCH (v:" + table + " {id:" + str(id) + "}) RETURN " + columns);
}
static void makeNodeCatalog(Connection& c) {
    auto& ctx = *c.getClientContext();
    if (!hasTable(c, "CsrSlots") &&
        !ctx.getDatabase()->getCatalog()->getTableEntries(Transaction::Get(ctx)).empty()) {
        throw std::runtime_error("Expected a dedicated Iceberg CSR sidecar");
    }
    ensureSlots(c, 0);
    nativeQuery(c, "CREATE NODE TABLE StableNodeHeads(id STRING PRIMARY KEY, snap INT64)");
    nativeQuery(c,
        "CREATE NODE TABLE StableNodeVersions(id STRING PRIMARY KEY, prev STRING, n INT64)");
    nativeQuery(c, "CREATE NODE TABLE StableKeyChunks(id STRING PRIMARY KEY, data INT64[])");
}

// The position of a key in a persisted append run is its stable slot.
static constexpr size_t KEYS_PER_CHUNK = 4096;
static void saveKeys(Connection& c, const std::string& version, const std::string& previous,
    const NodeKeys& keys) {
    nativeQuery(c, "CREATE (:StableNodeVersions {id:" + str(version) + ",prev:" + str(previous) +
                       ",n:" + std::to_string(keys.size()) + "})");
    auto insert = c.prepare("CREATE (:StableKeyChunks {id:$id,data:$keys})");
    if (!insert->isSuccess()) {
        throw std::runtime_error(insert->getErrorMessage());
    }
    for (size_t off = 0; off < keys.size(); off += KEYS_PER_CHUNK) {
        auto n = std::min(KEYS_PER_CHUNK, keys.size() - off);
        std::vector<std::unique_ptr<Value>> values;
        values.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            values.push_back(std::make_unique<Value>(keys[off + i]));
        }
        std::unordered_map<std::string, std::unique_ptr<Value>> args;
        args["id"] = std::make_unique<Value>(version + "_" + std::to_string(off));
        args["keys"] =
            std::make_unique<Value>(LogicalType::LIST(LogicalType::INT64()), std::move(values));
        auto r = c.executeWithParams(insert.get(), std::move(args));
        if (!r->isSuccess()) {
            throw std::runtime_error(r->getErrorMessage());
        }
    }
}
static StableNodes loadKeys(Connection& c, std::string version) {
    std::vector<std::pair<std::string, int64_t>> runs;
    while (!version.empty()) {
        auto r = byId(c, "StableNodeVersions", version, "v.prev,v.n");
        if (!r->hasNext()) {
            throw std::runtime_error("Missing stable vertex dictionary version");
        }
        auto row = r->getNext();
        runs.emplace_back(version, row->getValue(1)->getValue<int64_t>());
        version = row->getValue(0)->getValue<std::string>();
    }
    StableNodes result;
    for (auto it = runs.rbegin(); it != runs.rend(); ++it) {
        checkedRows(result.size(), it->second);
        auto keys = std::make_shared<NodeKeys>(it->second);
        for (size_t off = 0; off < keys->size(); off += KEYS_PER_CHUNK) {
            auto r = byId(c, "StableKeyChunks", it->first + "_" + std::to_string(off), "v.data");
            if (!r->hasNext()) {
                throw std::runtime_error("Missing stable vertex dictionary chunk");
            }
            auto row = r->getNext();
            auto list = row->getValue(0);
            auto n = NestedVal::getChildrenSize(list);
            if (n != std::min(KEYS_PER_CHUNK, keys->size() - off)) {
                throw std::runtime_error("Invalid stable vertex dictionary chunk");
            }
            for (size_t i = 0; i < n; ++i) {
                keys->at(off + i) = NestedVal::getChildVal(list, i)->getValue<int64_t>();
            }
        }
        result.append(std::move(keys));
    }
    return result;
}

std::shared_ptr<const StableNodes> NativeStore::stableNodes(LakeRead& lake, const Source& source) {
    auto& history = lake.history(source);
    const auto expected = history.rows(history.current);
    std::lock_guard lock(mutex);
    auto& cached = stableNodeCache[history.identity];
    if (cached.second && cached.first == history.current) {
        return cached.second;
    }
    open(1ULL << 30);
    Connection c(db.get());
    nativeQuery(c, "BEGIN TRANSACTION");
    if (!hasTable(c, "StableNodeHeads")) {
        makeNodeCatalog(c);
    }
    auto version = nodeVersion(history.identity, history.current);
    auto view = std::make_shared<StableNodes>();
    if (byId(c, "StableNodeVersions", version, "v.n")->hasNext()) {
        if (cached.second && cached.second->size() >= expected) {
            for (size_t i = 0; i < cached.second->runs.size() && cached.second->ends[i] <= expected;
                 ++i) {
                view->append(cached.second->runs[i]);
            }
            if (view->size() != expected) {
                throw std::runtime_error("Stable prefix is not at a snapshot boundary");
            }
            nativeQuery(c, "COMMIT");
            return view; // Keep the head cached for the next writer.
        }
        *view = loadKeys(c, version);
    } else {
        auto h = byId(c, "StableNodeHeads", history.identity, "v.snap");
        if (h->hasNext()) {
            auto head = h->getNext()->getValue(0)->getValue<int64_t>();
            *view = cached.second && cached.first == head ?
                        *cached.second :
                        loadKeys(c, nodeVersion(history.identity, head));
            for (const auto& step : history.after(head, history.current)) {
                auto delta = lake.readAdded(source, step, {source.key}, false);
                if (!delta) {
                    throw std::runtime_error("Unsupported stable vertex append");
                }
                auto keys = std::make_shared<NodeKeys>();
                for (auto& r : delta->rows) {
                    if (view->slot(*r[0])) {
                        throw std::runtime_error("Duplicate stable vertex key in " + source.table);
                    }
                    keys->push_back(*r[0]);
                }
                saveKeys(c, nodeVersion(history.identity, step.snapshot),
                    nodeVersion(history.identity, step.parentSnapshot), *keys);
                view->append(std::move(keys));
                if (view->size() != step.rows) {
                    throw std::runtime_error("Stable vertex count differs from snapshot");
                }
                std::clog << "[stable_csr] vertex append " << source.table
                          << " files=" << delta->files << " keys=" << delta->rows.size() << '\n';
            }
            nativeQuery(c, "MATCH (h:StableNodeHeads {id:" + str(history.identity) +
                               "}) SET h.snap=" + num(history.current));
        } else {
            auto keys = lake.nodeKeys(source, *this);
            if (keys->size() != static_cast<uint64_t>(expected)) {
                throw std::runtime_error("Stable base key count mismatch");
            }
            saveKeys(c, version, "", *keys);
            view->append(keys);
            nativeQuery(c, "CREATE (:StableNodeHeads {id:" + str(history.identity) +
                               ",snap:" + num(history.current) + "})");
            std::clog << "[stable_csr] vertex baseline " << source.table << " keys=" << keys->size()
                      << '\n';
        }
    }
    nativeQuery(c, "COMMIT");
    cached = {history.current, view};
    return view;
}

struct StableVersion {
    int64_t us, os, ps, users, orders, products, edges;
    bool sorted;
};
static std::string triple(const std::string& tag, int64_t u, int64_t o, int64_t p) {
    return tag + "_" + num(u) + "_" + num(o) + "_" + num(p);
}
static std::optional<StableVersion> stored(Connection& c, const std::string& version) {
    auto r = byId(c, "StableGraphVersions", version,
        "v.us,v.os,v.ps,v.users,v.orders,v.products,v.edges,v.sorted_rows");
    if (!r->hasNext()) {
        return std::nullopt;
    }
    auto row = r->getNext();
    auto n = [&](int i) { return row->getValue(i)->getValue<int64_t>(); };
    return StableVersion{n(0), n(1), n(2), n(3), n(4), n(5), n(6),
        row->getValue(7)->getValue<bool>()};
}
static void saveGraph(Connection& c, const std::string& tag, const std::string& id,
    const StableVersion& v) {
    nativeQuery(c, "CREATE (:StableGraphVersions {id:" + str(id) + ",us:" + num(v.us) +
                       ",os:" + num(v.os) + ",ps:" + num(v.ps) + ",users:" + num(v.users) +
                       ",orders:" + num(v.orders) + ",products:" + num(v.products) + ",edges:" +
                       num(v.edges) + ",sorted_rows:" + (v.sorted ? "true" : "false") + "})");
    nativeQuery(c, "MERGE (h:StableGraphHeads {id:" + str(tag) + "}) SET h.version=" + str(id));
}
static void makeGraphCatalog(Connection& c) {
    nativeQuery(c, "CREATE NODE TABLE StableGraphHeads(id STRING PRIMARY KEY, version STRING)");
    nativeQuery(c,
        "CREATE NODE TABLE StableGraphVersions(id STRING PRIMARY KEY, us INT64,os INT64,ps "
        "INT64,users INT64,orders INT64,products INT64,edges INT64,sorted_rows BOOL)");
    nativeQuery(c, "CREATE NODE TABLE StableOrderVersions(id STRING PRIMARY KEY, rows INT64)");
}

struct PendingOrders {
    std::string orders, missing, needs;
    explicit PendingOrders(const std::string& tag)
        : orders{"Pending_" + tag}, missing{"Missing_" + tag}, needs{"WaitsFor_" + tag} {}
    void create(Connection& c) const {
        nativeQuery(c, "CREATE NODE TABLE " + orders +
                           "(oid INT64 PRIMARY KEY,src INT64,dst INT64,ord INT64)");
        nativeQuery(c, "CREATE NODE TABLE " + missing + "(id STRING PRIMARY KEY)");
        nativeQuery(c, "CREATE REL TABLE " + needs + "(FROM " + orders + " TO " + missing + ")");
    }
    static std::string bucket(char role, int64_t key) { return std::string(1, role) + num(key); }
    void add(Connection& c, int64_t oid, std::optional<int64_t> src, std::optional<int64_t> dst,
        int64_t ord, bool missU, bool missP) const {
        nativeQuery(c, "CREATE (:" + orders + " {oid:" + num(oid) + ",src:" + num(src) +
                           ",dst:" + num(dst) + ",ord:" + num(ord) + "})");
        auto wait = [&](char role, int64_t key) {
            nativeQuery(c, "MATCH (o:" + orders + " {oid:" + num(oid) + "}) MERGE (w:" + missing +
                               " {id:" + str(bucket(role, key)) + "}) CREATE (o)-[:" + needs +
                               "]->(w)");
        };
        if (src && dst) {
            if (missU) {
                wait('U', *src);
            }
            if (missP) {
                wait('P', *dst);
            }
        }
    }
    // Indexed bucket PK -> native reverse adjacency, only orders waiting for this vertex.
    void wake(Connection& c, char role, int64_t key, const StableNodes& users,
        const StableNodes& products, AppendedOrders& ready) const {
        auto id = str(bucket(role, key));
        auto r = nativeQuery(c, "MATCH (w:" + missing + " {id:" + id + "})<-[:" + needs +
                                    "]-(o:" + orders + ") RETURN o.oid,o.src,o.dst,o.ord");
        while (r->hasNext()) {
            auto row = r->getNext();
            auto oid = row->getValue(0)->getValue<int64_t>();
            auto u = users.slot(row->getValue(1)->getValue<int64_t>()),
                 p = products.slot(row->getValue(2)->getValue<int64_t>());
            if (u && p) {
                ready.edges.push_back({*u, *p, row->getValue(3)->getValue<int64_t>(), oid});
                nativeQuery(c, "MATCH (o:" + orders + " {oid:" + num(oid) + "}) DETACH DELETE o");
            }
        }
        nativeQuery(c, "MATCH (w:" + missing + " {id:" + id + "}) DETACH DELETE w");
    }
};

std::shared_ptr<NativeGeneration> NativeStore::stablePin(LakeRead& lake, const EdgeMapping& edge,
    const Source& from, const Source& to, const StableNodes& users, const StableNodes& products,
    int64_t orderSnapshot, bool refreshOnly) {
    const auto& uh = lake.history(from);
    const auto& oh = lake.history(edge.source);
    const auto& ph = lake.history(to);
    if (refreshOnly && orderSnapshot != oh.current) {
        oh.after(orderSnapshot, oh.current);
    }
    auto tag = lake.stableTag(edge, from, to);
    const auto physical = "StableEdges_" + tag,
               id = triple(tag, uh.current, orderSnapshot, ph.current);
    PendingOrders pending(tag);
    std::lock_guard lock(mutex);
    open(1ULL << 30);
    Connection c(db.get());
    nativeQuery(c, "BEGIN TRANSACTION");
    if (!hasTable(c, "StableGraphHeads")) {
        makeGraphCatalog(c);
    }
    auto head = byId(c, "StableGraphHeads", tag, "v.version");
    auto tip =
        head->hasNext() ? head->getNext()->getValue(0)->getValue<std::string>() : std::string{};
    auto exact = stored(c, id);
    bool historical = !tip.empty() && tip != id;
    if (!exact) {
        auto start = std::chrono::steady_clock::now();
        StableVersion v{uh.current, orderSnapshot, ph.current, users.size(), oh.rows(orderSnapshot),
            products.size(), 0, true};
        if (tip.empty()) {
            if (orderSnapshot != oh.current) {
                throw std::runtime_error("Initialize the stable CSR on its current baseline first");
            }
            ensureSlots(c, std::max(users.size(), products.size()));
            pending.create(c);
            auto legacy = lake.sortedVersion(edge, from, to, std::nullopt);
            bool adopt = users.runs.size() <= 1 && products.runs.size() <= 1 &&
                         hasTable(c, legacy) && !hasTable(c, "CsrVersions");
            if (adopt) {
                // The existing immutable native table is already numbered in sorted baseline order.
                auto check = nativeQuery(c, "MATCH ()-[e:" + legacy + "]->() RETURN count(*)");
                adopt = check->getNext()->getValue(0)->getValue<int64_t>() == v.orders;
            }
            if (adopt) {
                nativeQuery(c, "ALTER TABLE " + legacy + " RENAME TO " + physical);
                v.edges = v.orders;
                std::clog << "[stable_csr] adopted existing CSR edges=" << v.edges << '\n';
            } else {
                auto rows = lake.mapEndpoints(edge, from, to);
                auto& src = lake.columns.at(edge.source).at(edge.from);
                auto& dst = lake.columns.at(edge.source).at(edge.to);
                size_t matched = 0;
                for (size_t i = 0; i < rows.edgeKeys.size(); ++i) {
                    auto a =
                        src[i].isNull() ? std::nullopt : std::optional{src[i].getValue<int64_t>()};
                    auto b =
                        dst[i].isNull() ? std::nullopt : std::optional{dst[i].getValue<int64_t>()};
                    auto u = a ? users.slot(*a) : std::nullopt,
                         p = b ? products.slot(*b) : std::nullopt;
                    if (u && p) {
                        auto& mapped = rows.edges.at(matched++);
                        if (mapped[2] != static_cast<int64_t>(i)) {
                            throw std::runtime_error("Stable baseline endpoints disagree");
                        }
                        mapped[0] = *u;
                        mapped[1] = *p;
                    } else {
                        pending.add(c, rows.edgeKeys[i].getValue<int64_t>(), a, b, i, !u, !p);
                    }
                }
                nativeQuery(c, "CREATE REL TABLE " + physical +
                                   "(FROM CsrSlots TO CsrSlots,lake_row INT64,lake_key INT64)");
                if (!rows.edges.empty()) {
                    copyTopology(c, physical, rows);
                }
                v.edges = rows.edges.size();
                std::clog << "[stable_csr] built baseline edges=" << v.edges << '\n';
            }
            nativeQuery(c, "CREATE (:StableOrderVersions {id:" + str(nodeVersion(tag, v.os)) +
                               ",rows:" + num(v.orders) + "})");
        } else {
            auto parent = stored(c, tip).value();
            auto known =
                byId(c, "StableOrderVersions", nodeVersion(tag, v.os), "v.rows")->hasNext();
            // An earlier, never-traversed combination of three individually indexed snapshots.
            if (known && v.users <= parent.users && v.products <= parent.products &&
                v.orders <= parent.orders &&
                (v.users < parent.users || v.products < parent.products ||
                    v.orders < parent.orders)) {
                auto r = nativeQuery(c, "MATCH (s:CsrSlots)-[e:" + physical +
                                            "]->(d:CsrSlots) WHERE s.pos<" + num(v.users) +
                                            " AND d.pos<" + num(v.products) + " AND e.lake_row<" +
                                            num(v.orders) + " RETURN count(*)");
                v.edges = r->getNext()->getValue(0)->getValue<int64_t>();
                v.sorted = parent.sorted;
                exact = v; // Query-local historical view; never move the durable head backwards.
            } else {
                uh.after(parent.us, v.us);
                ph.after(parent.ps, v.ps);
                auto steps = oh.after(parent.os, v.os);
                if (v.users > parent.users || v.products > parent.products) {
                    ensureSlots(c, std::max(v.users, v.products));
                }
                AppendedOrders ready;
                for (int64_t i = parent.users; i < v.users; ++i) {
                    pending.wake(c, 'U', users.key(i), users, products, ready);
                }
                for (int64_t i = parent.products; i < v.products; ++i) {
                    pending.wake(c, 'P', products.key(i), users, products, ready);
                }
                auto promoted = ready.edges.size();
                v.edges = checkedRows(parent.edges, promoted);
                writeEdges(c, physical, ready);
                v.sorted = parent.sorted;
                size_t orderRows = 0, files = 0, queued = 0;
                for (const auto& step : steps) {
                    auto added = lake.readAdded(edge.source, step,
                        {edge.source.key, edge.from, edge.to}, true);
                    if (!added) {
                        throw std::runtime_error("Unsupported stable order append");
                    }
                    ready.edges.clear();
                    int64_t ord = step.parentRows;
                    for (const auto& r : added->rows) {
                        auto k = *r[0];
                        auto u = r[1] ? users.slot(*r[1]) : std::nullopt,
                             p = r[2] ? products.slot(*r[2]) : std::nullopt;
                        if (!added->aboveParent &&
                            (nativeQuery(c, "MATCH ()-[e:" + physical + "]->() WHERE e.lake_key=" +
                                                num(k) + " RETURN 1 LIMIT 1")
                                    ->hasNext() ||
                                nativeQuery(c, "MATCH (o:" + pending.orders + " {oid:" + num(k) +
                                                   "}) RETURN 1")
                                    ->hasNext())) {
                            throw std::runtime_error("Duplicate order key in stable CSR");
                        }
                        if (u && p) {
                            ready.edges.push_back({*u, *p, ord, k});
                        } else {
                            pending.add(c, k, r[1], r[2], ord, !u, !p);
                            ++queued;
                        }
                        ++ord;
                    }
                    v.edges = checkedRows(v.edges, ready.edges.size());
                    writeEdges(c, physical, ready);
                    v.sorted &= added->aboveParent;
                    orderRows += added->rows.size();
                    files += added->files;
                    nativeQuery(c,
                        "CREATE (:StableOrderVersions {id:" + str(nodeVersion(tag, step.snapshot)) +
                            ",rows:" + num(step.rows) + "})");
                }
                std::clog << "[stable_csr] delta users=" << (v.users - parent.users)
                          << " products=" << (v.products - parent.products)
                          << " order_files=" << files << " order_rows=" << orderRows
                          << " queued=" << queued << " promoted=" << promoted
                          << " edges=" << v.edges << '\n';
            }
        }
        if (!exact) {
            saveGraph(c, tag, id, v);
            exact = v;
            historical = false;
        }
        nativeQuery(c, "COMMIT");
        if (!historical) {
            std::clog << "[stable_csr] graph published ms="
                      << std::chrono::duration<double, std::milli>(
                             std::chrono::steady_clock::now() - start)
                             .count()
                      << " unix_ns="
                      << std::chrono::duration_cast<std::chrono::nanoseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count()
                      << '\n';
        }
    } else {
        nativeQuery(c, "COMMIT");
    }
    auto g = openGeneration(physical);
    g->edgeCount = exact->edges;
    g->sortedRows = exact->sorted;
    if (historical) {
        g->rowEnd = exact->orders;
        g->sourceEnd = exact->users;
        g->destinationEnd = exact->products;
    }
    return g;
}
} // namespace lbug::iceberg
