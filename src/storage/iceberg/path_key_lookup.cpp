// Fixed-length, anchored paths over one persisted mapping. Each hop has its own native cursor.
#include <numeric>

#include "binder/expression/aggregate_function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/bound_regular_query.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/exception/interrupt.h"
#include "key_lookup.h"
#include "main/client_context.h"
#include "main/database_manager.h"
#include "native_csr.h"
#include "planner/operator/logical_table_function_call.h"
#include <bit>

namespace lbug::iceberg {
using namespace lbug::binder;
using namespace lbug::catalog;
using namespace lbug::common;
using namespace lbug::function;
using namespace lbug::main;
using namespace lbug::planner;
using lbug::transaction::Transaction;

namespace {
using Pattern = std::shared_ptr<NodeOrRelExpression>;

struct KeyTest {
    size_t left;
    ExpressionType comparison;
    std::optional<size_t> right;
    int64_t literal;
    bool accepts(const std::vector<int64_t>& keys) const {
        return compareKeys(comparison, keys[left], right ? keys[*right] : literal);
    }
};

struct PathVariable {
    Source source;
    std::vector<LookupColumn> payload;
    std::string filter;
    int64_t upper = 0;
};

struct PathColumn {
    size_t variable;
    std::optional<size_t> payload;
};

struct PathLookupPlan final : TableFuncBindData {
    PathLookupPlan(const RelGroupCatalogEntry& rel, const NodeTableCatalogEntry& src,
        const NodeTableCatalogEntry& dst, std::string owner)
        : relID{rel.getRelEntryInfos()[0].oid},
          stable{rel.getStorageFormatName() == "iceberg_stable_csr"}, dbName{std::move(owner)},
          edge{rel.getStorage()}, users{src.getStorage(), src.getPrimaryKeyName()},
          products{dst.getStorage(), dst.getPrimaryKeyName()} {}
    table_id_t relID;
    bool stable;
    std::string dbName;
    EdgeMapping edge;
    Source users, products;
    int64_t anchor = 0;
    std::vector<RelDataDirection> directions;
    // Alternating node and edge variables, starting with the anchor. Tests run as soon as
    // both operands exist. An edge and its destination are bound in the same cursor step.
    std::vector<PathVariable> variables;
    std::vector<std::vector<KeyTest>> tests;
    std::vector<PathColumn> output;
    std::vector<size_t> bufferedVariables;
    size_t lookupWindow = 0;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<PathLookupPlan>(*this);
    }
    std::string getDescription() const override {
        return "ICEBERG_CSR_PATH_LOOKUP (" + std::to_string(directions.size()) +
               " hops, native CSR, DuckDB candidate key lookup)";
    }
};

bool isIntegerLiteral(const Expression& e) {
    return e.expressionType == ExpressionType::LITERAL &&
           e.getDataType().getLogicalTypeID() == LogicalTypeID::INT64 &&
           !e.constCast<LiteralExpression>().value.isNull();
}

std::shared_ptr<NodeExpression> findAnchor(const QueryGraph& graph, const BoundMatchClause& match,
    int64_t& anchor) {
    for (auto& pred : match.getConjunctivePredicates()) {
        if (pred->expressionType != ExpressionType::EQUALS || pred->getNumChildren() != 2) {
            continue;
        }
        auto lhs = pred->getChild(0), rhs = pred->getChild(1);
        if (isIntegerLiteral(*lhs)) {
            std::swap(lhs, rhs);
        }
        if (lhs->expressionType != ExpressionType::PROPERTY || !isIntegerLiteral(*rhs)) {
            continue;
        }
        const auto& prop = lhs->constCast<PropertyExpression>();
        if (!prop.isPrimaryKey() || !graph.containsQueryNode(prop.getVariableName())) {
            continue;
        }
        auto start = graph.getQueryNode(prop.getVariableName());
        const auto edges = graph.getQueryRels();
        if (std::ranges::count_if(edges, [&](const auto& e) {
                return e->getSrcNodeName() == start->getUniqueName() ||
                       e->getDstNodeName() == start->getUniqueName();
            }) == 1) {
            anchor = rhs->constCast<LiteralExpression>().value.getValue<int64_t>();
            return start;
        }
    }
    return nullptr;
}

// The binder can enumerate edges in a different order from the path starting at the anchor.
std::vector<Pattern> bindWalk(PathLookupPlan& plan, const QueryGraph& graph,
    std::shared_ptr<NodeExpression> node) {
    auto remaining = graph.getQueryRels();
    std::vector<Pattern> vars{node};
    while (!remaining.empty()) {
        auto incident = [&](const auto& e) {
            return e->getSrcNodeName() == node->getUniqueName() ||
                   e->getDstNodeName() == node->getUniqueName();
        };
        auto it = std::ranges::find_if(remaining, incident);
        if (it == remaining.end() ||
            std::find_if(it + 1, remaining.end(), incident) != remaining.end()) {
            return {};
        }
        auto rel = *it;
        remaining.erase(it);
        bool forward = rel->getSrcNodeName() == node->getUniqueName();
        node = forward ? rel->getDstNode() : rel->getSrcNode();
        vars.push_back(rel);
        vars.push_back(node);
        plan.directions.push_back(forward ? RelDataDirection::FWD : RelDataDirection::BWD);
        if (plan.variables.empty()) {
            plan.variables.push_back({forward ? plan.users : plan.products, {}, {}});
        }
        plan.variables.push_back({plan.edge.source, {}, {}});
        plan.variables.push_back({forward ? plan.products : plan.users, {}, {}});
    }
    plan.tests.resize(plan.directions.size() + 1);
    return vars;
}

using VarIndex = std::unordered_map<std::string, size_t>;

bool bindTest(PathLookupPlan& plan, const VarIndex& index, const std::vector<Pattern>& vars,
    const Expression& pred) {
    if (!ExpressionTypeUtil::isComparison(pred.expressionType) || pred.getNumChildren() != 2) {
        return false;
    }
    auto lhs = pred.getChild(0), rhs = pred.getChild(1);
    auto op = pred.expressionType;
    if (isIntegerLiteral(*lhs)) {
        std::swap(lhs, rhs);
        op = ExpressionTypeUtil::reverseComparisonDirection(op);
    }
    if (lhs->expressionType != ExpressionType::PROPERTY) {
        return false;
    }
    const auto& p = lhs->constCast<PropertyExpression>();
    if (!index.contains(p.getVariableName())) {
        return false;
    }
    const auto left = index.at(p.getVariableName());
    auto& var = plan.variables[left];
    KeyTest test{left, op, std::nullopt, 0};
    if (isIntegerLiteral(*rhs)) {
        if (p.getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
            return false;
        }
        test.literal = rhs->constCast<LiteralExpression>().value.getValue<int64_t>();
        if (p.getPropertyName() != var.source.key) {
            if (op != ExpressionType::LESS_THAN || !var.filter.empty()) {
                return false;
            }
            var.filter = p.getPropertyName();
            var.upper = test.literal;
            return true;
        }
    } else if (rhs->expressionType == ExpressionType::PROPERTY) {
        const auto& q = rhs->constCast<PropertyExpression>();
        if (!index.contains(q.getVariableName())) {
            return false;
        }
        auto right = index.at(q.getVariableName());
        // Business keys preserve equality of internal IDs within one table. Their ordering
        // can differ from stable slot ordering after interleaved appends.
        if (p.isInternalID() || q.isInternalID()) {
            if (!p.isInternalID() || !q.isInternalID() ||
                vars[left]->getEntry(0) != vars[right]->getEntry(0) ||
                (op != ExpressionType::EQUALS && op != ExpressionType::NOT_EQUALS)) {
                return false;
            }
        } else if (p.getPropertyName() != var.source.key ||
                   q.getPropertyName() != plan.variables[right].source.key) {
            return false;
        }
        test.right = right;
    } else {
        return false;
    }
    const size_t last = std::max(left, test.right.value_or(left));
    plan.tests[(last + 1) / 2].push_back(test);
    return true;
}

bool bindColumn(PathLookupPlan& plan, const VarIndex& index, const std::shared_ptr<Expression>& e) {
    if (e->expressionType == ExpressionType::AGGREGATE_FUNCTION) {
        const auto& agg = e->constCast<AggregateFunctionExpression>();
        return agg.getFunction().name == "COUNT_STAR" && !agg.isDistinct() &&
               agg.getNumChildren() == 0;
    }
    if (e->expressionType != ExpressionType::PROPERTY) {
        return false;
    }
    const auto& p = e->constCast<PropertyExpression>();
    const auto type = p.getDataType().getLogicalTypeID();
    if (!index.contains(p.getVariableName()) ||
        (type != LogicalTypeID::STRING && type != LogicalTypeID::INT64)) {
        return false;
    }
    if (std::ranges::any_of(plan.columns, [&](const auto& col) { return *col == *e; })) {
        return true;
    }
    const size_t slot = index.at(p.getVariableName());
    auto& var = plan.variables[slot];
    PathColumn out{slot, std::nullopt};
    if (p.getPropertyName() != var.source.key) {
        out.payload = addColumn(var.payload, p);
    }
    plan.output.push_back(out);
    plan.columns.push_back(e);
    return true;
}

std::unique_ptr<PathLookupPlan> bindPath(const QueryGraph& graph, const BoundMatchClause& match,
    const BoundProjectionBody& body, ClientContext& ctx) {
    if (graph.getNumQueryRels() < 3 || graph.getNumQueryNodes() != graph.getNumQueryRels() + 1) {
        return nullptr;
    }
    const auto first = graph.getQueryRel(0);
    for (auto& pattern : graph.getAllPatterns()) {
        if (pattern->getNumEntries() != 1 || !pattern->getPropertyDataExprRef().empty()) {
            return nullptr;
        }
    }
    auto r = first->getEntry(0);
    auto u = first->getSrcNode()->getEntry(0), p = first->getDstNode()->getEntry(0);
    if (r->getType() != CatalogEntryType::REL_GROUP_ENTRY ||
        u->getType() != CatalogEntryType::NODE_TABLE_ENTRY ||
        p->getType() != CatalogEntryType::NODE_TABLE_ENTRY) {
        return nullptr;
    }
    const auto& rel = r->constCast<RelGroupCatalogEntry>();
    const auto& src = u->constCast<NodeTableCatalogEntry>();
    const auto& dst = p->constCast<NodeTableCatalogEntry>();
    const auto format = rel.getStorageFormatName();
    if ((format != "iceberg_stable_csr" && format != "iceberg_csr") ||
        src.getStorageFormatName() != format || dst.getStorageFormatName() != format ||
        rel.getRelEntryInfos().size() != 1) {
        return nullptr;
    }
    const auto dbName = first->getDbName(r);
    for (auto& e : graph.getQueryRels()) {
        if (e->isRecursive() || e->getDirectionType() != RelDirectionType::SINGLE ||
            e->getEntry(0) != r || e->getSrcNode()->getEntry(0) != u ||
            e->getDstNode()->getEntry(0) != p || e->getDbName(r) != dbName ||
            e->getSrcNode()->getDbName(u) != dbName || e->getDstNode()->getDbName(p) != dbName) {
            return nullptr;
        }
    }
    const auto& info = rel.getRelEntryInfos()[0];
    auto* catalog = DatabaseManager::resolveTableStorage(ctx, info.oid, dbName).first;
    if (info.nodePair.srcTableID != src.getTableID() ||
        info.nodePair.dstTableID != dst.getTableID() ||
        catalog->getTableCatalogEntry(Transaction::Get(ctx), rel.getName()) != r) {
        return nullptr;
    }
    auto plan = std::make_unique<PathLookupPlan>(rel, src, dst, dbName);
    auto anchor = findAnchor(graph, match, plan->anchor);
    if (!anchor) {
        return nullptr;
    }
    auto vars = bindWalk(*plan, graph, anchor);
    if (vars.empty()) {
        return nullptr;
    }
    VarIndex index;
    for (size_t i = 0; i < vars.size(); ++i) {
        if (!index.emplace(vars[i]->getUniqueName(), i).second) {
            return nullptr;
        }
    }
    for (auto& pred : match.getConjunctivePredicates()) {
        if (!bindTest(*plan, index, vars, *pred)) {
            return nullptr;
        }
    }
    for (auto& col : body.getProjectionExpressions()) {
        if (!bindColumn(*plan, index, col)) {
            return nullptr;
        }
    }
    for (auto& col : body.getOrderByExpressions()) {
        if (!bindColumn(*plan, index, col)) {
            return nullptr;
        }
    }
    if (std::ranges::none_of(plan->variables,
            [](const auto& v) { return !v.filter.empty() || !v.payload.empty(); })) {
        return nullptr; // Key-only traversals already use the native graph operators.
    }
    if (plan->columns.empty()) {
        // COUNT(*) still needs a vector of the right cardinality for the native aggregate.
        bindColumn(*plan, index, anchor->getPrimaryKey(anchor->getEntry(0)->getTableID()));
    }
    // Comparisons on topology keys are resolved by the cursor. Only keep keys needed
    // to filter or emit a completed path, regardless of how many hops it traversed.
    for (size_t i = 0; i < plan->variables.size(); ++i) {
        if (!plan->variables[i].filter.empty() ||
            std::ranges::any_of(plan->output, [i](auto field) { return field.variable == i; })) {
            plan->bufferedVariables.push_back(i);
        }
    }
    constexpr size_t keyWindowBytes = 8 * 1024 * 1024;
    const auto bytesPerPath = sizeof(size_t) + sizeof(int64_t) * plan->bufferedVariables.size();
    plan->lookupWindow = std::bit_floor(std::max(size_t{1}, keyWindowBytes / bytesPerPath));
    if (body.hasLimit() && !body.hasOrderByExpressions() && !body.hasAggregateExpressions()) {
        plan->lookupWindow = std::min<size_t>(plan->lookupWindow, DEFAULT_VECTOR_CAPACITY);
    }
    return plan;
}

struct PathScan final : TableFuncLocalState {
    PathScan(LakeRead& lake, ClientContext& context, size_t numVariables)
        : lake{lake}, context{context}, pathKeys(numVariables), payload(numVariables) {}
    LakeRead& lake;
    ClientContext& context;
    std::vector<Neighbors> cursors;
    std::vector<int64_t> keys;
    std::shared_ptr<const StableNodes> stable[2];
    std::shared_ptr<const NodeKeys> sorted[2];
    size_t depth = 0;
    bool exhausted = true;
    std::vector<std::vector<int64_t>> pathKeys;
    std::vector<LakeRead::KeyedRows> payload;
    std::vector<size_t> selected;
    size_t nextRow = 0;
    int64_t businessKey(size_t side, size_t slot) const {
        return stable[side] ? stable[side]->key(slot) : sorted[side]->at(slot);
    }
    bool accepts(const PathLookupPlan& plan, size_t bound) const {
        return std::ranges::all_of(plan.tests[bound],
            [&](const KeyTest& test) { return test.accepts(keys); });
    }
    // Leave the cursor on the last edge just emitted. The next batch resumes at its sibling.
    bool next(const PathLookupPlan& plan) {
        while (!exhausted) {
            if (context.interrupted()) {
                throw InterruptException{};
            }
            size_t neighbor;
            int64_t order;
            if (!cursors[depth].take(neighbor, &order)) {
                if (depth == 0) {
                    exhausted = true;
                } else {
                    --depth;
                }
                continue;
            }
            keys[2 * depth + 1] = order;
            keys[2 * depth + 2] =
                businessKey(plan.directions[depth] == RelDataDirection::FWD, neighbor);
            if (!accepts(plan, depth + 1)) {
                continue;
            }
            if (depth + 1 == cursors.size()) {
                return true;
            }
            cursors[++depth].seek(neighbor);
        }
        return false;
    }

    void readWindow(const PathLookupPlan& plan) {
        nextRow = 0;
        selected.clear();
        for (auto i : plan.bufferedVariables) {
            pathKeys[i].clear();
            payload[i].clear();
        }
        size_t count = 0;
        while (count < plan.lookupWindow && next(plan)) {
            for (auto i : plan.bufferedVariables) {
                pathKeys[i].push_back(keys[i]);
            }
            ++count;
        }
        selected.resize(count);
        std::iota(selected.begin(), selected.end(), size_t{0});
        auto lookup = [&](size_t i) {
            std::vector<int64_t> candidates;
            candidates.reserve(selected.size());
            for (auto row : selected) {
                candidates.push_back(pathKeys[i][row]);
            }
            const auto& var = plan.variables[i];
            payload[i] =
                lake.lookup(var.source, std::move(candidates), var.payload, var.filter, var.upper);
        };
        // Apply each predicate before collecting keys for the next lake read.
        for (auto i : plan.bufferedVariables) {
            if (!plan.variables[i].filter.empty() && !selected.empty()) {
                lookup(i);
                std::erase_if(selected,
                    [&](size_t row) { return !payload[i].contains(pathKeys[i][row]); });
            }
        }
        for (auto i : plan.bufferedVariables) {
            const auto& var = plan.variables[i];
            if (var.filter.empty() && !var.payload.empty() && !selected.empty()) {
                lookup(i);
            }
        }
    }
};

std::unique_ptr<TableFuncLocalState> initPath(const TableFuncInitLocalStateInput& in) {
    auto& plan = in.bindData.cast<PathLookupPlan>();
    auto& ctx = *in.clientContext;
    auto& lake = LakeRead::get(Transaction::Get(ctx));
    auto* sm = DatabaseManager::resolveTableStorage(ctx, plan.relID, plan.dbName).second;
    auto store = NativeStore::forDatabase(sm->getDatabasePath());
    auto state = std::make_unique<PathScan>(lake, ctx, plan.variables.size());
    auto gen = plan.stable ?
                   lake.stableGeneration(plan.relID, plan.edge, plan.users, plan.products, *store) :
                   lake.nativeGeneration(plan.relID, plan.edge, plan.users, plan.products, *store);
    state->cursors.reserve(plan.directions.size());
    for (auto dir : plan.directions) {
        state->cursors.emplace_back(gen, dir, true);
    }
    state->keys.resize(plan.variables.size());
    state->keys[0] = plan.anchor;
    const bool startsAtProduct = plan.directions.front() == RelDataDirection::BWD;
    std::optional<size_t> origin;
    if (plan.stable) {
        state->stable[0] = lake.stableNodes(plan.users, *store);
        state->stable[1] = lake.stableNodes(plan.products, *store);
        origin = state->stable[startsAtProduct]->slot(plan.anchor);
    } else {
        state->sorted[0] = lake.orderedNodeKeys(plan.users, *store);
        state->sorted[1] = lake.orderedNodeKeys(plan.products, *store);
        origin = lake.nodeOffset(plan.variables[0].source, plan.anchor, store.get());
    }
    if (origin && state->accepts(plan, 0)) {
        state->exhausted = false;
        state->cursors.front().seek(*origin);
    }
    return state;
}

offset_t scanPaths(const TableFuncInput& in, TableFuncOutput& out) {
    auto& scan = *in.localState->ptrCast<PathScan>();
    const auto& plan = in.bindData->cast<PathLookupPlan>();
    while (scan.nextRow == scan.selected.size()) {
        if (scan.exhausted) {
            return 0;
        }
        scan.readWindow(plan);
    }
    const auto count =
        std::min<size_t>(DEFAULT_VECTOR_CAPACITY, scan.selected.size() - scan.nextRow);
    for (size_t col = 0; col < plan.output.size(); ++col) {
        const auto field = plan.output[col];
        auto& vec = out.dataChunk.getValueVectorMutable(col);
        for (size_t j = 0; j < count; ++j) {
            const auto key = scan.pathKeys[field.variable][scan.selected[scan.nextRow + j]];
            if (field.payload) {
                vec.copyFromValue(j, scan.payload[field.variable].at(key)[*field.payload]);
            } else {
                vec.setValue<int64_t>(j, key);
            }
        }
    }
    scan.nextRow += count;
    return count;
}
} // namespace

std::shared_ptr<LogicalOperator> tryPlanPathLookup(const BoundRegularQuery& q, ClientContext& ctx) {
    if (q.getNumSingleQueries() != 1 || q.getSingleQuery(0)->getNumQueryParts() != 1) {
        return nullptr;
    }
    auto* part = q.getSingleQuery(0)->getQueryPart(0);
    if (part->getNumReadingClause() != 1 || part->hasUpdatingClause() ||
        !part->hasProjectionBody() || part->hasProjectionBodyPredicate() ||
        part->getReadingClause(0)->getClauseType() != ClauseType::MATCH) {
        return nullptr;
    }
    const auto& match = part->getReadingClause(0)->constCast<BoundMatchClause>();
    auto* graphs = match.getQueryGraphCollection();
    if (match.hasHint() || match.getMatchClauseType() != MatchClauseType::MATCH ||
        graphs->getNumQueryGraphs() != 1) {
        return nullptr;
    }
    const auto* body = part->getProjectionBody();
    auto plan = bindPath(*graphs->getQueryGraph(0), match, *body, ctx);
    if (!plan) {
        return nullptr;
    }
    TableFunction function("ICEBERG_CSR_PATH_LOOKUP", {});
    function.canParallelFunc = [] { return false; };
    function.initSharedStateFunc = TableFunction::initEmptySharedState;
    function.initLocalStateFunc = initPath;
    function.tableFunc = scanPaths;
    LogicalPlan result;
    auto lookup = std::make_shared<LogicalTableFunctionCall>(std::move(function), std::move(plan));
    lookup->computeFactorizedSchema();
    result.setLastOperator(std::move(lookup));
    Planner(&ctx).planProjectionBody(body, result);
    return result.getLastOperator();
}
} // namespace lbug::iceberg
