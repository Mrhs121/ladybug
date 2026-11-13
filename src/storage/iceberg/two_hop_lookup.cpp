#include <stdexcept>
#include <unordered_set>

#include "binder/expression/aggregate_function_expression.h"
#include "binder/expression/literal_expression.h"
#include "binder/query/bound_regular_query.h"
#include "binder/query/reading_clause/bound_match_clause.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "key_lookup.h"
#include "main/client_context.h"
#include "main/database_manager.h"
#include "native_csr.h"
#include "planner/operator/logical_order_by.h"
#include "planner/operator/logical_table_function_call.h"
#include "storage/storage_manager.h"

namespace lbug::iceberg {
using namespace lbug::binder;
using namespace lbug::catalog;
using namespace lbug::common;
using namespace lbug::function;
using namespace lbug::main;
using namespace lbug::planner;
using lbug::transaction::Transaction;

bool compareKeys(ExpressionType comparison, int64_t key, int64_t literal) {
    switch (comparison) {
    case ExpressionType::EQUALS:
        return key == literal;
    case ExpressionType::NOT_EQUALS:
        return key != literal;
    case ExpressionType::LESS_THAN:
        return key < literal;
    case ExpressionType::LESS_THAN_EQUALS:
        return key <= literal;
    case ExpressionType::GREATER_THAN:
        return key > literal;
    case ExpressionType::GREATER_THAN_EQUALS:
        return key >= literal;
    default:
        throw std::logic_error("Invalid order key comparison");
    }
}

size_t addColumn(std::vector<LookupColumn>& columns, const PropertyExpression& property) {
    auto name = property.getPropertyName();
    auto it = std::ranges::find(columns, name, &LookupColumn::name);
    if (it != columns.end()) {
        return it - columns.begin();
    }
    columns.push_back({name, property.getDataType().getLogicalTypeID()});
    return columns.size() - 1;
}

namespace {
enum class Field {
    ORDER_KEY,
    ORDER_PROPERTY,
    BUYER_KEY,
    BUYER_PROPERTY,
    PRODUCT_KEY,
    PRODUCT_PROPERTY
};
struct OutputField {
    Field field;
    size_t index = 0;
};

struct KeyBound {
    ExpressionType comparison;
    int64_t literal;
    bool accepts(int64_t key) const { return compareKeys(comparison, key, literal); }
};

struct KeyLookupPlan final : TableFuncBindData {
    KeyLookupPlan(const RelGroupCatalogEntry& rel, const NodeTableCatalogEntry& users,
        const NodeTableCatalogEntry& products)
        : stable{rel.getStorageFormatName() == "iceberg_stable_csr"},
          relID{rel.getRelEntryInfos()[0].oid}, edge{rel.getStorage()},
          users{users.getStorage(), users.getPrimaryKeyName()},
          products{products.getStorage(), products.getPrimaryKeyName()} {}
    bool stable, singleHop = false, count = false;
    std::optional<Field> distinctKey;
    table_id_t relID;
    EdgeMapping edge;
    Source users, products;
    std::string dbName;
    std::optional<int64_t> anchor;
    std::vector<KeyBound> orderBounds;
    std::string filter;
    int64_t upper = 0;
    std::vector<LookupColumn> orderColumns, buyerColumns, productColumns;
    std::vector<OutputField> output;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<KeyLookupPlan>(*this);
    }
    std::string getDescription() const override {
        if (singleHop) {
            return "ICEBERG_CSR_KEY_LOOKUP (CSR FWD, order key range, DuckDB candidate keys IN, " +
                   edge.source.table + ")";
        }
        return "ICEBERG_CSR_KEY_LOOKUP (CSR FWD + BWD, DuckDB candidate keys IN, " +
               edge.source.table + ")";
    }
};

bool bindConditions(KeyLookupPlan& plan, const BoundMatchClause& match, const RelExpression& first,
    const RelExpression& second) {
    const auto& origin = *first.getSrcNode();
    const auto& buyer = *second.getSrcNode();
    bool otherBuyer = false;
    for (const auto& pred : match.getConjunctivePredicates()) {
        if (pred->getNumChildren() != 2) {
            return false;
        }
        auto lhs = pred->getChild(0), rhs = pred->getChild(1);
        if (!plan.singleHop && pred->expressionType == ExpressionType::NOT_EQUALS) {
            auto comparesEndpoints = [&](const Expression& start, const Expression& end) {
                return (*lhs == start && *rhs == end) || (*rhs == start && *lhs == end);
            };
            const auto originKey = origin.tryGetPrimaryKey(origin.getEntry(0)->getTableID());
            const auto buyerKey = buyer.tryGetPrimaryKey(buyer.getEntry(0)->getTableID());
            if (comparesEndpoints(*origin.getInternalID(), *buyer.getInternalID()) ||
                (originKey && buyerKey && comparesEndpoints(*originKey, *buyerKey))) {
                otherBuyer = true;
                continue;
            }
        }
        auto comparison = pred->expressionType;
        if (lhs->expressionType == ExpressionType::LITERAL &&
            (comparison == ExpressionType::EQUALS ||
                (plan.singleHop && ExpressionTypeUtil::isComparison(comparison)))) {
            std::swap(lhs, rhs);
            comparison = ExpressionTypeUtil::reverseComparisonDirection(comparison);
        }
        if (lhs->expressionType != ExpressionType::PROPERTY ||
            rhs->expressionType != ExpressionType::LITERAL ||
            rhs->getDataType().getLogicalTypeID() != LogicalTypeID::INT64 ||
            lhs->getDataType().getLogicalTypeID() != LogicalTypeID::INT64) {
            return false;
        }
        const auto& p = lhs->constCast<PropertyExpression>();
        const auto& value = rhs->constCast<LiteralExpression>().value;
        if (value.isNull()) {
            return false;
        }
        if (comparison == ExpressionType::EQUALS && !plan.anchor && p.isPrimaryKey() &&
            p.getVariableName() == origin.getUniqueName()) {
            plan.anchor = value.getValue<int64_t>();
        } else if (plan.singleHop && p.getVariableName() == first.getUniqueName() &&
                   p.getPropertyName() == plan.edge.source.key &&
                   ExpressionTypeUtil::isComparison(comparison) &&
                   comparison != ExpressionType::NOT_EQUALS) {
            plan.orderBounds.push_back({comparison, value.getValue<int64_t>()});
        } else if (!plan.singleHop && comparison == ExpressionType::LESS_THAN &&
                   plan.filter.empty() && p.getVariableName() == second.getUniqueName()) {
            plan.filter = p.getPropertyName();
            plan.upper = value.getValue<int64_t>();
            addColumn(plan.orderColumns, p);
        } else {
            return false;
        }
    }
    // In a two-hop pattern the other-buyer condition also rules out reusing the first edge.
    return plan.anchor.has_value() && (plan.singleHop ? !plan.orderBounds.empty() : otherBuyer);
}

bool bindOutput(KeyLookupPlan& plan, const BoundProjectionBody& body, const RelExpression& second) {
    if (body.hasSkipOrLimit() || (!plan.singleHop && body.hasOrderByExpressions()) ||
        (plan.singleHop && body.isDistinct())) {
        return false;
    }
    const auto exprs = body.getProjectionExpressions();
    // The native ORDER BY above the lookup can only reference columns emitted by the lookup.
    for (const auto& order : body.getOrderByExpressions()) {
        if (std::ranges::none_of(exprs, [&](const auto& expr) { return *expr == *order; })) {
            return false;
        }
    }
    if (exprs.size() == 1 && exprs.front()->expressionType == ExpressionType::AGGREGATE_FUNCTION) {
        auto& agg = exprs.front()->constCast<AggregateFunctionExpression>();
        plan.count = agg.getFunction().name == "COUNT_STAR" && !agg.isDistinct() &&
                     agg.getNumChildren() == 0;
        if (!plan.count || plan.filter.empty()) {
            return false;
        }
    } else {
        std::unordered_set<std::string> emitted;
        for (const auto& expr : exprs) {
            if (expr->expressionType != ExpressionType::PROPERTY ||
                !emitted.insert(expr->getUniqueName()).second) {
                return false;
            }
            const auto& prop = expr->constCast<PropertyExpression>();
            const auto type = prop.getDataType().getLogicalTypeID();
            if (type != LogicalTypeID::INT64 && type != LogicalTypeID::STRING) {
                return false;
            }
            if (prop.getVariableName() == second.getUniqueName()) {
                if (prop.getPropertyName() == plan.edge.source.key) {
                    if (type != LogicalTypeID::INT64) {
                        return false;
                    }
                    plan.output.push_back({Field::ORDER_KEY});
                } else {
                    plan.output.push_back(
                        {Field::ORDER_PROPERTY, addColumn(plan.orderColumns, prop)});
                }
            } else if (prop.getVariableName() == second.getSrcNodeName()) {
                if (prop.isPrimaryKey()) {
                    plan.output.push_back({Field::BUYER_KEY});
                } else {
                    plan.output.push_back(
                        {Field::BUYER_PROPERTY, addColumn(plan.buyerColumns, prop)});
                }
            } else if (plan.singleHop && prop.getVariableName() == second.getDstNodeName()) {
                if (prop.isPrimaryKey()) {
                    plan.output.push_back({Field::PRODUCT_KEY});
                } else {
                    plan.output.push_back(
                        {Field::PRODUCT_PROPERTY, addColumn(plan.productColumns, prop)});
                }
            } else {
                return false;
            }
        }
        if (plan.orderColumns.empty() && plan.buyerColumns.empty() && plan.productColumns.empty()) {
            return false;
        }
        if (body.isDistinct()) {
            // In this pattern an order identifies its buyer. Buyer-only projections are
            // identified by the buyer key. Other DISTINCT expressions use the regular planner.
            if (std::ranges::find(plan.output, Field::ORDER_KEY, &OutputField::field) !=
                plan.output.end()) {
                plan.distinctKey = Field::ORDER_KEY;
            } else if (std::ranges::find(plan.output, Field::BUYER_KEY, &OutputField::field) !=
                           plan.output.end() &&
                       std::ranges::find(plan.output, Field::ORDER_PROPERTY, &OutputField::field) ==
                           plan.output.end()) {
                plan.distinctKey = Field::BUYER_KEY;
            } else {
                return false;
            }
        }
    }
    plan.columns = exprs;
    return true;
}

std::unique_ptr<KeyLookupPlan> translateKeyLookup(const BoundRegularQuery& query,
    ClientContext& ctx) {
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
    const bool singleHop = graph->getNumQueryRels() == 1 && graph->getNumQueryNodes() == 2;
    if (!singleHop && (graph->getNumQueryNodes() != 3 || graph->getNumQueryRels() != 2)) {
        return nullptr;
    }
    for (auto& var : graph->getAllPatterns()) {
        if (var->getNumEntries() != 1 || !var->getPropertyDataExprRef().empty()) {
            return nullptr;
        }
    }
    auto a = graph->getQueryRel(0), b = singleHop ? a : graph->getQueryRel(1);
    if (a->isRecursive() || b->isRecursive() || a->getDirectionType() != RelDirectionType::SINGLE ||
        b->getDirectionType() != RelDirectionType::SINGLE || a->getEntry(0) != b->getEntry(0) ||
        a->getDstNodeName() != b->getDstNodeName() ||
        a->getSrcNode()->getEntry(0) != b->getSrcNode()->getEntry(0)) {
        return nullptr;
    }
    const auto& rel = a->getEntry(0)->constCast<RelGroupCatalogEntry>();
    const auto& users = a->getSrcNode()->getEntry(0)->constCast<NodeTableCatalogEntry>();
    const auto& products = a->getDstNode()->getEntry(0)->constCast<NodeTableCatalogEntry>();
    const auto format = rel.getStorageFormatName();
    if ((format != "iceberg_csr" && format != "iceberg_stable_csr") ||
        users.getStorageFormatName() != format || products.getStorageFormatName() != format ||
        rel.getRelEntryInfos().size() != 1) {
        return nullptr;
    }
    const auto dbName = a->getDbName(a->getEntry(0));
    auto* catalog =
        DatabaseManager::resolveTableStorage(ctx, rel.getRelEntryInfos()[0].oid, dbName).first;
    auto* txn = Transaction::Get(ctx);
    const auto pair = rel.getRelEntryInfos()[0].nodePair;
    if (pair.srcTableID != users.getTableID() || pair.dstTableID != products.getTableID() ||
        !catalog->containsTable(txn, rel.getName()) ||
        catalog->getTableCatalogEntry(txn, rel.getName()) != &rel ||
        a->getSrcNode()->getDbName(a->getSrcNode()->getEntry(0)) != dbName ||
        a->getDstNode()->getDbName(a->getDstNode()->getEntry(0)) != dbName ||
        b->getDbName(b->getEntry(0)) != dbName) {
        return nullptr;
    }
    auto plan = std::make_unique<KeyLookupPlan>(rel, users, products);
    plan->singleHop = singleHop;
    plan->dbName = dbName;
    if (!bindConditions(*plan, match, *a, *b) ||
        !bindOutput(*plan, *part->getProjectionBody(), *b)) {
        return nullptr;
    }
    return plan;
}

struct Path {
    int64_t order, buyer, product = 0;
};

struct KeyLookupScan final : TableFuncLocalState {
    KeyLookupScan(LakeRead& lake, std::shared_ptr<NativeGeneration> generation, bool singleHop)
        : lake{lake}, products{generation, RelDataDirection::FWD, singleHop} {
        if (!singleHop) {
            buyers.emplace(std::move(generation), RelDataDirection::BWD, true);
        }
    }
    LakeRead& lake;
    Neighbors products;
    std::optional<Neighbors> buyers;
    std::shared_ptr<const StableNodes> stable;
    std::shared_ptr<const NodeKeys> sorted;
    std::optional<size_t> origin;
    bool countEmitted = false;
    std::unordered_set<int64_t> seen;

    int64_t neighborKey(size_t slot) const { return stable ? stable->key(slot) : sorted->at(slot); }
    std::vector<Path> nextPaths(const KeyLookupPlan& plan) {
        std::vector<Path> paths;
        size_t slot;
        int64_t oid;
        if (plan.singleHop) {
            while (paths.size() < DEFAULT_VECTOR_CAPACITY && products.take(slot, &oid)) {
                if (std::ranges::all_of(plan.orderBounds,
                        [&](const KeyBound& bound) { return bound.accepts(oid); })) {
                    paths.push_back({oid, *plan.anchor, neighborKey(slot)});
                }
            }
            return paths;
        }
        while (paths.size() < DEFAULT_VECTOR_CAPACITY) {
            if (buyers->take(slot, &oid)) {
                if (slot != *origin) {
                    paths.push_back({oid, neighborKey(slot)});
                }
            } else if (products.take(slot)) {
                buyers->seek(slot);
            } else {
                break;
            }
        }
        return paths;
    }
};

std::unique_ptr<TableFuncLocalState> initKeyLookup(const TableFuncInitLocalStateInput& in) {
    const auto& plan = in.bindData.cast<KeyLookupPlan>();
    auto& txn = *Transaction::Get(*in.clientContext);
    auto& lake = LakeRead::get(&txn);
    auto* storage =
        DatabaseManager::resolveTableStorage(*in.clientContext, plan.relID, plan.dbName).second;
    auto store = NativeStore::forDatabase(storage->getDatabasePath());
    auto generation =
        plan.stable ?
            lake.stableGeneration(plan.relID, plan.edge, plan.users, plan.products, *store) :
            lake.nativeGeneration(plan.relID, plan.edge, plan.users, plan.products, *store);
    auto state = std::make_unique<KeyLookupScan>(lake, std::move(generation), plan.singleHop);
    const auto& neighbor = plan.singleHop ? plan.products : plan.users;
    if (plan.stable) {
        state->stable = lake.stableNodes(neighbor, *store);
        state->origin = lake.stableNodes(plan.users, *store)->slot(*plan.anchor);
    } else {
        state->sorted = lake.orderedNodeKeys(neighbor, *store);
        state->origin = lake.nodeOffset(plan.users, *plan.anchor, store.get());
    }
    if (state->origin) {
        state->products.seek(*state->origin);
    }
    return state;
}

std::vector<int64_t> pathKeys(const std::vector<Path>& paths, int64_t Path::*member) {
    std::vector<int64_t> keys;
    keys.reserve(paths.size());
    for (auto& path : paths) {
        keys.push_back(path.*member);
    }
    return keys;
}

offset_t scanKeyLookup(const TableFuncInput& in, TableFuncOutput& out) {
    auto& scan = *in.localState->ptrCast<KeyLookupScan>();
    const auto& plan = in.bindData->cast<KeyLookupPlan>();
    if (scan.countEmitted) {
        return 0;
    }
    int64_t count = 0;
    for (auto paths = scan.nextPaths(plan); !paths.empty(); paths = scan.nextPaths(plan)) {
        LakeRead::KeyedRows orders, buyers, products;
        if (!plan.orderColumns.empty()) {
            orders = scan.lake.lookup(plan.edge.source, pathKeys(paths, &Path::order),
                plan.orderColumns, plan.filter, plan.upper);
            if (!plan.filter.empty()) {
                std::erase_if(paths, [&](const Path& p) { return !orders.contains(p.order); });
            }
        }
        if (plan.count) {
            count += paths.size();
            continue;
        }
        if (plan.distinctKey) {
            std::erase_if(paths, [&](const Path& p) {
                return !scan.seen.insert(*plan.distinctKey == Field::ORDER_KEY ? p.order : p.buyer)
                            .second;
            });
        }
        if (paths.empty()) {
            continue;
        }
        if (!plan.buyerColumns.empty()) {
            buyers = scan.lake.lookup(plan.users, pathKeys(paths, &Path::buyer), plan.buyerColumns);
        }
        if (!plan.productColumns.empty()) {
            products = scan.lake.lookup(plan.products, pathKeys(paths, &Path::product),
                plan.productColumns);
        }
        for (size_t row = 0; row < paths.size(); ++row) {
            const auto& path = paths[row];
            for (size_t i = 0; i < plan.output.size(); ++i) {
                auto& vec = out.dataChunk.getValueVectorMutable(i);
                auto field = plan.output[i];
                switch (field.field) {
                case Field::ORDER_KEY:
                    vec.setValue<int64_t>(row, path.order);
                    break;
                case Field::BUYER_KEY:
                    vec.setValue<int64_t>(row, path.buyer);
                    break;
                case Field::ORDER_PROPERTY:
                    vec.copyFromValue(row, orders.at(path.order)[field.index]);
                    break;
                case Field::BUYER_PROPERTY:
                    vec.copyFromValue(row, buyers.at(path.buyer)[field.index]);
                    break;
                case Field::PRODUCT_KEY:
                    vec.setValue<int64_t>(row, path.product);
                    break;
                case Field::PRODUCT_PROPERTY:
                    vec.copyFromValue(row, products.at(path.product)[field.index]);
                    break;
                }
            }
        }
        return paths.size();
    }
    if (plan.count) {
        out.dataChunk.getValueVectorMutable(0).setValue<int64_t>(0, count);
        scan.countEmitted = true;
        return 1;
    }
    return 0;
}

class KeyLookupPlanner final : public lbug::extension::QueryPlannerExtension {
    std::shared_ptr<LogicalOperator> tryPlanQuery(const BoundRegularQuery& q,
        ClientContext& ctx) override {
        if (!ctx.getCurrentSetting("iceberg_poc_key_pushdown").getValue<bool>()) {
            return nullptr;
        }
        auto plan = translateKeyLookup(q, ctx);
        if (!plan) {
            return tryPlanPathLookup(q, ctx);
        }
        TableFunction scan("ICEBERG_CSR_KEY_LOOKUP", {});
        scan.canParallelFunc = [] { return false; };
        scan.initSharedStateFunc = TableFunction::initEmptySharedState;
        scan.initLocalStateFunc = initKeyLookup;
        scan.tableFunc = scanKeyLookup;
        auto lookup = std::make_shared<LogicalTableFunctionCall>(std::move(scan), std::move(plan));
        const auto& body = *q.getSingleQuery(0)->getQueryPart(0)->getProjectionBody();
        if (body.hasOrderByExpressions()) {
            lookup->computeFactorizedSchema();
            return std::make_shared<LogicalOrderBy>(body.getOrderByExpressions(),
                body.getSortingOrders(), std::move(lookup));
        }
        return lookup;
    }
};
} // namespace

void registerKeyLookupPlanner(Database& db) {
    db.addExtensionOption("iceberg_poc_key_pushdown", LogicalTypeID::BOOL, Value(true));
    db.addPlannerExtension(std::make_unique<KeyLookupPlanner>());
}
} // namespace lbug::iceberg
