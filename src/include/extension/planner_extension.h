#pragma once

#include "binder/bound_statement.h"
#include "planner/planner.h"

namespace lbug::binder {
class BoundRegularQuery;
}

namespace lbug {
namespace extension {

class PlannerExtension {

public:
    PlannerExtension() {}

    virtual ~PlannerExtension() = default;

    virtual std::shared_ptr<planner::LogicalOperator> plan(
        const binder::BoundStatement& boundStatement) = 0;
};

// Opt in separately so extensions for custom clauses only see those clauses.
class LBUG_API QueryPlannerExtension : public PlannerExtension {
public:
    std::shared_ptr<planner::LogicalOperator> plan(const binder::BoundStatement&) final {
        return nullptr;
    }
    virtual std::shared_ptr<planner::LogicalOperator> tryPlanQuery(
        const binder::BoundRegularQuery& query, main::ClientContext& context) = 0;
};

} // namespace extension
} // namespace lbug
