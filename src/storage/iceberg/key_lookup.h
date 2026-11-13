#pragma once

#include "extension/planner_extension.h"
#include "lake.h"

namespace lbug::iceberg {
bool compareKeys(lbug::common::ExpressionType comparison, int64_t left, int64_t right);
size_t addColumn(std::vector<LookupColumn>& columns,
    const lbug::binder::PropertyExpression& property);
std::shared_ptr<lbug::planner::LogicalOperator> tryPlanPathLookup(
    const lbug::binder::BoundRegularQuery& query, lbug::main::ClientContext& context);
} // namespace lbug::iceberg
