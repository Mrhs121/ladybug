#pragma once

#include <string>
#include <vector>

#include "processor/operator/physical_operator.h"
#include "transaction/transaction_action.h"

namespace lbug {
namespace transaction {
class TransactionContext;
} // namespace transaction
namespace main {
class ClientContext;
class QueryResult;
} // namespace main

namespace processor {

struct TransactionPrintInfo final : OPPrintInfo {
    transaction::TransactionAction action;

    explicit TransactionPrintInfo(transaction::TransactionAction action) : action(action) {}

    std::string toString() const override;

    std::unique_ptr<OPPrintInfo> copy() const override {
        return std::unique_ptr<TransactionPrintInfo>(new TransactionPrintInfo(*this));
    }

private:
    TransactionPrintInfo(const TransactionPrintInfo& other)
        : OPPrintInfo(other), action(other.action) {}
};

class Transaction final : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::TRANSACTION;

public:
    Transaction(transaction::TransactionAction transactionAction, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, id, std::move(printInfo)}, transactionAction{transactionAction},
          hasExecuted{false} {}

    bool isSource() const final { return true; }
    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) final {
        hasExecuted = false;
    }

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> copy() override {
        return std::make_unique<Transaction>(transactionAction, id, printInfo->copy());
    }

private:
    std::unique_ptr<main::QueryResult> runQueryNoLock(main::ClientContext* clientContext,
        const std::string& query, const std::string& phase) const;
    std::vector<std::string> collectFirstColumn(main::ClientContext* clientContext,
        const std::string& query, const std::string& phase) const;
    void vacuumDatabase(main::ClientContext* clientContext) const;
    void validateActiveTransaction(const transaction::TransactionContext& context) const;

private:
    transaction::TransactionAction transactionAction;
    bool hasExecuted;
};

} // namespace processor
} // namespace lbug
