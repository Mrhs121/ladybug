#include "processor/operator/transaction.h"

#include <chrono>
#include <filesystem>
#include <format>

#include "common/exception/runtime.h"
#include "common/exception/transaction_manager.h"
#include "main/client_context.h"
#include "main/db_config.h"
#include "main/query_result.h"
#include "processor/execution_context.h"
#include "processor/result/flat_tuple.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

using namespace lbug::common;
using namespace lbug::transaction;

namespace lbug {
namespace processor {

class VacuumDBHelper {
public:
    static std::unique_ptr<main::QueryResult> queryNoLock(main::ClientContext* clientContext,
        const std::string& query) {
        return clientContext->queryNoLock(query);
    }
};

static void validateQueryResult(main::QueryResult* queryResult, const std::string& phase) {
    auto currentResult = queryResult;
    while (currentResult) {
        if (!currentResult->isSuccess()) {
            throw RuntimeException(
                std::format("VACUUM DATABASE failed during {}: {}", phase,
                    currentResult->getErrorMessage()));
        }
        currentResult = currentResult->getNextQueryResult();
    }
}

static std::string escapeSingleQuotes(const std::string& str) {
    std::string escaped;
    escaped.reserve(str.size());
    for (auto ch : str) {
        escaped.push_back(ch);
        if (ch == '\'') {
            escaped.push_back('\'');
        }
    }
    return escaped;
}

static std::string quoteIdentifier(const std::string& identifier) {
    std::string escaped;
    escaped.reserve(identifier.size() + 2);
    escaped.push_back('`');
    for (auto ch : identifier) {
        escaped.push_back(ch);
        if (ch == '`') {
            escaped.push_back('`');
        }
    }
    escaped.push_back('`');
    return escaped;
}

std::unique_ptr<main::QueryResult> Transaction::runQueryNoLock(main::ClientContext* clientContext,
    const std::string& query, const std::string& phase) const {
    auto res = VacuumDBHelper::queryNoLock(clientContext, query);
    validateQueryResult(res.get(), phase);
    return res;
}

std::vector<std::string> Transaction::collectFirstColumn(main::ClientContext* clientContext,
    const std::string& query, const std::string& phase) const {
    auto res = runQueryNoLock(clientContext, query, phase);
    std::vector<std::string> names;
    while (res->hasNext()) {
        auto tuple = res->getNext();
        names.push_back((*tuple)[0].toString());
    }
    return names;
}

void Transaction::vacuumDatabase(main::ClientContext* clientContext) const {
    if (clientContext->isInMemory()) {
        throw RuntimeException("VACUUM DATABASE is not supported for in-memory databases.");
    }
    if (clientContext->getDBConfig()->readOnly) {
        throw RuntimeException("VACUUM DATABASE is not supported in read-only mode.");
    }

    auto dbPath = clientContext->getDatabasePath();
    auto timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    auto exportDir = std::format("{}.__vacuum_export_{}", dbPath, timestamp);
    auto escapedExportDir = escapeSingleQuotes(exportDir);

    // Ensure this transaction statement starts from a checkpointed state.
    runQueryNoLock(clientContext, "CHECKPOINT;", "initial checkpoint");
    runQueryNoLock(clientContext,
        std::format("EXPORT DATABASE '{}' (FORMAT='parquet');", escapedExportDir), "export");

    // Clear table schema before importing the rebuilt layout.
    auto tableNames = collectFirstColumn(clientContext, "CALL SHOW_TABLES() RETURN name;",
        "collect tables");
    for (auto& tableName : tableNames) {
        runQueryNoLock(clientContext,
            std::format("DROP TABLE IF EXISTS {};", quoteIdentifier(tableName)), "drop table");
    }

    runQueryNoLock(clientContext, "CHECKPOINT;", "checkpoint after dropping objects");
    runQueryNoLock(clientContext, std::format("IMPORT DATABASE '{}';", escapedExportDir), "import");
    runQueryNoLock(clientContext, "CHECKPOINT;", "final checkpoint");

    std::error_code ec;
    std::filesystem::remove_all(exportDir, ec);
}

std::string TransactionPrintInfo::toString() const {
    std::string result = "Action: ";
    result += TransactionActionUtils::toString(action);
    return result;
}

bool Transaction::getNextTuplesInternal(ExecutionContext* context) {
    if (hasExecuted) {
        return false;
    }
    hasExecuted = true;
    auto clientContext = context->clientContext;
    auto transactionContext = TransactionContext::Get(*clientContext);
    validateActiveTransaction(*transactionContext);
    switch (transactionAction) {
    case TransactionAction::BEGIN_READ: {
        transactionContext->beginReadTransaction();
    } break;
    case TransactionAction::BEGIN_WRITE: {
        transactionContext->beginWriteTransaction();
    } break;
    case TransactionAction::COMMIT: {
        transactionContext->commit();
    } break;
    case TransactionAction::ROLLBACK: {
        transactionContext->rollback();
    } break;
    case TransactionAction::CHECKPOINT: {
        TransactionManager::Get(*clientContext)->checkpoint(*clientContext);
    } break;
    case TransactionAction::VACUUM_DATABASE: {
        vacuumDatabase(clientContext);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    return true;
}

void Transaction::validateActiveTransaction(const TransactionContext& context) const {
    switch (transactionAction) {
    case TransactionAction::BEGIN_READ:
    case TransactionAction::BEGIN_WRITE: {
        if (context.hasActiveTransaction()) {
            throw TransactionManagerException(
                "Connection already has an active transaction. Cannot start a transaction within "
                "another one. For concurrent multiple transactions, please open other "
                "connections.");
        }
    } break;
    case TransactionAction::COMMIT:
    case TransactionAction::ROLLBACK: {
        if (!context.hasActiveTransaction()) {
            throw TransactionManagerException(std::format("No active transaction for {}.",
                TransactionActionUtils::toString(transactionAction)));
        }
    } break;
    case TransactionAction::CHECKPOINT: {
        if (context.hasActiveTransaction()) {
            throw TransactionManagerException(std::format("Found active transaction for {}.",
                TransactionActionUtils::toString(transactionAction)));
        }
    } break;
    case TransactionAction::VACUUM_DATABASE: {
        if (context.hasActiveTransaction()) {
            throw TransactionManagerException(std::format("Found active transaction for {}.",
                TransactionActionUtils::toString(transactionAction)));
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace processor
} // namespace lbug
