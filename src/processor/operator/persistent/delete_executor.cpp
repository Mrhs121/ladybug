#include "processor/operator/persistent/delete_executor.h"

#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/assert.h"
#include "common/exception/message.h"
#include "common/vector/value_vector.h"
#include "main/client_context.h"
#include "main/settings.h"
#include "processor/execution_context.h"
#include "storage/table/rel_table.h"

using namespace lbug::common;
using namespace lbug::storage;
using namespace lbug::transaction;

namespace lbug {
namespace processor {

namespace {

struct DetachDeleteWorkItem {
    RelTable* relTable = nullptr;
    bool runFwd = false;
    bool runBwd = false;
};

template<typename FUNC>
void runInParallel(uint64_t maxThreads, size_t numItems, FUNC&& func) {
    if (numItems == 0) {
        return;
    }
    if (numItems == 1 || maxThreads <= 1) {
        func(0);
        return;
    }
    const auto numWorkers = std::min<size_t>(numItems, maxThreads);
    std::atomic<size_t> next{0};
    std::vector<std::thread> workers;
    workers.reserve(numWorkers);
    for (size_t i = 0; i < numWorkers; ++i) {
        workers.emplace_back([&]() {
            while (true) {
                const auto idx = next.fetch_add(1);
                if (idx >= numItems) {
                    break;
                }
                func(idx);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
}

void executeDetachDeleteWorkItem(const DetachDeleteWorkItem& item, Transaction* transaction,
    ValueVector& srcNodeIDVector) {
    if (!item.relTable) {
        return;
    }
    const auto localState = std::make_shared<DataChunkState>();
    ValueVector dstNodeIDVector(LogicalType::INTERNAL_ID());
    ValueVector relIDVector(LogicalType::INTERNAL_ID());
    dstNodeIDVector.setState(localState);
    relIDVector.setState(localState);
    if (item.runFwd) {
        item.relTable->detachDeleteBatch(transaction, srcNodeIDVector, dstNodeIDVector, relIDVector,
            RelDataDirection::FWD);
    }
    if (item.runBwd) {
        item.relTable->detachDeleteBatch(transaction, srcNodeIDVector, dstNodeIDVector, relIDVector,
            RelDataDirection::BWD);
    }
}

std::vector<DetachDeleteWorkItem> buildWorkItems(const NodeTableDeleteInfo& tableInfo) {
    std::unordered_map<RelTable*, size_t> workIdxByTable;
    std::vector<DetachDeleteWorkItem> workItems;
    workItems.reserve(tableInfo.fwdRelTables.size() + tableInfo.bwdRelTables.size());
    for (auto& relTable : tableInfo.fwdRelTables) {
        if (!workIdxByTable.contains(relTable)) {
            workIdxByTable.insert({relTable, workItems.size()});
            workItems.push_back({relTable, true, false});
        } else {
            workItems[workIdxByTable.at(relTable)].runFwd = true;
        }
    }
    for (auto& relTable : tableInfo.bwdRelTables) {
        if (!workIdxByTable.contains(relTable)) {
            workIdxByTable.insert({relTable, workItems.size()});
            workItems.push_back({relTable, false, true});
        } else {
            workItems[workIdxByTable.at(relTable)].runBwd = true;
        }
    }
    return workItems;
}

std::vector<DetachDeleteWorkItem> buildWorkItems(
    const common::table_id_map_t<NodeTableDeleteInfo>& tableInfos) {
    std::unordered_map<RelTable*, size_t> workIdxByTable;
    std::vector<DetachDeleteWorkItem> workItems;
    for (auto& [_, tableInfo] : tableInfos) {
        workItems.reserve(
            workItems.size() + tableInfo.fwdRelTables.size() + tableInfo.bwdRelTables.size());
        for (auto& relTable : tableInfo.fwdRelTables) {
            if (!workIdxByTable.contains(relTable)) {
                workIdxByTable.insert({relTable, workItems.size()});
                workItems.push_back({relTable, true, false});
            } else {
                workItems[workIdxByTable.at(relTable)].runFwd = true;
            }
        }
        for (auto& relTable : tableInfo.bwdRelTables) {
            if (!workIdxByTable.contains(relTable)) {
                workIdxByTable.insert({relTable, workItems.size()});
                workItems.push_back({relTable, false, true});
            } else {
                workItems[workIdxByTable.at(relTable)].runBwd = true;
            }
        }
    }
    return workItems;
}

uint64_t getNumDeleteWorkerThreads(ExecutionContext* context) {
    return context->clientContext->getCurrentSetting(main::ThreadsSetting::name)
        .getValue<uint64_t>();
}

} // namespace

void NodeDeleteInfo::init(const ResultSet& resultSet) {
    nodeIDVector = resultSet.getValueVector(nodeIDPos).get();
}

void NodeTableDeleteInfo::init(const ResultSet& resultSet) {
    pkVector = resultSet.getValueVector(pkPos).get();
}

static void throwDeleteNodeWithConnectedEdgesError(const std::string& tableName,
    offset_t nodeOffset, RelDataDirection direction) {
    throw RuntimeException(ExceptionMessage::violateDeleteNodeWithConnectedEdgesConstraint(
        tableName, std::to_string(nodeOffset), RelDirectionUtils::relDirectionToString(direction)));
}

void NodeTableDeleteInfo::deleteFromRelTable(Transaction* transaction,
    ValueVector* nodeIDVector) const {
    for (auto& relTable : fwdRelTables) {
        relTable->throwIfNodeHasRels(transaction, RelDataDirection::FWD, nodeIDVector,
            throwDeleteNodeWithConnectedEdgesError);
    }
    for (auto& relTable : bwdRelTables) {
        relTable->throwIfNodeHasRels(transaction, RelDataDirection::BWD, nodeIDVector,
            throwDeleteNodeWithConnectedEdgesError);
    }
}

void NodeTableDeleteInfo::detachDeleteFromRelTable(Transaction* transaction,
    RelTableDeleteState* detachDeleteState) const {
    for (auto& relTable : fwdRelTables) {
        detachDeleteState->detachDeleteDirection = RelDataDirection::FWD;
        relTable->detachDelete(transaction, detachDeleteState);
    }
    for (auto& relTable : bwdRelTables) {
        detachDeleteState->detachDeleteDirection = RelDataDirection::BWD;
        relTable->detachDelete(transaction, detachDeleteState);
    }
}

void NodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext*) {
    info.init(*resultSet);
    if (info.deleteType == DeleteNodeType::DETACH_DELETE) {
        const auto tempSharedState = std::make_shared<DataChunkState>();
        dstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
        relIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
        dstNodeIDVector->setState(tempSharedState);
        relIDVector->setState(tempSharedState);
        detachDeleteState = std::make_unique<RelTableDeleteState>(*info.nodeIDVector,
            *dstNodeIDVector, *relIDVector);
        batchSrcNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
        batchDstNodeIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
        batchRelIDVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID());
    }
}

void SingleLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    tableInfo.init(*resultSet);
}

void NodeDeleteExecutor::finalize(ExecutionContext* context) {
    if (!batchNodeIDs.empty()) {
        auto transaction = Transaction::Get(*context->clientContext);
        flushBatch(context, transaction);
        batchNodeIDs.clear();
    }
}

void SingleLabelNodeDeleteExecutor::flushBatch(ExecutionContext* context,
    transaction::Transaction* transaction) {
    const auto batchState = std::make_shared<DataChunkState>();
    batchSrcNodeIDVector->setState(batchState);
    batchState->getSelVectorUnsafe().setSelSize(batchNodeIDs.size());
    for (size_t i = 0; i < batchNodeIDs.size(); i++) {
        batchSrcNodeIDVector->setValue(i, batchNodeIDs[i]);
    }
    const auto workItems = buildWorkItems(tableInfo);
    const auto maxThreads = getNumDeleteWorkerThreads(context);
    runInParallel(maxThreads, workItems.size(), [&](size_t idx) {
        executeDetachDeleteWorkItem(workItems[idx], transaction, *batchSrcNodeIDVector);
    });
}

void SingleLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    KU_ASSERT(tableInfo.pkVector->state == info.nodeIDVector->state);
    auto deleteState =
        std::make_unique<NodeTableDeleteState>(*info.nodeIDVector, *tableInfo.pkVector);
    auto transaction = Transaction::Get(*context->clientContext);
    if (!tableInfo.table->delete_(transaction, *deleteState)) {
        return;
    }
    switch (info.deleteType) {
    case DeleteNodeType::DELETE: {
        tableInfo.deleteFromRelTable(transaction, info.nodeIDVector);
    } break;
    case DeleteNodeType::DETACH_DELETE: {
        auto& selVec = info.nodeIDVector->state->getSelVector();
        const auto pos = selVec[0];
        if (!info.nodeIDVector->isNull(pos)) {
            batchNodeIDs.push_back(info.nodeIDVector->getValue<internalID_t>(pos));
        }
        if (batchNodeIDs.size() >= BATCH_SIZE) {
            flushBatch(context, transaction);
            batchNodeIDs.clear();
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void MultiLabelNodeDeleteExecutor::init(ResultSet* resultSet, ExecutionContext* context) {
    NodeDeleteExecutor::init(resultSet, context);
    for (auto& [_, tableInfo] : tableInfos) {
        tableInfo.init(*resultSet);
    }
}

void MultiLabelNodeDeleteExecutor::flushBatch(ExecutionContext* context,
    transaction::Transaction* transaction) {
    const auto batchState = std::make_shared<DataChunkState>();
    batchSrcNodeIDVector->setState(batchState);
    batchState->getSelVectorUnsafe().setSelSize(batchNodeIDs.size());
    for (size_t i = 0; i < batchNodeIDs.size(); i++) {
        batchSrcNodeIDVector->setValue(i, batchNodeIDs[i]);
    }
    const auto workItems = buildWorkItems(tableInfos);
    const auto maxThreads = getNumDeleteWorkerThreads(context);
    runInParallel(maxThreads, workItems.size(), [&](size_t idx) {
        executeDetachDeleteWorkItem(workItems[idx], transaction, *batchSrcNodeIDVector);
    });
}

void MultiLabelNodeDeleteExecutor::delete_(ExecutionContext* context) {
    auto& nodeIDSelVector = info.nodeIDVector->state->getSelVector();
    KU_ASSERT(nodeIDSelVector.getSelSize() == 1);
    const auto pos = nodeIDSelVector[0];
    if (info.nodeIDVector->isNull(pos)) {
        return;
    }
    const auto nodeID = info.nodeIDVector->getValue<internalID_t>(pos);
    const auto& tableInfo = tableInfos.at(nodeID.tableID);
    auto deleteState =
        std::make_unique<NodeTableDeleteState>(*info.nodeIDVector, *tableInfo.pkVector);
    auto transaction = Transaction::Get(*context->clientContext);
    if (!tableInfo.table->delete_(transaction, *deleteState)) {
        return;
    }
    switch (info.deleteType) {
    case DeleteNodeType::DELETE: {
        tableInfo.deleteFromRelTable(transaction, info.nodeIDVector);
    } break;
    case DeleteNodeType::DETACH_DELETE: {
        batchNodeIDs.push_back(nodeID);
        if (batchNodeIDs.size() >= BATCH_SIZE) {
            flushBatch(context, transaction);
            batchNodeIDs.clear();
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void RelDeleteInfo::init(const ResultSet& resultSet) {
    srcNodeIDVector = resultSet.getValueVector(srcNodeIDPos).get();
    dstNodeIDVector = resultSet.getValueVector(dstNodeIDPos).get();
    relIDVector = resultSet.getValueVector(relIDPos).get();
}

void RelDeleteExecutor::init(ResultSet* resultSet, ExecutionContext*) {
    info.init(*resultSet);
}

void SingleLabelRelDeleteExecutor::delete_(ExecutionContext* context) {
    auto deleteState = std::make_unique<RelTableDeleteState>(*info.srcNodeIDVector,
        *info.dstNodeIDVector, *info.relIDVector);
    table->delete_(Transaction::Get(*context->clientContext), *deleteState);
}

void MultiLabelRelDeleteExecutor::delete_(ExecutionContext* context) {
    auto& idSelVector = info.relIDVector->state->getSelVector();
    KU_ASSERT(idSelVector.getSelSize() == 1);
    const auto pos = idSelVector[0];
    const auto relID = info.relIDVector->getValue<internalID_t>(pos);
    KU_ASSERT(tableIDToTableMap.contains(relID.tableID));
    auto table = tableIDToTableMap.at(relID.tableID);
    auto deleteState = std::make_unique<RelTableDeleteState>(*info.srcNodeIDVector,
        *info.dstNodeIDVector, *info.relIDVector);
    table->delete_(Transaction::Get(*context->clientContext), *deleteState);
}

} // namespace processor
} // namespace lbug
