#include "storage/storage_file_registry.h"

#include <filesystem>

#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"
#include "storage/buffer_manager/buffer_manager.h"

namespace lbug {
namespace storage {

StorageFileRegistry::StorageFileRegistry(std::string baseDirectory, BufferManager* bm,
    common::VirtualFileSystem* vfs, bool readOnly)
    : baseDirectory{std::move(baseDirectory)}, bm{bm}, vfs{vfs}, readOnly{readOnly} {}

StorageFileRegistry::~StorageFileRegistry() {
    closeAll();
}

void StorageFileRegistry::initDirectories(main::ClientContext* /*context*/) {
    if (readOnly) {
        return;
    }
    std::filesystem::path base(baseDirectory);
    if (!std::filesystem::exists(base)) {
        std::filesystem::create_directories(base);
    }
    std::filesystem::create_directories(base / "topology");
    std::filesystem::create_directories(base / "nodes");
    std::filesystem::create_directories(base / "rels");
    std::filesystem::create_directories(base / "wal");
}

FileHandle* StorageFileRegistry::openFileHandleInternal(const std::string& filePath,
    main::ClientContext* context) {
    auto flag = readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                           FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS;
    if (!readOnly) {
        flag |= FileHandle::O_LOCKED_PERSISTENT_FILE;
    }
    return bm->getFileHandle(filePath, flag, vfs, context);
}

FileHandle* StorageFileRegistry::getOrOpenTopoFile(common::table_id_t tableID,
    const std::string& tableName, main::ClientContext* context) {
    std::lock_guard<std::mutex> lck(mtx);
    if (topoFiles.contains(tableID)) {
        return topoFiles.at(tableID);
    }
    auto topoDir = std::filesystem::path(baseDirectory) / "topology";
    if (!readOnly && !std::filesystem::exists(topoDir)) {
        std::filesystem::create_directories(topoDir);
    }
    auto filePath = (topoDir / ("rel_" + tableName + ".topo")).string();
    auto* fh = openFileHandleInternal(filePath, context);
    topoFiles[tableID] = fh;
    return fh;
}

FileHandle* StorageFileRegistry::getOrOpenNodePropsFile(common::table_id_t tableID,
    const std::string& tableName, main::ClientContext* context) {
    std::lock_guard<std::mutex> lck(mtx);
    if (nodePropsFiles.contains(tableID)) {
        return nodePropsFiles.at(tableID);
    }
    auto tableDir = std::filesystem::path(baseDirectory) / "nodes" / tableName;
    if (!readOnly && !std::filesystem::exists(tableDir)) {
        std::filesystem::create_directories(tableDir);
    }
    auto filePath = (tableDir / "props.data").string();
    auto* fh = openFileHandleInternal(filePath, context);
    nodePropsFiles[tableID] = fh;
    return fh;
}

FileHandle* StorageFileRegistry::getOrOpenNodeIndexFile(common::table_id_t tableID,
    const std::string& tableName, main::ClientContext* context) {
    std::lock_guard<std::mutex> lck(mtx);
    if (nodeIndexFiles.contains(tableID)) {
        return nodeIndexFiles.at(tableID);
    }
    auto tableDir = std::filesystem::path(baseDirectory) / "nodes" / tableName;
    if (!readOnly && !std::filesystem::exists(tableDir)) {
        std::filesystem::create_directories(tableDir);
    }
    auto filePath = (tableDir / "pk.idx").string();
    auto* fh = openFileHandleInternal(filePath, context);
    nodeIndexFiles[tableID] = fh;
    return fh;
}

FileHandle* StorageFileRegistry::getOrOpenRelPropsFile(common::table_id_t tableID,
    const std::string& tableName, main::ClientContext* context) {
    std::lock_guard<std::mutex> lck(mtx);
    if (relPropsFiles.contains(tableID)) {
        return relPropsFiles.at(tableID);
    }
    auto tableDir = std::filesystem::path(baseDirectory) / "rels" / tableName;
    if (!readOnly && !std::filesystem::exists(tableDir)) {
        std::filesystem::create_directories(tableDir);
    }
    auto filePath = (tableDir / "props.data").string();
    auto* fh = openFileHandleInternal(filePath, context);
    relPropsFiles[tableID] = fh;
    return fh;
}

FileHandle* StorageFileRegistry::tryGetTopoFile(common::table_id_t tableID) const {
    std::lock_guard<std::mutex> lck(mtx);
    if (topoFiles.contains(tableID)) {
        return topoFiles.at(tableID);
    }
    return nullptr;
}

FileHandle* StorageFileRegistry::tryGetNodePropsFile(common::table_id_t tableID) const {
    std::lock_guard<std::mutex> lck(mtx);
    if (nodePropsFiles.contains(tableID)) {
        return nodePropsFiles.at(tableID);
    }
    return nullptr;
}

FileHandle* StorageFileRegistry::tryGetNodeIndexFile(common::table_id_t tableID) const {
    std::lock_guard<std::mutex> lck(mtx);
    if (nodeIndexFiles.contains(tableID)) {
        return nodeIndexFiles.at(tableID);
    }
    return nullptr;
}

FileHandle* StorageFileRegistry::tryGetRelPropsFile(common::table_id_t tableID) const {
    std::lock_guard<std::mutex> lck(mtx);
    if (relPropsFiles.contains(tableID)) {
        return relPropsFiles.at(tableID);
    }
    return nullptr;
}

std::vector<FileHandle*> StorageFileRegistry::getAllFiles() const {
    std::lock_guard<std::mutex> lck(mtx);
    std::vector<FileHandle*> result;
    for (const auto& [_, fh] : topoFiles) {
        if (fh)
            result.push_back(fh);
    }
    for (const auto& [_, fh] : nodePropsFiles) {
        if (fh)
            result.push_back(fh);
    }
    for (const auto& [_, fh] : nodeIndexFiles) {
        if (fh)
            result.push_back(fh);
    }
    for (const auto& [_, fh] : relPropsFiles) {
        if (fh)
            result.push_back(fh);
    }
    return result;
}

void StorageFileRegistry::closeAll() {
    std::lock_guard<std::mutex> lck(mtx);
    for (auto& [_, fh] : topoFiles) {
        if (fh)
            fh->resetFileInfo();
    }
    for (auto& [_, fh] : nodePropsFiles) {
        if (fh)
            fh->resetFileInfo();
    }
    for (auto& [_, fh] : nodeIndexFiles) {
        if (fh)
            fh->resetFileInfo();
    }
    for (auto& [_, fh] : relPropsFiles) {
        if (fh)
            fh->resetFileInfo();
    }
    topoFiles.clear();
    nodePropsFiles.clear();
    nodeIndexFiles.clear();
    relPropsFiles.clear();
}

} // namespace storage
} // namespace lbug
