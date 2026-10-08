#pragma once

#include <filesystem>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/types/types.h"
#include "storage/file_handle.h"

namespace lbug {
namespace common {
class VirtualFileSystem;
}

namespace main {
class ClientContext;
}

namespace storage {
class BufferManager;

class StorageFileRegistry {
public:
    StorageFileRegistry(std::string baseDirectory, BufferManager* bm,
        common::VirtualFileSystem* vfs, bool readOnly);
    ~StorageFileRegistry();

    void initDirectories(main::ClientContext* context);

    FileHandle* getOrOpenTopoFile(common::table_id_t tableID, const std::string& tableName,
        main::ClientContext* context);
    FileHandle* getOrOpenNodePropsFile(common::table_id_t tableID, const std::string& tableName,
        main::ClientContext* context);
    FileHandle* getOrOpenNodeIndexFile(common::table_id_t tableID, const std::string& tableName,
        main::ClientContext* context);
    FileHandle* getOrOpenRelPropsFile(common::table_id_t tableID, const std::string& tableName,
        main::ClientContext* context);

    FileHandle* tryGetTopoFile(common::table_id_t tableID) const;
    FileHandle* tryGetNodePropsFile(common::table_id_t tableID) const;
    FileHandle* tryGetNodeIndexFile(common::table_id_t tableID) const;
    FileHandle* tryGetRelPropsFile(common::table_id_t tableID) const;

    std::vector<FileHandle*> getAllFiles() const;
    void closeAll();

    const std::string& getBaseDirectory() const { return baseDirectory; }

private:
    FileHandle* openFileHandleInternal(const std::string& filePath, main::ClientContext* context);

private:
    std::string baseDirectory;
    BufferManager* bm;
    common::VirtualFileSystem* vfs;
    bool readOnly;
    mutable std::mutex mtx;

    std::unordered_map<common::table_id_t, FileHandle*> topoFiles;
    std::unordered_map<common::table_id_t, FileHandle*> nodePropsFiles;
    std::unordered_map<common::table_id_t, FileHandle*> nodeIndexFiles;
    std::unordered_map<common::table_id_t, FileHandle*> relPropsFiles;
};

} // namespace storage
} // namespace lbug
