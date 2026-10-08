#pragma once

#include <string>
#include <unordered_map>

#include "common/copy_constructors.h"
#include "storage/local_storage/local_table.h"
#include "storage/optimistic_allocator.h"

namespace lbug {
namespace main {
class ClientContext;
} // namespace main
namespace storage {
// Local tables are keyed by their owning catalog as well as their table ID: table IDs are
// per-catalog, so a transaction touching several graphs can hold uncommitted state for
// same-ID tables in different catalogs.
struct LocalTableKey {
    std::string ownerCatalogName;
    common::table_id_t tableID;
    bool operator==(const LocalTableKey&) const = default;
};

struct LocalTableKeyHash {
    std::size_t operator()(const LocalTableKey& key) const {
        return std::hash<std::string>{}(key.ownerCatalogName) ^
               (std::hash<common::table_id_t>{}(key.tableID) << 1);
    }
};

// Data structures in LocalStorage are not thread-safe.
// For now, we only support single thread insertions and updates. Once we optimize them with
// multiple threads, LocalStorage and its related data structures should be reworked to be
// thread-safe.
class LocalStorage {
public:
    explicit LocalStorage(main::ClientContext& clientContext) : clientContext{clientContext} {}
    DELETE_COPY_AND_MOVE(LocalStorage);

    // Do nothing if the table already exists, otherwise create a new local table.
    LocalTable* getOrCreateLocalTable(Table& table);
    // Return nullptr if no local table exists.
    LocalTable* getLocalTable(const Table& table) const;
    // Return nullptr if no local table exists, or several owners hold the same table ID.
    LocalTable* getLocalTable(common::table_id_t tableID) const;

    // `sm == nullptr` selects the current storage manager's main data file. The FileHandle
    // overload selects a physical topology or property file in decoupled storage.
    // Allocators are cached per file so workers share an allocator and commit/rollback
    // covers every physical file exactly once.
    PageAllocator* addOptimisticAllocator(StorageManager* sm = nullptr);
    PageAllocator* addOptimisticAllocator(FileHandle& dataFH);

    void commit();
    void rollback();

private:
    main::ClientContext& clientContext;
    std::unordered_map<LocalTableKey, std::unique_ptr<LocalTable>, LocalTableKeyHash> tables;

    // The mutex is only needed when working with the optimistic allocators
    std::mutex mtx;
    std::vector<std::unique_ptr<OptimisticAllocator>> optimisticAllocators;
    std::unordered_map<FileHandle*, OptimisticAllocator*> allocatorsByFile;
};

} // namespace storage
} // namespace lbug
