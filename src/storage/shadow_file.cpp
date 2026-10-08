#include "storage/shadow_file.h"

#include <algorithm>
#include <filesystem>
#include <unordered_set>

#include "common/exception/io.h"
#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/serializer/buffer_reader.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "main/client_context.h"
#include "main/database.h"
#include "main/db_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/database_header.h"
#include "storage/file_db_id_utils.h"
#include "storage/file_handle.h"
#include "storage/storage_manager.h"
#include <format>

using namespace lbug::common;
using namespace lbug::main;

namespace lbug {
namespace storage {

namespace {
constexpr uint64_t FILE_MANIFEST_VERSION = 1;

struct ShadowFileTarget {
    file_idx_t fileIdx;
    page_idx_t numPages;
    std::string relativePath;
    FileInfo* fileInfo = nullptr;
    FileHandle* fileHandle = nullptr;
    std::unique_ptr<FileInfo> ownedFileInfo;

    void serialize(Serializer& serializer) const {
        serializer.write(fileIdx);
        serializer.write(numPages);
        serializer.write(relativePath);
    }
};

bool isValidTargetPath(const std::string& relativePath, const std::string& metadataName) {
    const std::filesystem::path path{relativePath};
    if (relativePath.empty() || relativePath.find('\0') != std::string::npos ||
        path.is_absolute() || path != path.lexically_normal()) {
        return false;
    }
    std::vector<std::string> parts;
    for (const auto& part : path) {
        if (part == ".." || part == "." || part.empty()) {
            return false;
        }
        parts.push_back(part.string());
    }
    return relativePath == metadataName ||
           (parts.size() == 2 && parts[0] == "topology" && parts[1].starts_with("rel_") &&
               path.extension() == ".topo") ||
           (parts.size() == 3 &&
               ((parts[0] == "nodes" && (parts[2] == "props.data" || parts[2] == "pk.idx")) ||
                   (parts[0] == "rels" && parts[2] == "props.data")));
}

// Bounds every read to the buffer so a corrupted length field in a database header page (e.g. a
// debugging-string length) cannot read past the page allocation.
class BoundedPageReader final : public Reader {
public:
    BoundedPageReader(uint8_t* data, uint64_t dataSize, std::string outOfBoundsMessage)
        : data{data}, dataSize{dataSize}, outOfBoundsMessage{std::move(outOfBoundsMessage)} {}

    void read(uint8_t* outputData, uint64_t size) override {
        if (size > dataSize - readOffset) {
            throw RuntimeException(outOfBoundsMessage);
        }
        std::memcpy(outputData, data + readOffset, size);
        readOffset += size;
    }

    bool finished() override { return readOffset >= dataSize; }

    uint64_t getReadOffset() const override { return readOffset; }

private:
    uint8_t* data;
    uint64_t dataSize;
    uint64_t readOffset = 0;
    std::string outOfBoundsMessage;
};
} // namespace

void ShadowPageRecord::serialize(Serializer& serializer) const {
    serializer.write<file_idx_t>(originalFileIdx);
    serializer.write<page_idx_t>(originalPageIdx);
}

ShadowPageRecord ShadowPageRecord::deserialize(Deserializer& deserializer) {
    file_idx_t originalFileIdx = INVALID_FILE_IDX;
    page_idx_t originalPageIdx = INVALID_PAGE_IDX;
    deserializer.deserializeValue<file_idx_t>(originalFileIdx);
    deserializer.deserializeValue<page_idx_t>(originalPageIdx);
    return ShadowPageRecord{originalFileIdx, originalPageIdx};
}

ShadowFile::ShadowFile(BufferManager& bm, VirtualFileSystem* vfs, const std::string& databasePath,
    StorageManager* owningStorageManager)
    : bm{bm}, shadowFilePath{StorageUtils::getShadowFilePath(databasePath)}, vfs{vfs},
      owningStorageManager{owningStorageManager},
      multiFile{owningStorageManager && owningStorageManager->isDecoupledStorage() &&
                !owningStorageManager->isInMemory()},
      shadowingFH{nullptr} {
    DASSERT(vfs);
}

void ShadowFile::setDatabasePath(const std::string& databasePath_) {
    shadowFilePath = StorageUtils::getShadowFilePath(databasePath_);
}

void ShadowFile::clearShadowPage(file_idx_t originalFile, page_idx_t originalPage) {
    std::unique_lock lck{mtx};
    if (hasShadowPage(originalFile, originalPage)) {
        shadowPagesMap.at(originalFile).erase(originalPage);
        if (shadowPagesMap.at(originalFile).empty()) {
            shadowPagesMap.erase(originalFile);
        }
    }
}

page_idx_t ShadowFile::createShadowPage(file_idx_t originalFile, page_idx_t originalPage) {
    DASSERT(!hasShadowPage(originalFile, originalPage));
    std::unique_lock lck{mtx};
    const auto shadowPageIdx = getOrCreateShadowingFH()->addNewPage();
    // Records must stay in the same order as the shadow pages (see applyShadowPages). Readers
    // only use shadowPagesMap, so the page is not visible to them until it is published.
    shadowPageRecords.push_back({originalFile, originalPage});
    return shadowPageIdx;
}

void ShadowFile::publishShadowPage(file_idx_t originalFile, page_idx_t originalPage,
    page_idx_t shadowPageIdx) {
    std::unique_lock lck{mtx};
    shadowPagesMap[originalFile][originalPage] = shadowPageIdx;
    // Pairs with the acquire load in readShadowVersionIfExists.
    hasShadowPages.store(true, std::memory_order_release);
}

ShadowFile::ShadowSavepoint ShadowFile::createSavepoint() const {
    std::shared_lock lck{mtx};
    return shadowPageRecords.size();
}

void ShadowFile::rollbackToSavepoint(ShadowSavepoint savepoint) {
    std::unique_lock lck{mtx};
    DASSERT(savepoint <= shadowPageRecords.size());
    if (savepoint == shadowPageRecords.size()) {
        return;
    }
    // Drop the map entries for shadow pages created since the savepoint. An entry may
    // already be gone if its page was reclaimed via clearShadowPage, so tolerate misses.
    for (auto i = savepoint; i < shadowPageRecords.size(); i++) {
        const auto& record = shadowPageRecords[i];
        auto fileIt = shadowPagesMap.find(record.originalFileIdx);
        if (fileIt != shadowPagesMap.end()) {
            fileIt->second.erase(record.originalPageIdx);
            if (fileIt->second.empty()) {
                shadowPagesMap.erase(fileIt);
            }
        }
    }
    if (shadowingFH != nullptr) {
        // Evict the dropped pages' frames without flushing: their contents belong to the
        // failed checkpoint attempt and must not reach the data file. Frames of other
        // groups' shadow pages are left untouched. removePageFromFrameIfNecessary skips
        // pages that are not in a frame.
        const auto numPages = shadowingFH->getNumPages();
        for (auto pageIdx = savepoint + 1; pageIdx < numPages; pageIdx++) {
            shadowingFH->removePageFromFrameIfNecessary(static_cast<page_idx_t>(pageIdx));
        }
        // Truncate the page counter so later allocations reuse the freed indices, keeping
        // shadowPageRecords[i] describing shadow page i+1. Frame groups are retained at
        // their high-water mark and reused when the file grows again. Stale bytes possibly
        // left on disk past the truncation point are harmless: createShadowPage pins new
        // pages without reading them and fills them with the original page contents.
        shadowingFH->removePageIdxAndTruncateIfNecessary(static_cast<page_idx_t>(savepoint + 1));
    }
    shadowPageRecords.resize(savepoint);
    hasShadowPages.store(!shadowPagesMap.empty(), std::memory_order_relaxed);
}

page_idx_t ShadowFile::getShadowPage(file_idx_t originalFile, page_idx_t originalPage) const {
    DASSERT(hasShadowPage(originalFile, originalPage));
    return shadowPagesMap.at(originalFile).at(originalPage);
}

bool ShadowFile::readShadowVersionIfExists(file_idx_t originalFile, page_idx_t originalPage,
    const std::function<void(uint8_t*)>& readOp) const {
    if (!hasShadowPages.load(std::memory_order_acquire)) {
        return false;
    }
    std::shared_lock lck{mtx};
    if (!hasShadowPage(originalFile, originalPage)) {
        return false;
    }
    // Optimistic reads let concurrent readers share the page and retry if the checkpointer is
    // updating it at the same time.
    shadowingFH->optimisticReadPage(shadowPagesMap.at(originalFile).at(originalPage), readOp);
    return true;
}

void ShadowFile::applyShadowPages(StorageManager& storageManager, ClientContext& context) const {
    const auto pageBuffer = std::make_unique<uint8_t[]>(LBUG_PAGE_SIZE);
    page_idx_t shadowPageIdx = 1; // Skip header page.
    auto* bm = MemoryManager::Get(context)->getBufferManager();
    auto dataFH = storageManager.getDataFH();
    std::unordered_set<FileHandle*> modifiedFiles;
    DASSERT(shadowingFH);
    for (const auto& record : shadowPageRecords) {
        shadowingFH->readPageFromDisk(pageBuffer.get(), shadowPageIdx++);
        FileHandle* targetFH = (dataFH && record.originalFileIdx == dataFH->getFileIndex()) ?
                                   dataFH :
                                   bm->getFileHandle(record.originalFileIdx);
        DASSERT(targetFH && targetFH->getFileInfo());
        targetFH->getFileInfo()->writeFile(pageBuffer.get(), LBUG_PAGE_SIZE,
            record.originalPageIdx * LBUG_PAGE_SIZE);
        modifiedFiles.insert(targetFH);
        // Acquire page state lock before updating the in-memory frame. This ensures concurrent
        // optimistic readers will detect the version change and retry, seeing the new page data.
        bm->updateFrameIfPageIsInFrame(record.originalFileIdx, pageBuffer.get(),
            record.originalPageIdx);
    }
    for (auto* fh : modifiedFiles) {
        fh->getFileInfo()->syncFile();
    }
    if (modifiedFiles.empty() && dataFH && dataFH->getFileInfo()) {
        dataFH->getFileInfo()->syncFile();
    }
}

static uuid getOldDatabaseID(FileInfo& dataFileInfo) {
    auto oldHeader = DatabaseHeader::readDatabaseHeader(dataFileInfo);
    if (!oldHeader.has_value()) {
        throw InternalException("Found a shadow file for database {} but no valid database header. "
                                "The database is corrupted, please recreate it.");
    }
    return oldHeader->databaseID;
}

void ShadowFile::replayShadowPageRecords(ClientContext& context) {
    replayShadowPageRecords(context, context.getDatabase()->getStorageManager()->getDataFilePath());
}

void ShadowFile::replayShadowPageRecords(ClientContext& context, const std::string& databasePath,
    std::optional<uuid> legacyDatabaseID) {
    if (context.getDBConfig()->readOnly) {
        throw RuntimeException("Couldn't replay shadow pages under read-only mode. Please re-open "
                               "the database with read-write mode to replay shadow pages.");
    }
    auto vfs = VirtualFileSystem::GetUnsafe(context);
    auto shadowFilePath = StorageUtils::getShadowFilePath(databasePath);
    auto shadowFileInfo = vfs->openFile(shadowFilePath, FileOpenFlags(FileFlags::READ_ONLY));

    std::unique_ptr<FileInfo> dataFileInfo;
    try {
        dataFileInfo = vfs->openFile(databasePath,
            FileOpenFlags{FileFlags::WRITE | FileFlags::READ_ONLY, FileLockType::WRITE_LOCK});
    } catch (IOException& e) {
        throw RuntimeException(std::format(
            "Found shadow file {} but no corresponding database file. This file "
            "may have been left behind from a previous database with the same name. If it is safe "
            "to do so, please delete this file and restart the database.",
            shadowFilePath));
    }
    replayShadowPageRecordsCore(context, *shadowFileInfo, *dataFileInfo,
        context.getDBConfig()->maxDBSize / LBUG_PAGE_SIZE, legacyDatabaseID);
}

void ShadowFile::replayShadowPageRecordsForStorageManager(ClientContext& context,
    StorageManager& storageManager, std::optional<uuid> legacyDatabaseID) {
    // Variant for files whose handle is already open and locked by the given storage manager
    // (partition children during recovery): avoids taking a second lock on the data file,
    // which fails on platforms with per-handle locks (Windows).
    if (context.getDBConfig()->readOnly) {
        throw RuntimeException("Couldn't replay shadow pages under read-only mode. Please re-open "
                               "the database with read-write mode to replay shadow pages.");
    }
    auto vfs = VirtualFileSystem::GetUnsafe(context);
    auto shadowFilePath = StorageUtils::getShadowFilePath(storageManager.getDataFilePath());
    auto shadowFileInfo = vfs->openFile(shadowFilePath, FileOpenFlags(FileFlags::READ_ONLY));
    auto* dataFH = storageManager.getDataFH();
    replayShadowPageRecordsCore(context, *shadowFileInfo, *dataFH->getFileInfo(),
        context.getDBConfig()->maxDBSize / LBUG_PAGE_SIZE, legacyDatabaseID, &storageManager);
    const auto fileSize = dataFH->getFileInfo()->getFileSize();
    const auto numPages = (fileSize + dataFH->getPageSize() - 1) / dataFH->getPageSize();
    if (numPages > dataFH->getNumPages()) {
        dataFH->addNewPages(numPages - dataFH->getNumPages());
    }
}

void ShadowFile::replayShadowPageRecordsCore(ClientContext& context, FileInfo& shadowFileInfo,
    FileInfo& dataFileInfo, uint64_t maxDatabasePages, std::optional<uuid> legacyDatabaseID,
    StorageManager* storageManager) {
    const auto shadowFileSize = shadowFileInfo.getFileSize();
    if (shadowFileSize < LBUG_PAGE_SIZE + sizeof(uint64_t)) {
        throw RuntimeException(
            std::format("Cannot replay shadow file {}: file is too small.", shadowFileInfo.path));
    }
    ShadowFileHeader header;
    const auto headerBuffer = std::make_unique<uint8_t[]>(LBUG_PAGE_SIZE);
    shadowFileInfo.readFromFile(headerBuffer.get(), LBUG_PAGE_SIZE, 0);
    memcpy(&header, headerBuffer.get(), sizeof(ShadowFileHeader));
    if (header.fileManifestVersion != 0 && header.fileManifestVersion != FILE_MANIFEST_VERSION) {
        throw RuntimeException(std::format("Cannot replay shadow file {}: unsupported target-file "
                                           "manifest version {}.",
            shadowFileInfo.path, header.fileManifestVersion));
    }
    if (storageManager && storageManager->isDecoupledStorage() && header.fileManifestVersion == 0) {
        throw RuntimeException(std::format("Cannot recover committed checkpoint: shadow file {} "
                                           "has no target-file manifest for decoupled storage. "
                                           "Keep the WAL and shadow files in place.",
            shadowFileInfo.path));
    }
    const auto maxShadowPagesInFile = shadowFileSize / LBUG_PAGE_SIZE - 1;
    if (header.numShadowPages == INVALID_PAGE_IDX || header.numShadowPages > maxShadowPagesInFile) {
        throw RuntimeException(
            std::format("Cannot replay shadow file {}: invalid shadow page count {}.",
                shadowFileInfo.path, header.numShadowPages));
    }
    if (header.numShadowPages > maxDatabasePages) {
        throw RuntimeException(std::format(
            "Cannot replay shadow file {}: it needs {} shadow pages, but max_db_size limits "
            "the database to {} pages ({} bytes). Reopen the database with a larger "
            "max_db_size, keeping the WAL and shadow files in place.",
            shadowFileInfo.path, header.numShadowPages, maxDatabasePages,
            maxDatabasePages * LBUG_PAGE_SIZE));
    }
    const auto recordsOffset = (static_cast<uint64_t>(header.numShadowPages) + 1) * LBUG_PAGE_SIZE;
    const auto recordsSize = sizeof(uint64_t) + static_cast<uint64_t>(header.numShadowPages) *
                                                    (sizeof(file_idx_t) + sizeof(page_idx_t));
    if (recordsOffset > shadowFileSize || recordsSize > shadowFileSize - recordsOffset) {
        throw RuntimeException(std::format(
            "Cannot replay shadow file {}: page records are truncated.", shadowFileInfo.path));
    }

    // When replaying the shadow file we haven't read the database ID from the database
    // header yet
    // So we need to do it separately here to verify the shadow file matches the database
    const auto oldDatabaseID = getOldDatabaseID(dataFileInfo);
    const bool usesLegacyDatabaseID = header.databaseID.value != oldDatabaseID.value &&
                                      legacyDatabaseID.has_value() &&
                                      header.databaseID.value == legacyDatabaseID->value;
    if (header.databaseID.value != oldDatabaseID.value && !usesLegacyDatabaseID) {
        FileDBIDUtils::verifyDatabaseID(shadowFileInfo, oldDatabaseID, header.databaseID);
    }

    auto reader = std::make_unique<BufferedFileReader>(shadowFileInfo);
    reader->resetReadOffset(recordsOffset);
    Deserializer deSer(std::move(reader));
    uint64_t numShadowPageRecords = 0;
    deSer.deserializeValue(numShadowPageRecords);
    if (numShadowPageRecords != header.numShadowPages) {
        throw RuntimeException(std::format(
            "Cannot replay shadow file {}: header declares {} pages but the record list declares "
            "{}.",
            shadowFileInfo.path, header.numShadowPages, numShadowPageRecords));
    }

    const auto dataFileSize = dataFileInfo.getFileSize();
    const auto currentDataPages =
        dataFileSize / LBUG_PAGE_SIZE + static_cast<uint64_t>(dataFileSize % LBUG_PAGE_SIZE != 0);
    if (currentDataPages > maxDatabasePages) {
        throw RuntimeException(std::format(
            "Cannot replay shadow file {}: database file exceeds the configured maximum size.",
            shadowFileInfo.path));
    }
    std::vector<ShadowPageRecord> records;
    records.reserve(numShadowPageRecords);
    for (auto i = 0u; i < numShadowPageRecords; i++) {
        const auto record = ShadowPageRecord::deserialize(deSer);
        if (record.originalFileIdx == INVALID_FILE_IDX ||
            record.originalPageIdx == INVALID_PAGE_IDX) {
            throw RuntimeException(
                std::format("Cannot replay shadow file {}: invalid target page {}.",
                    shadowFileInfo.path, record.originalPageIdx));
        }
        if (record.originalPageIdx >= maxDatabasePages) {
            throw RuntimeException(std::format(
                "Cannot replay shadow file {}: its target page {} is beyond the {} pages "
                "({} bytes) allowed by max_db_size. Reopen the database with a larger "
                "max_db_size, keeping the WAL and shadow files in place.",
                shadowFileInfo.path, record.originalPageIdx, maxDatabasePages,
                maxDatabasePages * LBUG_PAGE_SIZE));
        }
        records.push_back(record);
    }

    // Persisted file indices only identify entries in this manifest. Never resolve them through
    // the recovering process's buffer manager: its file-opening order may be different.
    std::unordered_map<file_idx_t, ShadowFileTarget> targets;
    file_idx_t metadataFileIdx = INVALID_FILE_IDX;
    const auto dataPath = std::filesystem::path(dataFileInfo.path).lexically_normal();
    if (header.fileManifestVersion != 0) {
        auto requireBytes = [&](uint64_t size) {
            const auto offset = deSer.getReadOffset();
            if (offset > shadowFileSize || size > shadowFileSize - offset) {
                throw RuntimeException(std::format("Cannot replay shadow file {}: target-file "
                                                   "manifest is truncated.",
                    shadowFileInfo.path));
            }
        };
        requireBytes(sizeof(uint64_t));
        uint64_t numTargets = 0;
        deSer.deserializeValue(numTargets);
        const auto minEntrySize = sizeof(file_idx_t) + sizeof(page_idx_t) + sizeof(uint64_t);
        if (numTargets == 0 ||
            numTargets > (shadowFileSize - deSer.getReadOffset()) / minEntrySize) {
            throw RuntimeException(std::format("Cannot replay shadow file {}: invalid target-file "
                                               "manifest size {}.",
                shadowFileInfo.path, numTargets));
        }
        std::unordered_set<std::string> paths;
        uint64_t totalTargetPages = 0;
        for (uint64_t i = 0; i < numTargets; ++i) {
            requireBytes(sizeof(file_idx_t) + sizeof(page_idx_t) + sizeof(uint64_t));
            ShadowFileTarget target{};
            deSer.deserializeValue(target.fileIdx);
            deSer.deserializeValue(target.numPages);
            uint64_t pathLength = 0;
            deSer.deserializeValue(pathLength);
            requireBytes(pathLength);
            if (pathLength > 4096 || target.fileIdx == INVALID_FILE_IDX ||
                target.numPages > maxDatabasePages - totalTargetPages) {
                throw RuntimeException(std::format("Cannot replay shadow file {}: invalid "
                                                   "target-file manifest entry.",
                    shadowFileInfo.path));
            }
            target.relativePath.resize(pathLength);
            deSer.read(reinterpret_cast<uint8_t*>(target.relativePath.data()), pathLength);
            if (!isValidTargetPath(target.relativePath, dataPath.filename().string()) ||
                !paths.insert(target.relativePath).second || targets.contains(target.fileIdx)) {
                throw RuntimeException(std::format("Cannot replay shadow file {}: invalid or "
                                                   "duplicate target-file path {}.",
                    shadowFileInfo.path, target.relativePath));
            }
            totalTargetPages += target.numPages;
            if (target.relativePath == dataPath.filename().string()) {
                metadataFileIdx = target.fileIdx;
                target.fileInfo = &dataFileInfo;
            }
            targets.emplace(target.fileIdx, std::move(target));
        }
        if (metadataFileIdx == INVALID_FILE_IDX) {
            throw RuntimeException(std::format("Cannot replay shadow file {}: metadata file is "
                                               "missing from the target-file manifest.",
                shadowFileInfo.path));
        }
        for (const auto& record : records) {
            auto target = targets.find(record.originalFileIdx);
            if (target == targets.end() || record.originalPageIdx >= target->second.numPages) {
                throw RuntimeException(std::format("Cannot replay shadow file {}: invalid target "
                                                   "file or page {}.",
                    shadowFileInfo.path, record.originalPageIdx));
            }
        }
    }

    // The last database header page record identifies both the checkpoint's Database ID and,
    // through the header's dataFileNumPages, the page extent the checkpoint allocated. Failed
    // pre-marker checkpoint attempts can leave that extent ahead of the physical file size, so
    // the extent recorded in the committed checkpoint is the only valid upper bound for target
    // pages, not the physical file size plus the shadow page count.
    const auto headerRecord =
        std::find_if(records.rbegin(), records.rend(), [&](const auto& record) {
            return record.originalPageIdx == StorageConstants::DB_HEADER_PAGE_IDX &&
                   (metadataFileIdx == INVALID_FILE_IDX ||
                       record.originalFileIdx == metadataFileIdx);
        });
    if (headerRecord == records.rend()) {
        throw RuntimeException(std::format(
            "Cannot replay shadow file {}: database header page is missing.", shadowFileInfo.path));
    }
    const auto headerShadowPageIdx = static_cast<page_idx_t>(
        records.size() - static_cast<size_t>(std::distance(records.rbegin(), headerRecord)));
    const auto headerPage = std::make_unique<uint8_t[]>(LBUG_PAGE_SIZE);
    shadowFileInfo.readFromFile(headerPage.get(), LBUG_PAGE_SIZE,
        headerShadowPageIdx * LBUG_PAGE_SIZE);
    Deserializer headerDeserializer{
        std::make_unique<BoundedPageReader>(headerPage.get(), LBUG_PAGE_SIZE,
            std::format("Cannot recover committed checkpoint: shadow file {} holds a "
                        "corrupt database header page.",
                shadowFileInfo.path))};
    const auto checkpointHeader = DatabaseHeader::deserialize(headerDeserializer);
    if (checkpointHeader.databaseID.value != oldDatabaseID.value) {
        throw RuntimeException(std::format(
            "Cannot recover committed checkpoint: shadow file {} does not match the Database "
            "ID "
            "of {}. Do not delete the shadow file; it holds committed pages. Restore the "
            "database file it was written for.",
            shadowFileInfo.path, dataFileInfo.path));
    }
    if (header.fileManifestVersion == 0) {
        for (const auto& record : records) {
            if (record.originalFileIdx != headerRecord->originalFileIdx ||
                record.originalPageIdx >= checkpointHeader.dataFileNumPages) {
                throw RuntimeException(
                    std::format("Cannot replay shadow file {}: invalid target page {}.",
                        shadowFileInfo.path, record.originalPageIdx));
            }
        }
    } else {
        if (targets.at(metadataFileIdx).numPages != checkpointHeader.dataFileNumPages) {
            throw RuntimeException(
                std::format("Cannot replay shadow file {}: metadata extent does not match the "
                            "target-file manifest.",
                    shadowFileInfo.path));
        }
        auto* vfs = VirtualFileSystem::GetUnsafe(context);
        const auto openFiles = storageManager && storageManager->getFileRegistry() ?
                                   storageManager->getFileRegistry()->getAllFiles() :
                                   std::vector<FileHandle*>{};
        // Resolve and validate every file before the first write. A missing file must leave the
        // committed WAL/shadow bundle and all existing data files intact for a later retry.
        for (auto& [_, target] : targets) {
            if (target.fileInfo) {
                continue;
            }
            const auto targetPath = dataPath.parent_path() / target.relativePath;
            for (auto* fh : openFiles) {
                if (fh->getFileInfo() &&
                    std::filesystem::path(fh->getFileInfo()->path).lexically_normal() ==
                        targetPath) {
                    target.fileHandle = fh;
                    target.fileInfo = fh->getFileInfo();
                    break;
                }
            }
            if (!target.fileInfo) {
                target.ownedFileInfo = vfs->openFile(targetPath.string(),
                    FileOpenFlags(FileFlags::READ_ONLY | FileFlags::WRITE,
                        FileLockType::WRITE_LOCK),
                    &context);
                target.fileInfo = target.ownedFileInfo.get();
            }
            if (target.fileInfo->getFileSize() / LBUG_PAGE_SIZE > maxDatabasePages) {
                throw RuntimeException(std::format("Cannot replay shadow file {}: target file {} "
                                                   "exceeds the configured maximum size.",
                    shadowFileInfo.path, target.relativePath));
            }
        }
    }

    const auto pageBuffer = std::make_unique<uint8_t[]>(LBUG_PAGE_SIZE);
    page_idx_t shadowPageIdx = 1;
    for (const auto& record : records) {
        shadowFileInfo.readFromFile(pageBuffer.get(), LBUG_PAGE_SIZE,
            shadowPageIdx * LBUG_PAGE_SIZE);
        auto* target =
            header.fileManifestVersion == 0 ? nullptr : &targets.at(record.originalFileIdx);
        auto* fileInfo = target ? target->fileInfo : &dataFileInfo;
        fileInfo->writeFile(pageBuffer.get(), LBUG_PAGE_SIZE,
            record.originalPageIdx * LBUG_PAGE_SIZE);
        if (target && target->fileHandle) {
            MemoryManager::Get(context)->getBufferManager()->updateFrameIfPageIsInFrame(
                target->fileHandle->getFileIndex(), pageBuffer.get(), record.originalPageIdx);
        }
        shadowPageIdx++;
    }
    if (header.fileManifestVersion == 0) {
        dataFileInfo.syncFile();
    } else {
        for (auto& [_, target] : targets) {
            target.fileInfo->syncFile();
        }
    }
}

void ShadowFile::flushAll(const uuid& ownerDatabaseID) const {
    std::vector<ShadowFileTarget> targets;
    if (multiFile) {
        auto dataPath = std::filesystem::path(shadowFilePath);
        dataPath.replace_extension();
        dataPath = dataPath.lexically_normal();
        std::unordered_set<file_idx_t> seen;
        std::unordered_set<std::string> syncedDirectories;
        auto files = owningStorageManager->getFileRegistry()->getAllFiles();
        files.push_back(owningStorageManager->getDataFH());
        // Include files written directly by COPY/new-chunk flushing as well as shadowed files.
        // They also need durability and existence checks before publishing/replaying metadata.
        for (auto* fh : files) {
            if (!seen.insert(fh->getFileIndex()).second) {
                continue;
            }
            DASSERT(fh && fh->getFileInfo());
            const auto path = std::filesystem::path(fh->getFileInfo()->path).lexically_normal();
            const auto relativePath = path.lexically_relative(dataPath.parent_path()).string();
            if (!isValidTargetPath(relativePath, dataPath.filename().string())) {
                throw RuntimeException(std::format("Cannot checkpoint target file {} outside "
                                                   "the decoupled database layout.",
                    path.string()));
            }
            ShadowFileTarget target{};
            target.fileIdx = fh->getFileIndex();
            target.numPages = fh->getNumPages();
            target.relativePath = relativePath;
            targets.push_back(std::move(target));
            // New COPY pages and directory entries must be durable before the WAL's checkpoint
            // marker. Recovery opens existing files only; it must never silently recreate one.
            fh->getFileInfo()->syncFile();
            auto directoryEntry = path;
            while (directoryEntry != dataPath.parent_path()) {
                if (syncedDirectories.insert(directoryEntry.parent_path().string()).second) {
                    vfs->syncParentDirectory(directoryEntry.string());
                }
                directoryEntry = directoryEntry.parent_path();
            }
        }
        vfs->syncParentDirectory(dataPath.parent_path().string());
    }
    // Write header page to file.
    ShadowFileHeader header;
    header.numShadowPages = shadowPageRecords.size();
    header.databaseID = CHECKPOINT_BUNDLE_DATABASE_ID;
    header.ownerDatabaseID = ownerDatabaseID;
    header.fileManifestVersion = multiFile ? FILE_MANIFEST_VERSION : 0;
    const auto headerBuffer = std::make_unique<uint8_t[]>(LBUG_PAGE_SIZE);
    memcpy(headerBuffer.get(), &header, sizeof(ShadowFileHeader));
    DASSERT(shadowingFH && !shadowingFH->isInMemoryMode());
    shadowingFH->writePageToFile(headerBuffer.get(), 0);
    // Flush shadow pages to file.
    shadowingFH->flushAllDirtyPagesInFrames();
    // Append shadow page records to the end of the file.
    const auto writer = std::make_shared<BufferedFileWriter>(*shadowingFH->getFileInfo());
    writer->setFileOffset(shadowingFH->getNumPages() * LBUG_PAGE_SIZE);
    Serializer ser(writer);
    DASSERT(shadowPageRecords.size() + 1 == shadowingFH->getNumPages());
    ser.serializeVector(shadowPageRecords);
    if (multiFile) {
        ser.serializeVector(targets);
    }
    writer->flush();
    // Sync the file to disk.
    writer->sync();
}

void ShadowFile::clear(BufferManager& bm, bool syncParentDirectory) {
    DASSERT(shadowingFH);
    // Concurrent readers may be reading shadow pages (see readShadowVersionIfExists).
    std::unique_lock lck{mtx};
    // Evict pages, truncate to zero pages (which also truncates the on-disk file), then unlink
    // the path and drop the stale fd so no leftover .shadow file exists at rest (a present
    // .shadow is treated as "checkpoint in progress" by recovery/startup probes). The
    // retained FileHandle keeps its VM frame groups (see reset()); the file is re-created
    // lazily by getOrCreateShadowingFH on the next checkpoint, which also re-reserves the
    // header page.
    bm.removeFilePagesFromFrames(*shadowingFH);
    shadowPagesMap.clear();
    shadowPageRecords.clear();
    hasShadowPages.store(false, std::memory_order_release);
    shadowingFH->resetToZeroPagesAndPageCapacity();
    shadowingFH->resetFileInfo();
    vfs->removeFileIfExists(shadowFilePath);
    if (syncParentDirectory) {
        vfs->syncParentDirectory(shadowFilePath);
    }
}

void ShadowFile::reset() {
    // Keep the shadowing FileHandle (and its VM frame groups) alive across checkpoints:
    // each FileHandle allocates VMRegion frame groups that count against max_db_size and
    // are never reclaimed, so orphaning the handle here exhausted max_db_size after a
    // handful of checkpoints (see #924). Evict pages, truncate to zero pages (which also
    // truncates the on-disk file), drop the stale fd, and unlink the path so
    // recovery/startup probes — which treat a present .shadow file as "checkpoint in
    // progress" — don't trip on it. getOrCreateShadowingFH re-opens the path into the
    // same retained handle on the next checkpoint, so subsequent shadow pages land in a
    // real on-disk file visible to recovery (which replays by path).
    if (shadowingFH == nullptr) {
        return;
    }
    std::unique_lock lck{mtx};
    shadowPagesMap.clear();
    shadowPageRecords.clear();
    hasShadowPages.store(false, std::memory_order_release);
    // If clear() already ran (the normal checkpoint flow calls both), the file was truncated,
    // unlinked and the fd dropped; just make sure the in-memory maps are empty.
    if (shadowingFH->getFileInfo() != nullptr) {
        bm.removeFilePagesFromFrames(*shadowingFH);
        shadowingFH->resetToZeroPagesAndPageCapacity();
        shadowingFH->resetFileInfo();
        vfs->removeFileIfExists(shadowFilePath);
        vfs->syncParentDirectory(shadowFilePath);
    }
}

FileHandle* ShadowFile::getOrCreateShadowingFH() {
    if (!shadowingFH) {
        shadowingFH = bm.getFileHandle(shadowFilePath,
            FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs, nullptr);
    } else if (shadowingFH->getFileInfo() == nullptr) {
        // reset() unlinked the on-disk file and dropped the stale fd (which pointed at the
        // unlinked inode): re-open the same retained FileHandle in place so its identity
        // (fileIndex, frame groups) is preserved and no new VM accounting is consumed.
        // Matches constructPersistentFileHandle's flags for O_PERSISTENT_FILE_CREATE_NOT_EXISTS.
        shadowingFH->setFileInfo(vfs->openFile(shadowFilePath,
            FileOpenFlags(
                FileFlags::WRITE | FileFlags::READ_ONLY | FileFlags::CREATE_IF_NOT_EXISTS),
            nullptr));
    }
    if (shadowingFH->getNumPages() == 0) {
        // Reserve the first page for the header.
        shadowingFH->addNewPage();
    }
    return shadowingFH;
}

} // namespace storage
} // namespace lbug
