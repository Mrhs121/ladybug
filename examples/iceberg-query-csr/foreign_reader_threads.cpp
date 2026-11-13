#include <cstdio>

#include "duckdb.hpp"
#include "main/attached_database.h"
#include "main/connection.h"
#include "main/database_manager.h"

extern "C" LBUG_HELPER_DLL_EXPORT int attached_reader_threads(lbug::main::Connection* connection,
    int threads) {
    try {
        auto* manager = lbug::main::DatabaseManager::Get(*connection->getClientContext());
        auto* db = manager->getAttachedDatabase("g");
        auto setting = db->executeQuery("SET threads=" + std::to_string(threads));
        if (setting->HasError()) {
            throw std::runtime_error(setting->GetError());
        }
        auto result = db->executeQuery("SELECT current_setting('threads')");
        if (result->HasError()) {
            throw std::runtime_error(result->GetError());
        }
        return result->Fetch()->GetValue(0, 0).GetValue<int32_t>();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "%s\n", e.what());
        return -1;
    }
}
