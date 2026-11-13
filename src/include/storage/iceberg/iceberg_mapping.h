#pragma once

#include "function/function.h"

namespace lbug::main {
class Database;
}

namespace lbug::iceberg {

void registerIcebergMapping(main::Database& database);

struct RefreshCSR {
    static constexpr const char* name = "ICEBERG_CSR_REFRESH";
    static function::function_set getFunctionSet();
};

} // namespace lbug::iceberg
