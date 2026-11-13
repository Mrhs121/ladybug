add_executable(iceberg_query_bench EXCLUDE_FROM_ALL bench.cpp)
if(TARGET lbug_shared)
    target_link_libraries(iceberg_query_bench PRIVATE lbug_shared iceberg_duckdb)
else()
    target_link_libraries(iceberg_query_bench PRIVATE lbug iceberg_duckdb)
endif()

# The legacy attached DuckDB reader has its own thread setting.
if(UNIX AND TARGET lbug_shared)
    add_library(iceberg_bench_reader MODULE EXCLUDE_FROM_ALL foreign_reader_threads.cpp)
    target_link_libraries(iceberg_bench_reader PRIVATE lbug_shared iceberg_duckdb)
endif()
