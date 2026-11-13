add_executable(wal_dump main.cpp)

if(TARGET lbug)
    target_link_libraries(wal_dump PRIVATE lbug)
else()
    target_link_libraries(wal_dump PRIVATE lbug_shared)
endif()

install(TARGETS wal_dump)
