#file(GLOB_RECURSE magic-patch *.cpp)
#add_library(ch SHARED ${magic-patch})
add_library(ch SHARED JNI.cpp Bench.cpp)

target_link_libraries (ch clickhouse_common_io daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions arrow)

target_include_directories(ch PRIVATE ${ClickHouse_SOURCE_DIR}/../../protocol/c-jni)

find_package(JNI)
if (JNI_FOUND)
    target_include_directories(ch PRIVATE ${JNI_INCLUDE_DIRS})
endif()

add_executable(ch-raw CHRaw.cpp)

target_link_libraries (ch-raw clickhouse_common_io daemon clickhouse_storages_system clickhouse_functions clickhouse_aggregate_functions clickhouse_table_functions arrow)
