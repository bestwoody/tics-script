#file(GLOB_RECURSE magic-patch *.cpp)
#add_library(ch SHARED ${magic-patch})
add_library(ch SHARED JNI.cpp Bench.cpp)

target_link_libraries(ch
    clickhouse-client
    clickhouse-local
    clickhouse-extract-from-config
    clickhouse-compressor-lib
    arrow)

target_include_directories(ch PRIVATE ${ClickHouse_SOURCE_DIR}/../../protocol/c-jni)

find_package(JNI)
if (JNI_FOUND)
    target_include_directories(ch PRIVATE ${JNI_INCLUDE_DIRS})
endif()

add_executable(ch-raw CHRaw.cpp)

target_link_libraries(ch-raw
    clickhouse-client
    clickhouse-local
    clickhouse-extract-from-config
    clickhouse-compressor-lib)
