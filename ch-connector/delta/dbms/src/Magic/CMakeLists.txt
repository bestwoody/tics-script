file(GLOB_RECURSE magic-patch *.cpp)
add_library(ch SHARED ${magic-patch})
target_link_libraries(ch
    clickhouse-client
    clickhouse-local
    clickhouse-extract-from-config
    clickhouse-compressor-lib)
add_subdirectory(JNI)

add_executable(ch-raw main.cpp)
target_link_libraries(ch-raw
    clickhouse-client
    clickhouse-local
    clickhouse-extract-from-config
    clickhouse-compressor-lib)
