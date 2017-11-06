add_library(JNI
    Version.cpp
    )

target_include_directories (clickhouse-server PUBLIC ${ClickHouse_SOURCE_DIR}/libs/libdaemon/include)
