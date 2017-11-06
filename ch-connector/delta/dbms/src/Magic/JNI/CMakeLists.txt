target_include_directories(ch PRIVATE ${ClickHouse_SOURCE_DIR}/../../protocol/c-jni)

find_package(JNI)
if (JNI_FOUND)
    target_include_directories(ch PRIVATE ${JNI_INCLUDE_DIRS})
endif()
