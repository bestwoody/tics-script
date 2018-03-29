cmake_minimum_required(VERSION 3.1)
project(TheFlashProtoBench VERSION 1.0.0 LANGUAGES C CXX)

set(CMAKE_CXX_FLAGS "-std=c++11 -pthread -O3")

include_directories(../../c-jni)
aux_source_directory(./ DIR_SRCS)

find_package(JNI)
if (JNI_FOUND)
	include_directories(${JNI_INCLUDE_DIRS})
endif()

add_library(bench SHARED ${DIR_SRCS})
add_executable(main ${DIR_SRCS})

target_link_libraries(bench arrow)
target_link_libraries(main arrow)
