cmake_minimum_required(VERSION 3.1)
project(PatternIO VERSION 1.0.0 LANGUAGES C CXX)

include_directories(src)

if(APPLE)
	message("Use g++-8 on Mac OS")
	set(CMAKE_CXX_COMPILER "g++-8")
else()
	set(CMAKE_CXX_COMPILER "g++")
endif()

set(CMAKE_CXX_FLAGS "-std=c++17 -pthread -O3 -mmmx -msse -msse2 -msse3 -msse4")

aux_source_directory(src/app DIR_SRCS)

add_executable(moonshine ${DIR_SRCS})
