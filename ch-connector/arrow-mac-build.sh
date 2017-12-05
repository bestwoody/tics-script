set -eu

# Remove '-stdlib=libc++' in arrow/cpp/*
cf="cpp/cmake_modules/SetupCxxFlags.cmake"
cp "arrow-delta/$cf" "arrow/$cf"
cf="cpp/cmake_modules/ThirdpartyToolchain.cmake"
cp "arrow-delta/$cf" "arrow/$cf"

build_path="g++-6_build"
cd arrow/cpp
mkdir -p "$build_path"
cd "$build_path"

# Find gcc path in homebrew
gcc_path=`find /usr/local/Cellar/gcc@6 -name stdlib.h | head -n 1`
gcc_path=`dirname "$gcc_path"`

if [ -z "$gcc_path" ]; then
	echo "gcc path not found, exiting" >&2
	exit 1
fi

# Env vars
export CPLUS_INCLUDE_PATH="$gcc_path":/usr/local/include:/usr/include
export C_INCLUDE_PATH=$CPLUS_INCLUDE_PATH
export CC="gcc-6"
export CXX="g++-6"

# Just for debug build
cmake .. -DARROW_BUILD_TESTS=off -DARROW_WITH_SNAPPY=off -DARROW_WITH_ZLIB=off -DARROW_WITH_ZSTD=off

# Remove '-stdlib=libc++' in arrow/cpp/g++-6_build/*
cf="flatbuffers_ep-prefix/src/flatbuffers_ep/CMakeLists.txt"
cp "../../../arrow-delta/cpp/g++-6_build/$cf" "$cf"
#cf="cpp/g++-6_build/flatbuffers_ep-prefix/src/flatbuffers_ep-build/CMakeFiles/flatbuffers.dir/flags.make"
#cp "arrow-delta/$cf" "arrow/$cf"

make
make install
