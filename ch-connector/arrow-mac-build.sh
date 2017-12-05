set -eu

# Manually remove '-stdlib=libc++' in arrow/cpp/*{CMakeLists.txt}

cd arrow/cpp
mkdir -p g++-6_build
cd g++-6_build

gcc_path=`find /usr/local/Cellar/gcc@6 -name stdlib.h | head -n 1`
gcc_path=`dirname "$gcc_include"`

export CPLUS_INCLUDE_PATH="$gcc_path":/usr/local/include:/usr/include
export C_INCLUDE_PATH=$CPLUS_INCLUDE_PATH
export CC="gcc-6"
export CXX="g++-6"

cmake .. -DARROW_BUILD_TESTS=off -DARROW_WITH_SNAPPY=off -DARROW_WITH_ZLIB=off -DARROW_WITH_ZSTD=off

# Manually remove '-stdlib=libc' in arrow/cpp/g++-6_build/*

make
make install
