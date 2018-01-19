set -eu

source ./_helper.sh
setup_gcc_on_mac

# Remove '-stdlib=libc++' in arrow/cpp/*
cf="cpp/cmake_modules/SetupCxxFlags.cmake"
cp "arrow-delta/$cf" "arrow/$cf"
cf="cpp/cmake_modules/ThirdpartyToolchain.cmake"
cp "arrow-delta/$cf" "arrow/$cf"

build_path="g++_build"
cd arrow/cpp
mkdir -p "$build_path"
cd "$build_path"

# Just for debug build
cmake .. -DARROW_BUILD_TESTS=off -DARROW_WITH_SNAPPY=off -DARROW_WITH_ZLIB=off -DARROW_WITH_ZSTD=off

# Remove '-stdlib=libc++' in arrow/cpp/g++_build/*
cf="flatbuffers_ep-prefix/src/flatbuffers_ep/CMakeLists.txt"
cp "../../../arrow-delta/cpp/g++_build/$cf" "$cf"

make

echo "sudo make install"
sudo make install
