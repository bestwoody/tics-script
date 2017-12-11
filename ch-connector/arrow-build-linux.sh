set -eu

cd arrow/cpp

build_path="g++_build"
mkdir -p "$build_path"
cd "$build_path"

cmake ..
make
make install
