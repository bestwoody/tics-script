# Build projects on Mac OS (with g++-6)

## Build CH
* Building CH strickly require g++(not clang wrap in g++ alias)
* Follow instructings on CH docs
* Or just run `ch-connector> ./clickhouse-build.sh`

## Build POC projects (Benchmark, Protocol, etc)
* No special things.
* Normally, run `./build.sh` will be fine.

## Build CH-Connector
* Steps
    * Install boost (for arrow)
    * Git clone arrow, build with g++ (manually, follow notes bellow)
    * Apply patch we made for CH: `ch-connector> ./delta-apply.sh`
    * Build CH-Connector: `ch-connector> ./libch-build.sh`
* Reasons
    * CH require g++ as compiler, also require all libs compiled with g++.
    * So CH build(from source) and link all libs as static libs.
    * We also need arrow built by g++.

## Notes of g++ on Mac OS
* Use the right way to specify compiler
    * Cmake args `CMAKE_C_COMPILER=gcc-6` `DCMAKE_CXX_COMPILER=g++-6`
        * OK if project has no submodule.
        * Not OK if project has submodule:
            * Cmake args not pass though submodules
    * Env vars `CC=gcc-6` `CXX=g++-6`
        * OK
* Make sure the compiler including paths are in right order.
    * The first one must be the g++ including dir
* Disable `__float128` support
    * Remove `_GLIBCXX_USE_FLOAT128` marco in `<g++ including dir>/.../c++config.h`
        * Totally remove it, not just set to `0` (default: `1`)
        * Because some project check this marco's existence, not it's value.
* Arrow building problem:
    * Cmake args like `-DARROW_WITH_SNAPPY=off` do not functional
    * Because `ARROW_BUILD_TESTS` will open it again, and `ARROW_BUILD_TESTS` is `on` by default
* Zlib building bug:
    * Some code lines are generated by `#define` with checking steps
        * Generated code: function prototype `z_off_t` `z_off64_t`
        * Generated code: Type definitions `adler32_combine64`
        * Deferrent checking steps cause `adler32_combine64`'s prototype not match it's impl.
        * Work around: disable zlib feature
            * pass args `-DARROW_BUILD_TESTS=off` and `-DARROW_WITH_SNAPPY=off` to cmake
            * For developing only
    * Checking list
        * Platform
        * System source files
        * Type size
        * Building flags
* Flatbuffer building bug:
    * Arrow depend on it.
    * Flatbuffer assume using clang when building on Mac OS, fail to build:
        * Use clang-only compiler arg: `-stdlib=libc++`
            * Source: [CMakeLists.txt#L109](https://github.com/google/flatbuffers/blob/d233b38008f30cb671fe03f14963806ffcbf99cb/CMakeLists.txt#L109)
        * Fix: manually remove this arg from `<flatbuffers>/CMakeLists.txt`