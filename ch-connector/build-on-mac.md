# Build projects on Mac OS (with g++-6)

## Build CH
* Building CH strickly require g++(not clang with g++ alias)
* Follow instructings on CH docs
* Or just `ch-connector> ./clickhouse-build.sh`

## Build POC projects
* No special things.
* Normally, run `./build.sh` will be fine.

## Build CH-Connector
* Steps
    * Install boost (for arrow)
    * Git clone arrow, build with g++
    * Apply patch we made for CH: `ch-connector> ./delta-apply.sh`
    * Build CH-Connector: `ch-connector> ./libch-build.sh`
* Reasons
    * CH require g++ as compiler, also require all libs compiled with g++.
    * So CH build(from source) and link all libs as static libs.
    * We alos need arrow built by g++.

## Notes of g++ on Mac OS
* Use the right way to specify compiler
    * Cmake args `CMAKE_C_COMPILER` `DCMAKE_CXX_COMPILER`
        * OK if project has no submodule.
        * Not OK if project has submodule, because cmake args not pass though submodules
    * Env var `CC` `CXX`
        * OK
* Make sure include paths are in right order.
    * The first one must be the g++ include dir
* Flatbuffer bug:
    * Assume using clang when building on Mac OS, and use some clang-only compiler args
