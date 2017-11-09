# Build projects on Mac OS

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
    * git clone arrow, build with g++
    * Apply patch we made for CH: `ch-connector> ./delta-apply.sh`
    * Build CH-Connector: `ch-connector> ./libch-build.sh`
* Reasons
    * CH require g++ as compiler, also require all libs compiled with g++.
    * So CH build(from source) and link all libs as static libs.
    * We alos need arrow built by g++.
