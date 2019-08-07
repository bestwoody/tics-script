# The Flash
An OLAP project of TiDB

## Design Docs
* [Docs on confluence](https://internal.pingcap.net/confluence/pages/viewpage.action?pageId=14451924)
* [Earlier docs](./deprecated/docs)


## Quick start guide
* Build
    * `theflash> git submodule update --init --recursive`: fetch all submodule
    * Build CH:
        * `theflash/storage> ./build.sh`: build CH (use `./build_clang.sh` if it's on Mac)
    * Build Spark and CHSpark
        * `theflash/computing/spark> follow the build method of Spark`: build Spark
        * `theflash/computing> ./build.sh`: build CHSpark
* Play around
    * With Patched CH
        * `theflash/benchmark> ./storage-server-start.sh`: run CH server
        * `theflash/benchmark> ./storage-client.sh`: play with CH in intereactive mode
        * `theflash/benchmark> ./storage-client.sh <sql>`: play with CH in command mode
        * `theflash/storage> ./storage-import-data.sh <name>`: import table data `storage/running/data/<name>` to CH
        * Look around
            * `theflash/storage> vim _env.sh`: check the config, IMPORTANT: `storage_db`
            * `theflash/benchmark> vim _env.sh`: each `_env.sh` ONLY affect the current dir
            * `theflash/benchmark> vim /data/theflash/server.log`: check server log
            * `theflash/benchmark> ls /data/theflash/db`: check database files
            * `theflash/benchmark> vim storage-server/config/*.xml`: check server configs
    * With Spark
        * `theflash/computing> ./spark-start-all.sh 127.0.0.1`: run Spark master and workers
        * `theflash/computing> ./spark-shell.sh`: play with Spark
        * `theflash/benchmark> ./spark-shell.sh`: play with Spark
    * With Spark connecting to CH
        * Command mode
            * `theflash/benchmark> ./spark-q.sh <sql>`: can access CH tables in your sql
        * Intereactive mode
            * `theflash/benchmark> ./spark-shell.sh`: start Spark as usual
            * `scala> val ch = new org.apache.spark.sql.CHContext(spark)`: create CH context
            * `scala> ch.mapCHClusterTable(database=<database>, table=<table>)`: map a CH table to Spark
            * `scala> ch.sql("select count(*) from <table>")`: use the mapped table in Spark
* TPCH benchmark
    * Load data
        * `theflash/benchmark/tpch-dbgen> make`: build TPCH dbgen
        * `theflash/benchmark/tpch-load> vim _env.sh`: check the loading config, IMPORTANT: `tpch_scale`
        * `theflash/benchmark/tpch-load> ./load-all.sh`: generate - transform - load data, make sure you have enough disk space
    * Run TPCH benchmark
        * `theflash/benchmark/tpch-load> vim _env.sh`: check the config
        * `theflash/benchmark> ./tpch-spark-q.sh 1`: run TPCH Q1
        * Loop running
            * `theflash/benchmark> ./stable-test-ch-stable.sh`: loop running TPCH Q1-Q22
            * `theflash/benchmark> vim tpch.log`: check log
            * `theflash/benchmark> vim tpch.log.md`: check report
* Deployment
    * Binary packing for development
        * `theflash/deployment/publish> ./publish.sh`: packing binary in a compiling server
