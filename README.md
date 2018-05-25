# The Flash
An OLAP project of TiDB

## Design
* [The big picture](./docs/the-big-picture.md)
* [The CH-Spark solution](./docs/ch-spark-tcp.md)
* [Speed compare to parquet](./docs/benchmark-result/theflash-tpch-1-node.md)
* [More docs](./docs)


## Quick start guide
* Build
    * `theflash> git submodule update --init --recursive`: fetch all submodule
    * Build patched CH:
        * `theflash/ch-connector> ./patch-apply.sh`: apply patch to CH
        * `theflash/ch-connector> ./build.sh`: build patched CH
    * Build Spark and CHSpark
        * `theflash/spark-connector/spark> follow the build method of Spark`: build Spark
        * `theflash/spark-connector> ./build.sh`: build CHSpark
* Play around
    * With Patched CH
        * `theflash/benchmark> ./storage-server.sh`: run CH server
        * `theflash/benchmark> ./storage-client.sh`: play with CH in intereactive mode
        * `theflash/benchmark> ./storage-client.sh <sql>`: play with CH in command mode
        * `theflash/ch-connector> ./storage-import-data.sh <name>`: import table data `ch-connector/running/data/<name>` to CH
        * Look around
            * `theflash/ch-connector> vim _env.sh`: check the config, IMPORTANT: `storage_db`
            * `theflash/benchmark> vim _env.sh`: each `_env.sh` ONLY affect the current dir
            * `theflash/benchmark> vim /data/theflash/server.log`: check server log
            * `theflash/benchmark> ls /data/theflash/db`: check database files
            * `theflash/benchmark> vim storage-server/config/*.xml`: check server configs
    * With Spark
        * `theflash/spark-connector> ./spark-start-all.sh 127.0.0.1`: run Spark master and workers
        * `theflash/spark-connector> ./spark-shell.sh`: play with Spark
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
        * `theflash/benchmark/loading> vim _env.sh`: check the loading config, IMPORTANT: `tpch_scale`
        * `theflash/benchmark/loading> ./load-all.sh`: generate - transform - load data, make sure you have enough disk space
    * Run TPCH benchmark
        * `theflash/benchmark/loading> vim _env.sh`: check the config
        * `theflash/benchmark> ./tpch-spark-q.sh 1`: run TPCH Q1
        * Loop running
            * `theflash/benchmark> ./stable-test-ch-stable.sh`: loop running TPCH Q1-Q22
            * `theflash/benchmark> vim tpch.log`: check log
            * `theflash/benchmark> vim tpch.log.md`: check report
* Deployment
    * Binary packing for development
        * `theflash/deployment/publish> ./publish.sh`: packing binary in a compiling server
