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
    * Build patched ClickHouse:
        * `theflash/ch-connector> ./arrow-build-<your-os>.sh`: build Arrow, run it again if script fail
        * `theflash/ch-connector> ./clickhouse-patch-apply.sh`: apply patch to ClickHouse
        * `theflash/ch-connector> ./clickhouse-build.sh`: build patched ClickHouse
    * Build Spark and CHSpark
        * `theflash/spark-connector/spark> follow the build method of Spark`: build Spark
        * `theflash/spark-connector> ./build-all.sh`: build CHSpark
* Play around
    * With Patched ClickHouse
        * `theflash/benchmark> ./ch-server.sh`: run ClickHouse server
        * `theflash/benchmark> ./ch-cli.sh`: play with ClickHouse in intereactive mode
        * `theflash/benchmark> ./ch-q.sh <sql>`: play with ClickHouse in command mode
        * `theflash/ch-connector> ./clickhouse-import.sh <name>`: import table data `ch-connector/running/data/<name>` to ClickHouse
        * Look around
            * `theflash/ch-connector> vim _env.sh`: check the config, IMPORTANT: `chdb`
            * `theflash/benchmark> vim _env.sh`: each `_env.sh` ONLY affect the current dir
            * `theflash/benchmark> vim /data/ch-server/server.log`: check server log
            * `theflash/benchmark> ls /data/ch-server/db`: check database files
            * `theflash/benchmark> vim ch-server/config/*.xml`: check server configs
    * With Spark
        * `theflash/spark-connector> ./start-all.sh 127.0.0.1`: run Spark master and workers
        * `theflash/spark-connector> ./spark-shell.sh`: play with Spark
        * `theflash/benchmark> ./spark-shell.sh`: play with Spark
    * With Spark connecting to ClickHouse
        * Command mode
            * `theflash/benchmark> ./spark-q.sh <sql>`: can access ClickHouse tables in your sql
        * Intereactive mode
            * `theflash/benchmark> ./spark-shell.sh`: start Spark as usual
            * `scala> val ch = new org.apache.spark.sql.CHContext(spark)`: create ClickHouse context
            * `scala> ch.mapCHClusterTable(database=<>, table=<>)`: map a ClickHouse table to Spark
            * `scala> ch.sql("select count(*) from <>")`: use the mapped table in Spark
* TPCH benchmark
    * Load data
        * `theflash/benchmark/tpch-dbgen> make`: build TPCH dbgen
        * `theflash/benchmark/loading> vim _env.sh`: check the loading config, IMPORTANT: `tpch_scale`
        * `theflash/benchmark/loading> ./load-all.sh`: generate/transform/load data, make sure you have enough disk space
    * Run TPCH benchmark
        * `theflash/benchmark/loading> vim _env.sh`: check the config
        * `theflash/benchmark> ./tpch-spark-q.sh 1`: run TPCH Q1
        * Loop running
            * `theflash/benchmark> ./stable-test-ch-stable.sh`: loop running TPCH Q1-Q22
            * `theflash/benchmark> vim tpch.log`: check log
            * `theflash/benchmark> vim tpch.log.md`: check report


## TODO
Online:
```
[Features]
**--- Fully test (IMPORTANT: types system)
----- Writing transaction(batch level) support
*---- Binlog syncer
*---- MyDump data importer
****- Data update supporting
```
Further Features:
```
----- External executor
----- Share storage
```
