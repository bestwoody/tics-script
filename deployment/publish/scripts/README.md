# TheFlash published package usage guide

## IMPORTANT: this package is built for developing test, dot NOT use in producting environment

## Run storage
* `here> ./storage-server.sh`: run storage server
* `here> ./storage-client.sh`: connect to storage in intereactive mode
* `here> ./storage-client.sh <sql>`: connect to storage in command mode

## Run Spark
* `[here]> ./spark-start-all.sh 127.0.0.1`: run Spark master and workers
* `[here]> ./spark-q.sh <sql>`: can access storage tables in `<sql>`
* Intereactive mode
    * `[here]> ./spark-shell.sh`: into Spark intereactive mode
    * `[here]> val storage = new org.apache.spark.sql.CHContext(spark)`: create storage context
    * `[here]> storage.mapCHClusterTable(database=<database>, table=<table>)`: map a storage table to Spark
    * `[here]> storage.sql("select count(*) from <table>")`: use the mapped table in Spark

## Data and log path. IMPORTANT: data in these paths may lost on system reboot!
* `/tmp/theflash/db`: default database path
* `/tmp/theflash/server.log`: default storage log
* `/tmp/theflash/spark`: default Spark data path

## TPCH benchmark
* Load data
    * `[here]/tpch/load> vim _env.sh`: check the loading config, IMPORTANT: `tpch_scale`
    * `[here]/tpch/load> ./load-all.sh`: generate and transform and load data, make sure you have enough disk space
* Run TPCH benchmark
    * `[here]> ./tpch-spark-q.sh 1`: run TPCH Q1
    * Loop running
        * `[here]> ./tpch-stable-test.sh`: loop running TPCH Q1-Q22
        * `[here]> vim tpch.log`: check Spark shell log
        * `[here]> vim tpch.log.md`: check report: elapsed time, etc

## Configuring
* `[here]/_env.sh`: runtime vars, include storage server, client and Spark's
* `[here]/storage/config.xml + users.xml`: storage configs
* `[here]/tpch/load/_env.sh`: tpch data loading configs
* `[here]/spark/conf/*`: Spark configs
