# The Flash
An OLAP project of TiDB

## Quick start guide
* Create a cluster define file:
```
tiflash/integrated> ti.sh new my.ti spark=1
```

* (Deploy and) Launch a cluster defined by `my.ti`:
```
tiflash/integrated> ops/ti.sh my.ti up
=> pd #0 (nodes/4/pd)
25239
=> tikv #0 (nodes/4/tikv)
25429
=> tidb #0 (nodes/4/tidb)
25581
=> tiflash #0 (nodes/4/tiflash)
25961
=> rngine #0 (nodes/4/rngine)
26180
=> spark_m #0 (nodes/4/spark_m)
26535
=> spark_w #0 (nodes/4/spark_w)
27057
```

* Check the cluster status:
```
tiflash/integrated> ops/ti.sh my.ti
OK     pd #0 (nodes/4/pd)
OK     tikv #0 (nodes/4/tikv)
OK     tidb #0 (nodes/4/tidb)
OK     tiflash #0 (nodes/4/tiflash)
OK     rngine #0 (nodes/4/rngine)
OK     spark_m #0 (nodes/4/spark_m)
OK     spark_w #0 (nodes/4/spark_w)
```

* Load tpch data into the cluster (scale = 0.01, tables = all):
```
tiflash/integrated> ops/ti.sh my.ti tpch/load 0.01 all
=> loading customer
   loaded
=> loading nation
   loaded
=> loading orders
   loaded
=> loading part
   loaded
=> loading region
   loaded
=> loading supplier
   loaded
=> loading partsupp
   loaded
=> loading lineitem
   loaded
```

* Execute a query on TiDB:
```
tiflash/integrated> ops/ti.sh my.ti mysql "select count(*) from tpch_0_01.lineitem"
count(*)
60175
```

* Execute a query directly on TiFlash Storage:
```
tiflash/integrated> ops/ti.sh my.ti ch "select count(*) from tpch_0_01.lineitem"
count()
60175
```

* Execute a query from Spark to TiFlash Storage:
```
tiflash/integrated> ops/ti.sh my.ti beeline "tpch_0_01" -e "select count(*) from lineitem"
Connecting to jdbc:hive2://127.0.0.1:10004
+-----------+
| count(1)  |
+-----------+
| 60175     |
+-----------+
1 row selected (2.399 seconds)
Closing: 0: jdbc:hive2://127.0.0.1:10004
```

### More usage
* The cluster managing tool: ops/ti.sh
    * `tiflash/integrated> ops/ti.sh`: run it without args to get help and the cmd set
    * `tiflash/integrated> ops/ti.sh foo.ti some-cmd`: run a cmd without args to get help
    * [Doc](./integrated/docs/ti.sh.md)

### How To Build binaries
* `tiflash> git submodule update --init --recursive`: fetch all submodule
* Build TiFlash storage module:
    * `tiflash/storage> ./build.sh`: build CH (use `./build_clang.sh` if it's on Mac)
* Build CHSpark, a TiFlash connector for Spark:
    * `tiflash/computing> ./build.sh`: build CHSpark
* Put the built result into usage:
    * Put binary's path into `tiflash/integrated/conf/bin.paths`
        * TiFlash storage module built result: `tiflash/storage/build/dbms/src/Server/tiflash`
        * CHSpark built result: `tiflash/chspark/target/chspark-*.jar`
    * Launch cluster as above.

## Docs
* [Design docs on confluence](https://internal.pingcap.net/confluence/pages/viewpage.action?pageId=14451924)
* [Earlier docs](./deprecated/docs)
