# TiFlash
An OLAP engine of TiDB

## Quick start guide
Go to `tiflash/integrated` directory:
```sh
cd ${tiflash_root_dir}/integrated
```

Create a cluster define file:
```sh
ops/ti.sh new my.ti tidb=1 tikv=1 pd=1 tiflash=1
```

Then you will get a `my.ti` file in current directory with content as follow:
```
h0=127.0.0.1

dir=my_cluster

delta=0

pd: {dir}/pd ports+{delta} host={h0}

tikv: {dir}/tikv ports+{delta} host={h0}

tidb: {dir}/tidb ports+{delta} host={h0}

tiflash: {dir}/tiflash ports+{delta} host={h0}
```

You can change the deploy path of cluster by modifying the `dir`.

If you want to deploy a cluster that can be accessed by other machines, then you should change the `host` to a public IP address.

(Deploy and) Launch a cluster defined by `my.ti`:
```sh
ops/ti.sh my.ti up
=> pd #0 (my_cluster/pd)
25239
=> tikv #0 (my_cluster/tikv)
25429
=> tidb #0 (my_cluster/tidb)
25581
=> tiflash #0 (my_cluster/tiflash)
25961
```

Check the cluster status:
```
ops/ti.sh my.ti status
OK     pd #0 (my_cluster/pd)
OK     tikv #0 (my_cluster/tikv)
OK     tidb #0 (my_cluster/tidb)
OK     tiflash #0 (my_cluster/tiflash)
```

Check the cluster config:
```sh
ops/ti.sh my.ti prop
```

You can specify version of the components by adding a `ver` property, which follows TiUP version rules. For example, we'd like to bump version of all components to `v5.0.1`, we can modify `my.ti` as follows:
```
h0=127.0.0.1

dir=my_cluster

delta=0

v=v5.0.1

pd: {dir}/pd ports+{delta} host={h0} ver={v}

tikv: {dir}/tikv ports+{delta} host={h0} ver={v}

tidb: {dir}/tidb ports+{delta} host={h0} ver={v}

tiflash: {dir}/tiflash ports+{delta} host={h0} ver={v}

```

## Advanced feature

### Replace some binaries in a cluster

`ops/ti.sh` will try to use binaries provided in `integrated/conf/bin.paths` to deploy the cluster, you can replace the server binary(e.g. `tidb-server`, `tiflash`) with your own binary by editing `integrated/conf/bin.paths`.

Once `ops/ti.sh` is starting a `stopped` service, it will replace the server binary and then run service with it.

Here's an example to replace `tiflash` binary in a `running` cluster:

1. `cd integrated`
2. Stop the `tiflash` service with `ops/ti.sh -m tiflash my.ti stop`
3. Edit `conf/bin.paths`, replace the corresponding binary paths with yours:
   ```
   # name \t bin_name \t path1:path2:...
   tiflash	tiflash	/home/you/path_to_tics/build/dbms/src/Server/tiflash
   tiflash_proxy libtiflash_proxy.so /home/you/path_to_tics/libs/libtiflash-proxy/libtiflash_proxy.so
   #tikv	tikv-server	[path_to_tikv_binary]
   #pd	pd-server	[path_to_pd_binary]
   #tidb	tidb-server	[path_to_tidb_binary]
   ```
4. Restart `tiflash` service with `ops/ti.sh -m tiflash my.ti up`

Then you will get your own tiflash binary running.

### TPCH tools

Load tpch data into the cluster (scale = 0.01, tables = all):
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
```sh
ops/ti.sh my.ti mysql "select count(*) from tpch_0_01.lineitem"
count(*)
60175
```

* Execute a query directly on TiFlash Storage:
```sh
ops/ti.sh my.ti ch "select count(*) from tpch_0_01.lineitem"
count()
60175
```

* Execute a query from Spark to TiFlash Storage:
```sh
ops/ti.sh my.ti beeline "select count(*) from tpch_0_01.lineitem"
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
