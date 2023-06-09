# The ops/ti.sh tool
The ops/ti.sh is designed for fast testing, CI, or POCing, anyone can easily deploy and control a TiDB+TiFlash cluster in no time.


## Quick start
Create a cluster define file `my.ti` by command `ops/ti.sh new my.ti spark=1`, this file will be like:
```
pd: node0/pd
tikv: node0/tikv
tidb: node0/tidb
tiflash: node0/tiflash
spark_m: node0/spark_m
spark_w: node0/spark_w cores=1 mem=1G
```

Then run `ops/ti.sh my.ti run`:
```
=> pd #0 (node0/pd)            <-- "node0/pd" is this pd's running dir
18036                          <-- pid
=> tikv #0 (node0/tikv)
18143
=> tidb #0 (node0/tidb)
18238
=> tiflash #0 (node0/tiflash)
18511
=> spark_m #0 (node0/spark_m)
18278
=> spark_w #0 (node0/spark_w)
18531
```

Now it works, we got a cluster running.
We can use `ops/ti.sh my.ti status`(the word `status` can be omitted) to check cluster status:
```
OK     pd #0 (node0/pd)        <-- "#0" means it's the 1st pd
OK     tikv #0 (node0/tikv)
OK     tidb #0 (node0/tidb)
OK     tiflash #0 (node0/tiflash)
OK     spark_m #0 (node0/spark_m)
OK     spark_w #0 (node0/spark_w)
```

And use `ops/ti.sh my.ti prop` to check cluster config:
```
pd #0 (node0/pd)
    pid: 18036
    pd_name: pd0
    advertise_host: 10.0.0.14
    pd_port: 13579
    peer_port: 13680
    initial_cluster: pd0=http://10.0.0.14:13680
tikv #0 (node0/tikv)
    pid: 18143
    listen_host: 10.0.0.14
    tikv_port: 20431
    advertise_host: 10.0.0.14
    pd_addr: 10.0.0.14:13579
...
```
The `stop` `fstop`(fast/force stop) commands can be used for close the cluster


## One file, one cluster
One `ti` file means one cluster, you can bring up lots of cluster in one computer, without cross interference.
When we define more than one `pd` in a file:
```
pd: node1/pd ports+1
pd: node2/pd ports+2
pd: node3/pd ports+3
```
The `ti.sh` tool will connect them together as a pd-cluster automacally.

When `pd` and other modules in a file:
```
pd: node1/pd ports+1
tikv: node1/tikv ports+1
tidb: node1/tidb ports+1
```
The address of `pd` will be used to other modules for connect.

Although, we can use `pd` prop to connect modules outside this file:
```
tikv: node1/tikv pd=:+1        <-- the pd host is empty, so will be auto set (localhost)
                                   the pd port will be "default+1"
```


## How to edit a `*.ti` file
Take one line for example `pd: node0/pd`
The format is simple: `mod-name: prop prop ...`, there is space chars seperated the mod-name and the props.
```
pd:             <- the module name, we have a small set of modules:
                   pd, tikv, tidb, tiflash
```

The props supported now are:
```
node0/pd        <- the module's dir, can be abs-path or rel-path,
                   dir is the uniq id of a module instance.

ports+1         <- the ports-delta of this module,
ports=n            "+1" means all listening ports will increase 1 (from default value),
                   "-n" is allowed.
                   use default port if this prop is not provided

host=10.0.0.1   <- this module should be deployed to which host.
                   run on local host if this prop is not provided

pd=ip           <- connect to which pd.
pd=ip:port         module will connect to the pd in the same file if not provided.
pd=ip:port         "port" can be real number or "+n" "-n" delta form.

cores=10        <- only used by spark_w module,
                   total cpu cores to allow spark applications to use on the machine
                    (default: all available)

mem=10G         <- only used by spark_w module,
                   total amount of memory to allow spark applications to use on the machine
                   (default: your machine's total RAM minus 1 GB)
 
ver             <- specify binary version.
                    If this property is set, then ti.sh will try to download specify version
                    of binary using tiup mirror.
                    Version format:
                      * "4.0.4" -- download specify version
                      * "4.0.x" -- download the latest release version of branch 4.0

branch & hash   <- specify binary branch & hash.
                    If these two property are set, then ti.sh will try to download specify version
                    of binary using PingCAP internal mirror.
                    * branch should be "master" / "release-x.x"
                    * hash should be "latest" or the commit hash

More about ver, branch & hash
                    Note that you can not set `ver` and `barnch`:`hash` at the same time. If you don't
                    need it, set it to an empty string for the sake of convenience.
                    The priority for finding binary is:
                      * Find by `conf/bin.paths`
                      * Download from tiup mirror if version is not empty
                      * Download from PingCAP internal mirror if branch and hash is not empty
                      * Download from bin.urls / bin.urls.mac
                    Only support for pd/tidb/tikv/tiflash/pd_ctl/tikv_ctl now.

failpoint       <- only used by tidb module,
                    add this if you want to deploy a failpoint-enabled version of tidb.
                    Note that it only take effect when downloading from PingCAP internal mirror
                    (see branch & hash for more details). Otherwise you need to build tidb using
                    `make failpoint-enable && make server`

standalone      <- only used by tiflash module,
                    add this if you want to deploy a standalone tiflash process without pd/tidb/tikv

engine=(dt|tmt) <- only used by tiflash module,
                    set storage engine (DeltaTree or TxnMergeTree) for tiflash process
                    (default: will use "storage_engine" in conf/tiflash/config.toml)
                    (if this property is set, will replace the storage_engine for that tiflash's config)
```

A little more complex case:
```
p1=+1
p2=+2
p3=+3
v=v4.0.4

b=""
hash=""
# or 
# b="release-4.0"
# hash="latest"

pd:      node1/pd      ports{p1} ver={v}
pd:      node2/pd      ports{p2} ver={v}
pd:      node3/pd      ports{p3} ver={v}
tikv:    node1/tikv    ports{p1} ver={v}
tikv:    node2/tikv    ports{p2} ver={v}
tidb:    node1/tidb    ports{p1} ver={v}
tidb:    node2/tidb    ports{p2} ver={v}
tiflash: node1/tiflash ports{p1} ver={v}  branch={b} hash={hash}
tiflash: node2/tiflash ports{p2} ver={v}
spark_m: node1/spark_m ports{p1}
spark_w: node1/spark_w ports{p1} cores=10 mem=10G
spark_w: node2/spark_w ports{p2} cores=10 mem=10G
node_exporter: node1/node_exporter ports{p1}
prometheus: node1/prometheus ports{p1}
grafana: node1/grafana ports{p1}
```

A standalone tiflash process without pd/tidb/tikv:
```
# standalone.ti
dir=nodes/standalone
p=+22
h0=127.0.0.1
tiflash: {dir}/tiflash ports{p} host={h0} standalone engine=dt
```

## Vars supporting
Define a var in `my.ti` like this:
```
root_dir=/data/ti
```

Use a var like this:
```
tidb: {root_dir}/tidb1 ports+1
tidb: {root_dir}/tidb2 ports+2
```
A var's value can be set by defining lines, or pass from `ti.sh` args (`ops/ti.sh ti-file cmd [args(k=v#k=v#..)]`)

## "import" supporting
Edit file `idc.ti`:
```
ip1=172.16.5.81
ip2=172.16.5.82
ip3=172.16.5.85
```

Then import this file:
```
import idc.ti

pd: /data/pd ports+1 host={ip1}
pd: /data/pd ports+2 host={ip2}
pd: /data/pd ports+3 host={ip3}
```

Or, combine `import` with `var`:
```
import {hosts}
```
And pass the specific hosts conf by `ops/ti.sh my.ti run "hosts=idc.ti"`

## Detail configuring
There are 3 things are involved when we using `ops/ti.sh`:
* The tool itself: `ops/ti.sh` and it's supporting scripts, no data stored.
* The conf templates: it stored in a dir.
    * The default dir located at `./conf`
    * You can pass args `conf-templ-dir`(`ops/ti.sh ti-file cmd [args] [conf-templ-dir]`) to use another one.
    * `templates` means they're not the real conf files, the real ones are in module dirs.
    * Take a look at the `default.ports` will helps to avoid port-conflict when we modifys ports using `+n`
    * `conf/bin.paths` and `conf/bin.urls[.mac]` defined where we can get the module bins
* The `my.ti` file and the dirs used by modules in the file, these are the data we care.
    * Once a module is running, it doesn't rely on `conf templates` or `ops/ti.sh` any more.

## `ops/ti.sh` command line help
These commands output responsive help:
```
ops/ti.sh help
ops/ti.sh flags
ops/ti.sh my.ti flags
```

## Why we should use this tool
* It's the easy to use, powerful, and robust, we need those in coding and testing and POC and any casual-use scenario.
* Scripts are flexible, especially we are in the rapid-dev phase.
* The formal tool (ansible) can't be valid (has rich features) in a short time.

## How it plans to go
* The features of one-node-cluster are stable now.
* Working in progress: support multi-node-cluster, remote deployment and other ops.
* Support integration test and benchmark test, maybe POC deployment in this year.
* Step down when it's most features can be covered by ansible (or other tools).

## How to import data as CSV
1. Add tikv_importer to your cluster.
```
tikv_importer: {dir}/importer ports{p}
```
2. Create corresponding database and table for import.
3. Generate data CSVs in a folder and name them as `${db_name}.${tbl_name}.*.csv`
4. Use lightning command to import `lightning/load_csv ${db_name} ${tbl_name} ${path_to_dir} ${check_checksum} ${with_header}`
For example, to import data to ontime.ontime from data folder `/path/table_data/load/splited`.
```
./ops/ti.sh x.ti lightning/load_csv dbname tablename /path/table_data/load/splited false false
```
