# The ops/ti.sh tool
The ops/ti.sh is design for fast testing, CI, or POCing, anyone can easily deploy and control a TiDB+TiFlash cluster in no time.


## Quick start
Edit a file `my.ti` like this(or copy from: `./ti/*.ti`), you can put it anywhere you want:
```
pd: node0/pd
tikv: node0/tikv
tidb: node0/tidb
tiflash: node0/tiflash
rngine: node0/rngine tiflash=node0/tiflash
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
=> rngine #0 (node0/rngine)
18647
```

Now it works, we got a cluster running.
We can use `ops/ti.sh my.ti status`(the word `status` can be omitted) to check cluster status:
```
OK     pd #0 (node0/pd)        <-- "#0" means it's the 1st pd
OK     tikv #0 (node0/tikv)
OK     tidb #0 (node0/tidb)
OK     tiflash #0 (node0/tiflash)
OK     rngine #0 (node0/rngine)
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
ghe address of `pd` will be used to other modules for connect.

Although, we can use `pd` prop (and `tiflash` prop in rngine) to connect modules outside this file:
```
tikv: node1/tikv pd=:+1        <-- the pd host is empty, so will be auto set (localhost)
                                   the pd port will be "default+1"
```


## How to edit a `*.ti` file
Take one line for example `pd: node0/pd`
The format is simple: `mod-name: prop prop ...`, there is space chars seperated the mod-name and the props.
```
pd:             <- the module name, we have a small set of modules:
                   pd, tikv, tidb, tiflash, rngine
```

The props supported now are:
```
node0/pd        <- the module's dir, can be abs-path or rel-path,
                   dir is the uniq id of a module instance.

ports+1         <- the ports-delta of this module,
ports=n            "+1" means all listening ports will increase 1 (from default value),
                   "-n" is allowed.
                   use default port if this prop is not provided

host=10.0.0.1   <- this module should be deployed to which host. (WIP)
                   run on local host if this prop is not provided

pd=ip           <- connect to which pd.
pd=ip:port         module will connect to the pd in the same file if not provided.
pd=ip:port         "port" can be real number or "+n" "-n" delta form.

tiflash=dir     <- only used by rngine module,
tiflash=ip:port    it means which tiflash instance rngine should connect,
tiflash=ip:dir    "tiflash=host:dir" is used for support remote connect.
                   (WIP)
```

A little more complex case:
```
pd: node1/pd ports+1
pd: node2/pd ports+2
pd: node3/pd ports+3
tikv: node1/tikv ports+1
tikv: node2/tikv ports+2
tidb: node1/tidb ports+1
tidb: node2/tidb ports+2
tiflash: node1/tiflash ports+1
tiflash: node2/tiflash ports+2
rngine: node1/rngine tiflash=node1/tiflash ports+1
rngine: node2/rngine tiflash=node2/tiflash ports+2
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
    * `templates` means they'er not the real conf files, the real ones are in module dirs.
    * Take a look at the `default.ports` will helps to avoid port-conflict when we modifys ports using `+n`
    * `conf/bin.paths` and `conf/bin.urls` defined where we can get the module bins
* The `my.ti` file and the dirs used by modules in the file, these are the data we care.
    * Once a module is running, it doesn't rely on `conf templates` or `ops/ti.sh` any more.

## `ops/ti.sh` command line help
```
ops/ti [-c conf_templ_dir] [-s cmd =_dir] [-t cache_dir] [-k ti_file_kvs] [-m pd|tikv|..]
       [-h host,host] [-b] ti_file_path cmd(run|stop|fstop|status|..) [args]
    -c:
        specify the config template dir, will be `ops/../conf` if this arg is not provided.
    -s:
        specify the sub-comand dir, will be `ops/ti.sh.cmds` if this arg is not provided.
    -t:
        specify the cache dir for download bins and other things in all hosts.
        will be `/tmp/ti` if this arg is not provided.
    -k:
        specify the key-value(s) string, will be used as vars in the .ti file, format: k=v#k=v#..
    -m:
        the module name, could be one of pd|tikv|tidb|tiflash|rngine.
        and could be multi modules like: `pd,tikv`.
        if this arg is not provided, it means all modules.
    -h:
        the host names, format: `host,host,..`
        if this arg is not provided, it means all specified host names in the .ti file.
    -b:
        execute command on each host(node).
        if this arg is not provided, execute command on each module.
    cmd:
        could be one of run|stop|fstop|status.
        (`up` and `down` are aliases of `run` and `stop`)
        and could be one of `ops/ti.sh.cmds/<command>.sh`
        (Could be one of `ops/ti.sh.cmds/byhost/<command>.sh` if `-b`)
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
