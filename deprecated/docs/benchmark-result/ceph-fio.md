# Fio on Ceph benchmark report

## Deployment
* Each node:
    * 40 Cores
    * 128G RAM
    * 3.6T SSD, 3G+/s
* Network, between each two nodes:
    * Ping `<` 0.1ms, avg: 0.09ms
    * IO Throughput `~=` 1.1GB/s
* h0: cephfs client
    * Mounted by `mount` (not ceph-fuse)
* h1-h3: ceph cluster
    * 1 `mon` (3 will be better, but can't be faster), 3 `mgr`, 3 `ods`
    * 1 fold in 3.6T disk for each `ods`, using bluestore


## Fio result
```
fio -filename={} -direct=1 -rw={operation} -size 4G -numjobs={jobs} -bs={bs} -runtime=10 -group_reporting -name=pc
```
| Jobs | Block |  randread | randwrite |  seq read |  seq write |
| ---: | ----: | --------: | --------: | --------: | ---------: |
|    1 |    4K |   12 MB/s |    2 MB/s |   18 MB/s |     2 MB/s |
|    1 |    8K |   23 MB/s |    4 MB/s |   36 MB/s |     5 MB/s |
|    1 |   16K |   40 MB/s |    8 MB/s |   51 MB/s |     9 MB/s |
|    1 |   32K |   70 MB/s |   16 MB/s |   84 MB/s |    16 MB/s |
|    1 |   64K |  124 MB/s |   31 MB/s |  146 MB/s |    32 MB/s |
|    1 |  128K |  210 MB/s |   59 MB/s |  248 MB/s |    60 MB/s |
|    1 |  256K |  324 MB/s |  102 MB/s |  325 MB/s |   103 MB/s |
|    1 |  512K |  471 MB/s |  151 MB/s |  410 MB/s |   152 MB/s |
|    1 |    1M |  621 MB/s |  195 MB/s |  530 MB/s |   196 MB/s |
|    1 |    2M |  726 MB/s |  225 MB/s |  652 MB/s |   222 MB/s |
|    1 |    4M |  802 MB/s |  241 MB/s |  744 MB/s |   240 MB/s |
|    1 |   32M |  722 MB/s |  239 MB/s |  743 MB/s |   242 MB/s |
|    2 |  512K |  845 MB/s |  293 MB/s |  748 MB/s |   285 MB/s |
|    2 |    1M |  989 MB/s |  364 MB/s | 1004 MB/s |   362 MB/s |
|    4 |    1M | 1120 MB/s |  590 MB/s | 1112 MB/s |   539 MB/s |
|    8 |    1M | 1121 MB/s |  891 MB/s | 1118 MB/s |   735 MB/s |
|   16 |    1M | 1122 MB/s | 1055 MB/s | 1117 MB/s |   693 MB/s |
|   16 |    4k |  156 MB/s |   34 MB/s |   80 MB/s |    17 MB/s |
|   16 |    4k |  165 MB/s |   38 MB/s |   90 MB/s |    18 MB/s |
|   16 |   16k |  533 MB/s |  149 MB/s |  313 MB/s |    59 MB/s |
|   16 |   32k |  877 MB/s |  207 MB/s |  518 MB/s |   123 MB/s |
|   16 |   64k | 1101 MB/s |  413 MB/s |  862 MB/s |   285 MB/s |
|   16 |  128k | 1115 MB/s |  667 MB/s | 1039 MB/s |   573 MB/s |
