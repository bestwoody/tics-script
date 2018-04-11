# Fio on Ceph benchmark report


## Deployment
* Each node:
    * 16 Cores
    * 16G RAM
    * 300G SSD, 2.0G/s
* Network, between each two nodes:
    * Ping `<` 0.5ms, avg: 0.35ms
    * IO Throughput `>` 500MB/s
* h0: cephfs client
    * Mounted by ceph-fuse
* h1-h3: ceph cluster
    * 1 `mon` (3 will be better, but can't be faster), 3 `mgr`, 3 `ods`
    * 300G raw disk for each `ods`


## Fio randread
```
fio -filename={} -direct=1 -rw=randread -size 4G -numjobs={jobs} -bs={bs} -runtime=10 -group_reporting -name=pc
```

* When jobs`<=`2, the first time always slower, after that, can be 15% faster (as below)

| Jobs | Block |  MB/s |
| ---: | ----: | ----: |
|    1 |    4K |     4 |
|    1 |    8K |     9 |
|    1 |   16K |    16 |
|    1 |   32K |    31 |
|    1 |   64K |    52 |
|    1 |  128K |    77 |
|    1 |  256K |    78 |
|    1 |  512K |    87 |
|    1 |    1M |    90 |
|    1 |    2M |    94 |
|    1 |    4M |    91 |
|    1 |   32M |    90 |
|    2 |  512K |   137 |
|    2 |    1M |   141 |
|    4 |    1M |   204 |
|    8 |    1M |   258 |
|   16 |    1M |   282 |


## Fio randwrite
```
fio -filename={} -direct=1 -rw=randwrite -size 4G -numjobs={jobs} -bs={bs} -runtime=10 -group_reporting -name=pc
```
| Jobs | Block |  MB/s |
| ---: | ----: | ----: |
|    1 |    4K |   1.5 |
|    1 |    8K |   2.7 |
|    1 |   16K |     5 |
|    1 |   32K |     9 |
|    1 |   64K |    16 |
|    1 |  128K |    30 |
|    1 |  256K |    31 |
|    1 |  512K |    32 |
|    1 |    1M |    32 |
|    1 |    2M |    32 |
|    1 |    4M |    32 |
|    1 |   32M |    31 |
|    2 |  512K |    32 |
|    2 |    1M |    33 |
|    4 |    1M |    31 |
|    8 |    1M |    31 |
|   16 |    1M |    32 |
