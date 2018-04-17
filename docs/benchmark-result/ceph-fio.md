# Fio on Ceph benchmark report

## Deployment
* Slow group
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
* Fast group
    * Each node:
        * 40 Cores
        * 128G RAM
        * 3.6T SSD, 3G+/s
    * Network, between each two nodes:
        * Ping `<` 0.1ms, avg: 0.09ms
        * IO Throughput `~=` 1.1GB/s
    * h0: cephfs client
        * Mounted by ceph-fuse
    * h1-h3: ceph cluster
        * 1 `mon` (3 will be better, but can't be faster), 3 `mgr`, 3 `ods`
        * 3.6T raw disk for each `ods`


## Fio randread
```
fio -filename={} -direct=1 -rw=randread -size 4G -numjobs={jobs} -bs={bs} -runtime=10 -group_reporting -name=pc
```
| Jobs | Block | Slow Group(MB/s) | Fast Group(MB/s) |
| ---: | ----: | ---------------: | ---------------: |
|    1 |    4K |                4 |                6 |
|    1 |    8K |                9 |               11 |
|    1 |   16K |               16 |               22 |
|    1 |   32K |               31 |               41 |
|    1 |   64K |               52 |               73 |
|    1 |  128K |               77 |              134 |
|    1 |  256K |               78 |              142 |
|    1 |  512K |               87 |              147 |
|    1 |    1M |               90 |              146 |
|    1 |    2M |               94 |              147 |
|    1 |    4M |               91 |              150 |
|    1 |   32M |               90 |              144 |
|    2 |  512K |              137 |              317 |
|    2 |    1M |              141 |              313 |
|    4 |    1M |              204 |              673 |
|    8 |    1M |              258 |             1056 |
|   16 |    1M |              282 |             1113 |
|   16 |    4k |                  |               53 |
|   16 |    4k |                  |              111 |
|   16 |   16k |                  |              218 |
|   16 |   32k |                  |              432 |
|   16 |   64k |                  |              914 |
|   16 |  128k |                  |             1114 |


## Fio randwrite
```
fio -filename={} -direct=1 -rw=randwrite -size 4G -numjobs={jobs} -bs={bs} -runtime=10 -group_reporting -name=pc
```
| Jobs | Block | Slow Group(MB/s) | Fast Group(MB/s) |
| ---: | ----: | ---------------: | ---------------: |
|    1 |    4K |              1.5 |              1.6 |
|    1 |    8K |              2.7 |              3.2 |
|    1 |   16K |                5 |                6 |
|    1 |   32K |                9 |               13 |
|    1 |   64K |               16 |               26 |
|    1 |  128K |               30 |               49 |
|    1 |  256K |               31 |               49 |
|    1 |  512K |               32 |               49 |
|    1 |    1M |               32 |               49 |
|    1 |    2M |               32 |               49 |
|    1 |    4M |               32 |               50 |
|    1 |   32M |               31 |               51 |
|    2 |  512K |               32 |               50 |
|    2 |    1M |               33 |               50 |
|    4 |    1M |               31 |               50 |
|    8 |    1M |               31 |               50 |
|   16 |    1M |               32 |               51 |
