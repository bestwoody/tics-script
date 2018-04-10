# Fio on Ceph benchmark report


## Deployment
* Each node:
    * 16 Cores
    * 16G RAM
    * 300G SSD, 2.0G/s
    * ping `<` 0.5ms between each two nodes (avg: 0.35ms)
* h0: cephfs client, mounted by ceph-fuse
* h1-h3: ceph cluster
    * 1 `mon` (3 will be better, but can't be faster), 3 `mgr`, 3 `ods`
    * 300G raw disk for each `ods`


## Fio randread
```
fio -filename={} -direct=1 -rw=randread -size 4G -numjobs={jobs} -bs={bs} -runtime=10 -group_reporting -name=pc
```
* When jobs`<=`2, the first time always slower, after that, can be 15% faster (as below)
| Args             |  MB/s |
| ---------------- | ----: |
| jobs= 1, bs=  4K |     4 |
| jobs= 1, bs=  8K |     9 |
| jobs= 1, bs= 16K |    16 |
| jobs= 1, bs= 32K |    31 |
| jobs= 1, bs= 64K |    52 |
| jobs= 1, bs=128K |    77 |
| jobs= 1, bs=256K |    78 |
| jobs= 1, bs=512K |    87 |
| jobs= 1, bs=  1M |    90 |
| jobs= 1, bs=  2M |    94 |
| jobs= 1, bs=  4M |    91 |
| jobs= 1, bs= 32M |    90 |
| jobs= 2, bs=512K |   137 |
| jobs= 2, bs=  1M |   141 |
| jobs= 4, bs=  1M |   204 |
| jobs= 8, bs=  1M |   258 |
| jobs=16, bs=  1M |   282 |


## Fio randwrite
```
fio -filename={} -direct=1 -rw=randwrite -size 4G -numjobs={jobs} -bs={bs} -runtime=10 -group_reporting -name=pc
```
| Args             |  MB/s |
| ---------------- | ----: |
| jobs= 1, bs=  4K |   1.5 |
| jobs= 1, bs=  8K |   2.7 |
| jobs= 1, bs= 16K |     5 |
| jobs= 1, bs= 32K |     9 |
| jobs= 1, bs= 64K |    16 |
| jobs= 1, bs=128K |    30 |
| jobs= 1, bs=256K |    31 |
| jobs= 1, bs=512K |    32 |
| jobs= 1, bs=  1M |    32 |
| jobs= 1, bs=  2M |    32 |
| jobs= 1, bs=  4M |    32 |
| jobs= 1, bs= 32M |    31 |
| jobs= 2, bs=512K |    32 |
| jobs= 2, bs=  1M |    33 |
| jobs= 4, bs=  1M |    31 |
| jobs= 8, bs=  1M |    31 |
| jobs=16, bs=  1M |    32 |
