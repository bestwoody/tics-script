# TheFlash TPCH on 4 node, pushdown + codegen + broadcast

## Result

* Time unit: sec
* Comparation
    * Parquet: Spark + Parquet
    * CHSpark: Spark + CH, pushdown, codegen, broadcast

| Query | Parquet | CHSpark |
| ----- | ------: | ------: |
| Q1    |    41.6 |     6.7 |
| Q2    |    25.9 |    19.9 |
| Q3    |    78.5 |    52.5 |
| Q4    |   534.1 |   220.6 |
| Q5    |   152.7 |   117.5 |
| Q6    |    52.2 |     4.1 |
| Q7    |   123.0 |    58.2 |
| Q8    |   148.5 |   113.6 |
| Q9    |   178.2 |   176.7 |
| Q10   |    46.9 |    35.1 |
| Q11   |    27.4 |    11.8 |
| Q12   |    23.5 |    15.5 |
| Q13   |    21.7 |    20.0 |
| Q14   |    42.4 |     8.5 |
| Q16   |    47.1 |    45.9 |
| Q17   |   112.7 |   106.1 |
| Q18   |   145.9 |    99.8 |
| Q19   |    16.6 |    16.3 |
| Q20   |    87.5 |    57.5 |
| Q22   |   104.2 |    16.3 |
## Environment

### Hardware
* 4 nodes.
* Intel Xeon(R) CPU E5-2630 v3 @ 2.40GHz 8 cores * 2
* 128G Memory 1600MHz .
* HDD 16O.0MB+/s.

### Software
* CentOS Linux release 7.2.1511
* Spark version 2.1.1
* ClickHouse server version 1.1.54310
* Hadoop version 2.7.5

### Spark
* SPARK EXECUTOR MEMORY=36G
* SPARK WORKER MEMORY=36G
* SPARK WORKER CORES=24
* SPARK Workers: 4

### HDFS
* DataNode: 4
* Storage node : 4 * 4 HDD

## TheFlash
* Partitions=8
* Decoders=1
* Encoders=2
* Pushdown=true
* Codegen=true
* Server nodes: 16

### Data Set
* TPCH-100 100G data scala
