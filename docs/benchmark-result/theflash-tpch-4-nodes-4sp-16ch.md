# TheFlash TPCH on 4 node, pushdown + codegen + broadcast

## Result

* Time unit: sec
* Comparation
    * Parquet: Spark + Parquet
    * CHSpark: Spark + CH, pushdown, codegen, broadcast

| Query | Parquet | CHSpark |
| ----- | ------: | ------: |
| Q1    |    39.6 |     6.7 |
| Q2    |    25.9 |    19.9 |
| Q3    |    80.4 |    52.5 |
| Q4    |   278.7 |   220.6 |
| Q5    |   132.8 |   117.5 |
| Q6    |    51.2 |     4.1 |
| Q7    |   121.2 |    58.2 |
| Q8    |   117.9 |   113.6 |
| Q9    |   150.5 |   176.7 |
| Q10   |    49.1 |    35.1 |
| Q11   |    24.6 |    11.8 |
| Q12   |    22.9 |    15.5 |
| Q13   |    24.7 |    20.0 |
| Q14   |    49.4 |     8.5 |
| Q16   |    54.2 |    45.9 |
| Q17   |    99.2 |   106.1 |
| Q18   |   125.8 |    99.8 |
| Q19   |    17.7 |    16.3 |
| Q20   |    84.8 |    57.5 |
| Q22   |    78.3 |    16.3 |

## Environment

### Hardware
* 4 nodes.
* Intel Xeon(R) CPU E5-2630 v3 @ 2.40GHz 8 cores * 2
    * TODO: check cores
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
    * TODO: 16 encoders will be faster
* Pushdown=true
* Codegen=true
* Server nodes: 16

### Data Set
* TPCH-100 100G data scala
