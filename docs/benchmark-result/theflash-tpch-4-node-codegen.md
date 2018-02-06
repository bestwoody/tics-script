# TheFlash TPCH on 4 node, pushdown + codegen + broadcast

## Result

* Time unit: sec
* Comparation
    * Parquet: Spark + Parquet
    * CHSpark: Spark + CH, pushdown, codegen, broadcast

| Query | Parquet | CHSpark |
| ----- | ------: | ------: |
| Q1    |    38.2 |     6.7 |
| Q2    |    25.0 |    19.9 |
| Q3    |    71.3 |    52.5 |
| Q4    |   326.7 |   220.6 |
| Q5    |   135.4 |   117.5 |
| Q6    |    48.4 |     4.1 |
| Q7    |   130.5 |    58.2 |
| Q8    |   126.3 |   113.6 |
| Q9    |   161.6 |   176.7 |
| Q10   |    45.1 |    35.1 |
| Q11   |    29.7 |    11.8 |
| Q12   |    25.9 |    15.5 |
| Q13   |    23.9 |    20.0 |
| Q14   |    39.3 |     8.5 |
| Q16   |    50.4 |    45.9 |
| Q17   |    83.8 |   106.1 |
| Q18   |   109.7 |    99.8 |
| Q19   |    24.7 |    16.3 |
| Q20   |    84.1 |    57.5 |
| Q22   |    73.3 |    16.3 |
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

### Spark
* SPARK EXECUTOR MEMORY=36G
* SPARK WORKER MEMORY=36G
* SPARK WORKER CORES=24
* SPARK Workers: 4

## TheFlash
* Partitions=8
* Decoders=1
* Encoders=2
* Pushdown=true
* Codegen=true
* Server nodes: 16

### Data Set
* TPCH-100 100G data scala
