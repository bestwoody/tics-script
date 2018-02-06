# TheFlash TPCH on 4 node, pushdown + codegen + broadcast

## Result

* Time unit: sec
* Run 20 times of each query
    * Discard a few unstable result (cause by network retry)
    * Calculate avg result
* Comparation
    * Parquet: Spark + Parquet
    * CHSpark: Spark + CH, pushdown, codegen, broadcast

| Query | Parquet | CHSpark | Faster |
| ----- | ------: | ------: | :----- |
| Q1    |         |     6.7 |        |
| Q2    |         |    19.9 |        |
| Q3    |         |    52.5 |        |
| Q4    |         |   220.6 |        |
| Q5    |         |   117.5 |        |
| Q6    |         |     4.1 |        |
| Q7    |         |    58.2 |        |
| Q8    |         |   113.6 |        |
| Q9    |         |   176.7 |        |
| Q10   |         |    35.1 |        |
| Q11   |         |    11.8 |        |
| Q12   |         |    15.5 |        |
| Q13   |         |    20.0 |        |
| Q14   |         |     8.5 |        |
| Q16   |         |    45.9 |        |
| Q17   |         |   106.1 |        |
| Q18   |         |    99.8 |        |
| Q19   |         |    16.3 |        |
| Q20   |         |    57.5 |        |
| Q22   |         |    16.3 |        |
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
