# TheFlash TPCH on 1 node, pushdown + codegen + broadcast

## Result

* Time unit: sec
* Run 20 times of each query
    * Discard a few unstable result (cause by network retry)
    * Calculate avg result
* Comparation
    * Parquet: Spark + Parquet
    * CHSpark: Spark + CH, pushdown, codegen, broadcast

| Query    | Parquet | CHSpark | Faster |
| -------- | ------: | ------: | :----- |
| Q1       |  122.0  |    9.6  | ====== |
| Q2       |   53.5  |   44.6  | ==     |
| Q3       |  177.0  |   74.1  | =====  |
| Q4       |  332.0  |  358.4  | SLOWER |
| Q5       |  227.5  |  124.1  | ===    |
| Q6       |    3.5  |    4.5  |        |
| Q7       |  305.0  |   92.6  | =====  |
| Q8       |  159.0  |  129.3  | =      |
| Q9       |  156.5  |  180.0  | SLOWER |
| Q10      |   69.0  |   62.0  | =      |
| Q11      |   32.5  |   19.6  | ====   |
| Q12      |   45.0  |   39.9  | =      |
| Q13      |   78.0  |   56.8  | ==     |
| Q14      |  206.0  |   13.9  | ====== |
| Q16      |  151.5  |  138.4  | =      |
| Q19      |   36.5  |   54.8  | SLOWER |
| Q20      |  267.0  |         |        |
| Q22      |   72.5  |         |        |

Note:
Q4 was retested by Xiaoyu at the same time for both cases twice. CH version is faster for 10-15%.

## Environment

### Hardware
* Intel Xeon E312xx 16 cores 2.4Ghz.
* 16G Memory 1600MHz .
* SSD 2.0GB+/s.

### Software
* CentOS Linux release 7.2.1511
* Spark version 2.1.1
* ClickHouse server version 1.1.54310

### Spark
* SPARK EXECUTOR MEMORY=12G
* SPARK WORKER MEMORY=12G
* SPARK WORKER CORES=16

## TheFlash
* Partitions=16
* Decoders=2
* Encoders=8
* Pushdown=true
* Codegen=true

### Data Set
* TPCH-100 100G data scala
