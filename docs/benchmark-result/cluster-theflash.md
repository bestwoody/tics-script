# TPCH-Test cluster
## TPCH  test  : 4 Spark Workers vs 16 ClickHouse 
* time unit minute

| Test Type             | Case | Time  1 | Time 2 | Time 3 |
| --------------------- | :--- | :------ | :----- | :----- |
| CH-Spark              | Q1   | 00:41   | 00:33  | 00:39  |
| CH-Spark Optimization | Q1   | 00:11   | 00:07  |        |
| CH-Spark push down    | Q1   | 00:07   | 00:07  |        |
| Spark Codegen         | Q1   | 00:18   | 00:19  |        |
| Spark Persist Memory  | Q1   |         |        |        |
| CH-Spark              | Q2   | 01:02   | 00:57  | 00:45  |
| CH-Spark Optimization | Q2   | 00:51   | 00:46  |        |
| CH-Spark push down    | Q2   | 00:46   | 00:44  |        |
| Spark Codegen         | Q2   | 00:45   | 00:45  |        |
| Spark Persist Memory  | Q2   |         |        |        |
| CH-Spark              | Q3   | 00:40   | 00:44  | 00:54  |
| CH-Spark Optimization | Q3   | 00:54   | 00:48  |        |
| CH-Spark push down    | Q3   | 00:50   | 00:45  |        |
| Spark Codegen         | Q3   | 00:43   | 00:31  |        |
| Spark Persist Memory  | Q3   |         |        |        |
| CH-Spark              | Q4   |         |        |        |
| CH-Spark Optimization | Q4   |         |        |        |
| CH-Spark push down    | Q4   |         |        |        |
| Spark Codegen         | Q4   |         |        |        |
| Spark Persist Memory  | Q4   |         |        |        |
| CH-Spark              | Q5   | 02:04   | 01:51  | 01:50  |
| CH-Spark Optimization | Q5   | 03:05   | 01:55  |        |
| CH-Spark push down    | Q5   |         |        |        |
| Spark Codegen         | Q5   |         |        |        |
| Spark Persist Memory  | Q5   |         |        |        |
| CH-Spark              | Q6   | 00:05   | 00:04  | 00:04  |
| CH-Spark Optimization | Q6   | 00:10   | 00:04  |        |
| CH-Spark push down    | Q6   |         |        |        |
| Spark Codegen         | Q6   |         |        |        |
| Spark Persist Memory  | Q6   |         |        |        |
| CH-Spark              | Q7   |         |        |        |
| CH-Spark Optimization | Q7   |         |        |        |
| CH-Spark push down    | Q7   |         |        |        |
| Spark Codegen         | Q7   |         |        |        |
| Spark Persist Memory  | Q7   |         |        |        |
| CH-Spark              | Q8   | 01:57   | 02:10  |        |
| CH-Spark Optimization | Q8   | 02:49   | 02:01  |        |
| CH-Spark push down    | Q8   |         |        |        |
| Spark Codegen         | Q8   |         |        |        |
| Spark Persist Memory  | Q8   |         |        |        |
| CH-Spark              | Q9   | 02:21   | 02:53  |        |
| CH-Spark Optimization | Q9   | 02:55   | 02:41  |        |
| CH-Spark push down    | Q9   |         |        |        |
| Spark Codegen         | Q9   |         |        |        |
| Spark Persist Memory  | Q9   |         |        |        |
| CH-Spark              | Q10  | 00:38   | 00:41  |        |
| CH-Spark Optimization | Q10  | 00:40   | 00:38  |        |
| CH-Spark push down    | Q10  |         |        |        |
| Spark Codegen         | Q10  |         |        |        |
| Spark Persist Memory  | Q10  |         |        |        |
| CH-Spark              | Q11  | 00:36   | 00:38  |        |
| CH-Spark Optimization | Q11  | 00:35   | 00:35  |        |
| CH-Spark push down    | Q11  |         |        |        |
| Spark Codegen         | Q11  |         |        |        |
| Spark Persist Memory  | Q11  |         |        |        |
| CH-Spark              | Q12  | 00:42   | 00:49  |        |
| CH-Spark Optimization | Q12  | 00:52   | 00:17  |        |
| CH-Spark push down    | Q12  |         |        |        |
| Spark Codegen         | Q12  |         |        |        |
| Spark Persist Memory  | Q12  |         |        |        |
| CH-Spark              | Q13  | 00:37   | 00:46  |        |
| CH-Spark Optimization | Q13  | 00:39   | 00:26  |        |
| CH-Spark push down    | Q13  |         |        |        |
| Spark Codegen         | Q13  |         |        |        |
| Spark Persist Memory  | Q13  |         |        |        |
| CH-Spark              | Q14  | 00:09   | 00:09  |        |
| CH-Spark Optimization | Q14  | 00:08   | 00:09  |        |
| CH-Spark push down    | Q14  |         |        |        |
| Spark Codegen         | Q14  |         |        |        |
| Spark Persist Memory  | Q14  |         |        |        |
| CH-Spark              | Q16  | 01:26   | 01:23  |        |
| CH-Spark Optimization | Q16  | 00:39   | 00:46  |        |
| CH-Spark push down    | Q16  |         |        |        |
| Spark Codegen         | Q16  |         |        |        |
| Spark Persist Memory  | Q16  |         |        |        |
| CH-Spark              | Q17  |         |        |        |
| CH-Spark Optimization | Q17  |         |        |        |
| CH-Spark push down    | Q17  |         |        |        |
| Spark Codegen         | Q17  |         |        |        |
| Spark Persist Memory  | Q17  |         |        |        |
| CH-Spark              | Q18  |         |        |        |
| CH-Spark Optimization | Q18  |         |        |        |
| CH-Spark push down    | Q18  |         |        |        |
| Spark Codegen         | Q18  |         |        |        |
| Spark Persist Memory  | Q18  |         |        |        |
| CH-Spark              | Q19  | 00:25   | 00:27  |        |
| CH-Spark Optimization | Q19  | 00:28   | 00:16  |        |
| CH-Spark push down    | Q19  |         |        |        |
| Spark Codegen         | Q19  |         |        |        |
| Spark Persist Memory  | Q19  |         |        |        |
| CH-Spark              | Q20  | 01:04   | 01:09  |        |
| CH-Spark Optimization | Q20  | 00:55   | 00:52  |        |
| CH-Spark push down    | Q20  |         |        |        |
| Spark Codegen         | Q20  |         |        |        |
| Spark Persist Memory  | Q20  |         |        |        |
| CH-Spark              | Q21  |         |        |        |
| CH-Spark Optimization | Q21  |         |        |        |
| CH-Spark push down    | Q21  |         |        |        |
| Spark Codegen         | Q21  |         |        |        |
| Spark Persist Memory  | Q21  |         |        |        |
| CH-Spark              | Q22  |         |        |        |
| CH-Spark Optimization | Q22  |         |        |        |
| CH-Spark push down    | Q22  |         |        |        |
| Spark Codegen         | Q22  |         |        |        |
| Spark Persist Memory  | Q22  |         |        |        |

# Testing environment
## Hardware Config
* 4 nodes.
* Intel Xeon(R) CPU E5-2630 v3 @ 2.40GHz 8 cores * 2
* 128G Memory 1600MHz .
* HDD 16O.0MB+/s.

## Software Configuration
* CentOS Linux release 7.2.1511
* Spark version 2.1.0
* ClickHouse server version 1.1.54310

## Spark Config
* SPARK EXECUTOR MEMORY=36G
* SPARK WORKER MEMORY=36G
* SPARK WORKER CORES=24
* SPARK Workers: 4

## ClickHouse Config
* Server nodes: 16

# Data scale
* TPCH-100 100G data scala

