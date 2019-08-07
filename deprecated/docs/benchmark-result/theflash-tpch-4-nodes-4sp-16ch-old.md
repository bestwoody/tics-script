# TPCH-Test cluster

## TPCH  test  : 4 Spark Workers vs 16 CH
| Test Type             | Case | Time  1 | Time 2 | Time 3 |
| --------------------- | :--- | :------ | :----- | :----- |
| CH-Spark              | Q1   | 00:41   | 00:33  | 00:39  |
| CH-Spark Optimization | Q1   | 00:11   | 00:07  | 00:15  |
| CH-Spark push down    | Q1   | 00:07   | 00:07  | 00:07  |
| Spark Codegen         | Q1   | 00:18   | 00:19  | 00:16  |
| Spark Persist Memory  | Q1   |         |        |        |
| CH-Spark              | Q2   | 01:02   | 00:57  | 00:45  |
| CH-Spark Optimization | Q2   | 00:51   | 00:46  | 00:43  |
| CH-Spark push down    | Q2   | 00:46   | 00:44  | 00:59  |
| Spark Codegen         | Q2   | 00:45   | 00:45  | 00:44  |
| Spark Persist Memory  | Q2   |         |        |        |
| CH-Spark              | Q3   | 00:40   | 00:44  | 00:54  |
| CH-Spark Optimization | Q3   | 00:54   | 00:48  | 00:50  |
| CH-Spark push down    | Q3   | 00:50   | 00:45  | 00:48  |
| Spark Codegen         | Q3   | 00:43   | 00:31  | 00:46  |
| Spark Persist Memory  | Q3   |         |        |        |
| CH-Spark              | Q4   | 03:19   | 03:55  | 03:05  |
| CH-Spark Optimization | Q4   | 08:08   | 04:05  | 03:27  |
| CH-Spark push down    | Q4   | 05:30   | 05:32  | 04:58  |
| Spark Codegen         | Q4   | 05:02   | 03:27  | 03:39  |
| Spark Persist Memory  | Q4   |         |        |        |
| CH-Spark              | Q5   | 02:04   | 01:51  | 01:50  |
| CH-Spark Optimization | Q5   | 02:15   | 01:55  | 01:47  |
| CH-Spark push down    | Q5   | 02:01   | 02:07  | 01:55  |
| Spark Codegen         | Q5   | 02:04   | 01:49  | 01:43  |
| Spark Persist Memory  | Q5   |         |        |        |
| CH-Spark              | Q6   | 00:05   | 00:04  | 00:04  |
| CH-Spark Optimization | Q6   | 00:10   | 00:04  | 00:04  |
| CH-Spark push down    | Q6   | 00:05   | 00:04  | 00:04  |
| Spark Codegen         | Q6   | 00:04   | 00:04  | 00:04  |
| Spark Persist Memory  | Q6   |         |        |        |
| CH-Spark              | Q7   | 01:51   | 01:43  | 01:57  |
| CH-Spark Optimization | Q7   | 02:20   | 02:14  | 02:13  |
| CH-Spark push down    | Q7   | 01:46   | 01:43  | 01:12  |
| Spark Codegen         | Q7   | 01:56   | 01:58  | 01:55  |
| Spark Persist Memory  | Q7   |         |        |        |
| CH-Spark              | Q8   | 01:57   | 02:10  | 01:50  |
| CH-Spark Optimization | Q8   | 02:49   | 02:01  | 02:06  |
| CH-Spark push down    | Q8   | 02:02   | 02:36  | 01:54  |
| Spark Codegen         | Q8   | 02:04   | 02:24  | 01:54  |
| Spark Persist Memory  | Q8   |         |        |        |
| CH-Spark              | Q9   | 02:21   | 02:53  | 02:31  |
| CH-Spark Optimization | Q9   | 02:55   | 02:41  | 02:34  |
| CH-Spark push down    | Q9   | 03:00   | 02:26  | 02:29  |
| Spark Codegen         | Q9   | 02:10   | 02:11  | 02:12  |
| Spark Persist Memory  | Q9   |         |        |        |
| CH-Spark              | Q10  | 00:38   | 00:41  | 00:40  |
| CH-Spark Optimization | Q10  | 00:40   | 00:38  | 00:37  |
| CH-Spark push down    | Q10  | 00:41   | 00:37  | 00:37  |
| Spark Codegen         | Q10  | 00:30   | 00:30  | 00:31  |
| Spark Persist Memory  | Q10  |         |        |        |
| CH-Spark              | Q11  | 00:36   | 00:38  | 00:35  |
| CH-Spark Optimization | Q11  | 00:35   | 00:35  | 00:30  |
| CH-Spark push down    | Q11  | 00:35   | 00:32  | 00:34  |
| Spark Codegen         | Q11  | 00:39   | 00:36  | 00:30  |
| Spark Persist Memory  | Q11  |         |        |        |
| CH-Spark              | Q12  | 00:42   | 00:49  | 00:43  |
| CH-Spark Optimization | Q12  | 00:35   | 00:17  | 00:26  |
| CH-Spark push down    | Q12  | 00:37   | 00:45  | 00:39  |
| Spark Codegen         | Q12  | 00:27   | 00:39  | 00:30  |
| Spark Persist Memory  | Q12  |         |        |        |
| CH-Spark              | Q13  | 00:37   | 00:46  | 00:37  |
| CH-Spark Optimization | Q13  | 00:39   | 00:26  | 00:30  |
| CH-Spark push down    | Q13  | 00:30   | 00:38  | 00:35  |
| Spark Codegen         | Q13  | 00:23   | 00:30  | 00:26  |
| Spark Persist Memory  | Q13  |         |        |        |
| CH-Spark              | Q14  | 00:09   | 00:09  | 00:09  |
| CH-Spark Optimization | Q14  | 00:08   | 00:09  | 00:08  |
| CH-Spark push down    | Q14  | 00:09   | 00:09  | 00:10  |
| Spark Codegen         | Q14  | 00:08   | 00:08  | 00:09  |
| Spark Persist Memory  | Q14  |         |        |        |
| CH-Spark              | Q16  | 01:26   | 01:23  | 00:51  |
| CH-Spark Optimization | Q16  | 00:39   | 00:46  | 00:55  |
| CH-Spark push down    | Q16  | 01:39   | 01:05  | 01:22  |
| Spark Codegen         | Q16  | 01:04   | 01:14  | 01:01  |
| Spark Persist Memory  | Q16  |         |        |        |
| CH-Spark              | Q17  | 01:13   | 01:16  | 01:19  |
| CH-Spark Optimization | Q17  | 01:46   | 02:47  | 02:47  |
| CH-Spark push down    | Q17  | 02:29   | 01:26  | 02:47  |
| Spark Codegen         | Q17  | 01:28   | 01:17  | 01:23  |
| Spark Persist Memory  | Q17  |         |        |        |
| CH-Spark              | Q18  | 03:19   | 01:23  | 01:39  |
| CH-Spark Optimization | Q18  | 03:24   | 03:10  | 02:36  |
| CH-Spark push down    | Q18  | 04:48   | 02:14  | 02:20  |
| Spark Codegen         | Q18  | 01:50   | 01:28  | 01:48  |
| Spark Persist Memory  | Q18  |         |        |        |
| CH-Spark              | Q19  | 00:25   | 00:27  | 00:29  |
| CH-Spark Optimization | Q19  | 00:28   | 00:16  | 00:17  |
| CH-Spark push down    | Q19  | 00:27   | 00:28  | 00:27  |
| Spark Codegen         | Q19  | 00:15   | 00:16  | 00:17  |
| Spark Persist Memory  | Q19  |         |        |        |
| CH-Spark              | Q20  | 01:04   | 01:09  | 00:56  |
| CH-Spark Optimization | Q20  | 00:55   | 00:52  | 00:48  |
| CH-Spark push down    | Q20  | 01:08   | 01:09  | 01:04  |
| Spark Codegen         | Q20  | 00:55   | 00:53  | 00:55  |
| Spark Persist Memory  | Q20  |         |        |        |
| CH-Spark              | Q21  |         |        |        |
| CH-Spark Optimization | Q21  |         |        |        |
| CH-Spark push down    | Q21  |         |        |        |
| Spark Codegen         | Q21  |         |        |        |
| Spark Persist Memory  | Q21  |         |        |        |
| CH-Spark              | Q22  | 01:16   | 01:12  | 01:17  |
| CH-Spark Optimization | Q22  | 01:13   | 01:10  | 01:06  |
| CH-Spark push down    | Q22  | 01:46   | 01:43  | 01:12  |
| Spark Codegen         | Q22  | 01:03   | 01:13  | 01:05  |
| Spark Persist Memory  | Q22  |         |        |        |

# Testing environment
## Hardware Config
* 4 nodes.
* Intel Xeon(R) CPU E5-2630 v3 @ 2.40GHz 8 cores * 2
    * TODO: check cores
* 128G Memory 1600MHz .
* HDD 16O.0MB+/s.

## Software Configuration
* CentOS Linux release 7.2.1511
* Spark version 2.1.0
* CH server version 1.1.54310

## Spark Config
* SPARK EXECUTOR MEMORY=36G
* SPARK WORKER MEMORY=36G
* SPARK WORKER CORES=24
* SPARK Workers: 4

## CH Config
* Server nodes: 16

# Data scale
* TPCH-100 100G data scala
