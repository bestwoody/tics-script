# TPCH-Test cluster 4 nodes
## TPCH compare test 
* time unit minute

| Test Type            | Case | Time  1 | Time 2 |
| -------------------- | :--- | :------ | :----- |
| CH-Spark             | Q1   | 02:23   | 02:24  |
| CH-Spark push down   | Q1   | 02:21   | 02:20  |
| Spark Parquet        | Q1   | 00:46   | 00:40  |
| Spark Persist Memory | Q1   | 01:09   | 01:04  |
| Spark Persist Disk   | Q1   | 01:11   | 01:02  |
| CH-Spark             | Q2   | 01:13   | 01:13  |
| Spark Parquet        | Q2   | 00:27   | 00:30  |
| Spark Persist Memory | Q2   | 00:58   | 01:10  |
| Spark Persist Disk   | Q2   | 01:10   | 01:02  |
| CH-Spark             | Q3   | 02:14   | 02:14  |
| Spark Parquet        | Q3   | 01:08   | 01:10  |
| Spark Persist Memory | Q3   | 01:33   | 01:17  |
| Spark Persist Disk   | Q3   | 01:44   | 02:18  |
| CH-Spark             | Q4   | 10:28   | 10:28  |
| Spark Parquet        | Q4   | 06:02   | 04:44  |
| Spark Persist Memory | Q4   | 10:06   | 11:19  |
| Spark Persist Disk   | Q4   | 09:44   | 11:30  |
| CH-Spark             | Q5   | 06:17   | 06:17  |
| Spark Parquet        | Q5   | 01:55   | 01:17  |
| Spark Persist Memory | Q5   | 03:39   | 03:55  |
| Spark Persist Disk   | Q5   | 02:56   | 03:05  |
| CH-Spark             | Q6   | 00:16   | 00:16  |
| Spark Parquet        | Q6   | 00:47   | 00:49  |
| Spark Persist Memory | Q6   | 00:06   | 00:09  |
| Spark Persist Disk   | Q6   | 00:08   | 00:02  |
| CH-Spark             | Q7   | 03:19   | 02:54  |
| Spark Parquet        | Q7   | 01:57   | 01:33  |
| Spark Persist Memory | Q7   | 01:54   | 02:48  |
| Spark Persist Disk   | Q7   | 03:07   | 02:31  |
| CH-Spark             | Q8   | 16:23   | 17:23  |
| Spark Parquet        | Q8   | 02:09   | 01:55  |
| Spark Persist Memory | Q8   | 05:27   | 05:37  |
| Spark Persist Disk   | Q8   | 07:24   | 07:14  |
| CH-Spark             | Q9   | 05:35   | 06:03  |
| Spark Parquet        | Q9   | 02:29   | 02:23  |
| Spark Persist Memory | Q9   | 05:44   | 06:15  |
| Spark Persist Disk   | Q9   | 04:20   | 04:26  |
| CH-Spark             | Q10  | 01:49   | 01:17  |
| Spark Parquet        | Q10  | 00:41   | 00:41  |
| Spark Persist Memory | Q10  | 01:02   | 01:03  |
| Spark Persist Disk   | Q10  | 01:07   | 01:03  |
| CH-Spark             | Q11  | 01:12   | 00:40  |
| Spark Parquet        | Q11  | 00:23   | 00:25  |
| Spark Persist Memory | Q11  | 00:34   | 00:36  |
| Spark Persist Disk   | Q11  | 00:41   | 00:34  |
| CH-Spark             | Q12  | 02:50   | 01:50  |
| Spark Parquet        | Q12  | 01:40   | 01:55  |
| Spark Persist Memory | Q12  | 01:42   | 01:31  |
| Spark Persist Disk   | Q12  | 01:40   | 01:38  |
| CH-Spark             | Q13  | 04:31   | 02:31  |
| Spark Parquet        | Q13  | 00:29   | 00:34  |
| Spark Persist Memory | Q13  | 02:49   | 01:56  |
| Spark Persist Disk   | Q13  | 01:19   | 01:25  |
| CH-Spark             | Q14  | 00:13   | 00:11  |
| Spark Parquet        | Q14  | 00:53   | 00:48  |
| Spark Persist Memory | Q14  | 00:08   | 00:09  |
| Spark Persist Disk   | Q14  | 00:10   | 00:08  |
| CH-Spark             | Q16  | 02:12   | 01:58  |
| Spark Parquet        | Q16  | 01:28   | 01:09  |
| Spark Persist Memory | Q16  | 02:03   | 02:15  |
| Spark Persist Disk   | Q16  | 03:05   | 02:10  |
| CH-Spark             | Q17  | 01:51   | 01:41  |
| Spark Parquet        | Q17  | 02:09   | 01:44  |
| Spark Persist Memory | Q17  | 01:59   | 01:41  |
| Spark Persist Disk   | Q17  | 06:44   | 01:48  |
| CH-Spark             | Q18  | 04:45   | 04:21  |
| Spark Parquet        | Q18  | 01:59   | 01:36  |
| Spark Persist Memory | Q18  | 04:50   | 04:14  |
| Spark Persist Disk   | Q18  | 04:38   | 04:55  |
| CH-Spark             | Q19  | 01:20   | 01:01  |
| Spark Parquet        | Q19  | 00:21   | 00:21  |
| Spark Persist Memory | Q19  | 02:17   | 02:39  |
| Spark Persist Disk   | Q19  | 02:39   | 01:58  |
| CH-Spark             | Q20  |         |        |
| Spark Parquet        | Q20  | 01:27   | 01:12  |
| Spark Persist Memory | Q20  | 02:29   | 02:26  |
| Spark Persist Disk   | Q20  | 02:39   | 04:30  |
| CH-Spark             | Q22  | 06:09   |        |
| Spark Parquet        | Q22  | 01:32   | 01:14  |
| Spark Persist Memory | Q22  | 03:46   | 03:48  |
| Spark Persist Disk   | Q22  | 03:57   | 04:03  |
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
* Server nodes: 1

# Data scale
* TPCH-100 100G data scala
