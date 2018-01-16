# TPCH-Test cluster 4 nodes
## TPCH compare test 
* time unit minute

| Test Type                      | Case    |  Time  1  | Time 2 | 
| --------                       | -----:  |   ----:   | :----: | 
| CH-Spark                       | Q1      |   02:33   | 02:33  |
| CH-Spark push down             | Q1      |   00:00   | 00:00  |
| Spark Parquet                  | Q1      |   01:00   | 00:00  |
| Spark Persist Memory           | Q1      |   00:09   | 00:00  |
| Spark Persist Disk             | Q1      |   01:11   | 01:01  |
| CH-Spark                       | Q2      |   01:13   | 01:13  |
| Spark Parquet                  | Q2      |   01:16   | 00:00  |
| Spark Persist Memory           | Q2      |   00:58   | 00:00  |
| Spark Persist Disk             | Q2      |   01:10   | 01:02  |
| CH-Spark                       | Q3      |   02:14   | 02:14  |
| Spark Parquet                  | Q3      |   01:42   | 00:00  |
| Spark Persist Memory           | Q3      |   01:33   | 00:00  |
| Spark Persist Disk             | Q3      |   01:44   | 02:18  |
| CH-Spark                       | Q4      |   10:28   | 10:28  |
| Spark Parquet                  | Q4      |   15:23   | 00:00  |
| Spark Persist Memory           | Q4      |   00:00   | 00:00  |
| Spark Persist Disk             | Q4      |   44:29   | 11:30  |
| CH-Spark                       | Q5      |   06:17   | 06:17  |
| Spark Parquet                  | Q5      |   05:06   | 00:00  |
| Spark Persist Memory           | Q5      |   03:39   | 00:00  |
| Spark Persist Disk             | Q5      |   02:56   | 00:00  |
| CH-Spark                       | Q6      |   00:16   | 00:16  |
| Spark Parquet                  | Q6      |   00:04   | 00:00  |
| Spark Persist Memory           | Q6      |   00:06   | 00:00  |
| Spark Persist Disk             | Q6      |   00:08   | 00:02  |
| CH-Spark                       | Q7      |   03:19   | 02:54  |
| Spark Parquet                  | Q7      |   02:35   | 00:00  |
| Spark Persist Memory           | Q7      |   01:54   | 00:00  |
| Spark Persist Disk             | Q7      |   03:07   | 02:31  |
| CH-Spark                       | Q8      |   16:23   | 17:23  |
| Spark Parquet                  | Q8      |   04:51   | 00:00  |
| Spark Persist Memory           | Q8      |   05:27   | 00:00  |
| Spark Persist Disk             | Q8      |   07:24   | 07:14  |
| CH-Spark                       | Q9      |   05:35   | 06:03  |
| Spark Parquet                  | Q9      |   07:09   | 00:00  |
| Spark Persist Memory           | Q9      |   05:44   | 00:00  |
| Spark Persist Disk             | Q9      |   04:20   | 00:00  |
| CH-Spark                       | Q10     |   01:49   | 01:17  |
| Spark Parquet                  | Q10     |   01:15   | 00:00  |
| Spark Persist Memory           | Q10     |   01:02   | 00:00  |
| Spark Persist Disk             | Q10     |   01:07   | 01:03  |
| CH-Spark                       | Q11     |   01:12   | 00:40  |
| Spark Parquet                  | Q11     |   00:49   | 00:00  |
| Spark Persist Memory           | Q11     |   00:00   | 00:00  |
| Spark Persist Disk             | Q11     |   00:41   | 00:34  |
| CH-Spark                       | Q12     |   02:50   | 01:50  |
| Spark Parquet                  | Q12     |   01:40   | 00:00  |
| Spark Persist Memory           | Q12     |   01:42   | 00:00  |
| Spark Persist Disk             | Q12     |   01:40   | 01:38  |
| CH-Spark                       | Q13     |   04:31   | 02:31  |
| Spark Parquet                  | Q13     |   01:30   | 00:00  |
| Spark Persist Memory           | Q13     |   02:49   | 00:00  |
| Spark Persist Disk             | Q13     |   01:19   | 01:25  |
| CH-Spark                       | Q14     |   00:13   | 00:11  |
| Spark Parquet                  | Q14     |   00:11   | 00:00  |
| Spark Persist Memory           | Q14     |   00:08   | 00:09  |
| Spark Persist Disk             | Q14     |   00:10   | 00:08  |
| CH-Spark                       | Q16     |   02:12   | 01:58  |
| Spark Parquet                  | Q16     |   02:16   | 00:00  |
| Spark Persist Memory           | Q16     |   02:03   | 00:00  |
| Spark Persist Disk             | Q16     |   03:05   | 02:10  |
| CH-Spark                       | Q17     |   01:51   | 00:00  |
| Spark Parquet                  | Q17     |   03:00   | 00:00  |
| Spark Persist Memory           | Q17     |   01:59   | 00:00  |
| Spark Persist Disk             | Q17     |   06:44   | 01:48  |
| CH-Spark                       | Q18     |   04:45   | 00:00  |
| Spark Parquet                  | Q18     |   05:05   | 00:00  |
| Spark Persist Memory           | Q18     |   04:50   | 00:00  |
| Spark Persist Disk             | Q18     |   04:38   | 00:00  |
| CH-Spark                       | Q19     |   01:20   | 00:00  |
| Spark Parquet                  | Q19     |   01:22   | 00:00  |
| Spark Persist Memory           | Q19     |   02:17   | 00:00  |
| Spark Persist Disk             | Q19     |   02:39   | 01:58  |
| CH-Spark                       | Q20     |   00:00   | 00:00  |
| Spark Parquet                  | Q20     |   03:37   | 00:00  |
| Spark Persist Memory           | Q20     |   02:29   | 00:00  |
| Spark Persist Disk             | Q20     |   02:39   | 04:30  |
| CH-Spark                       | Q21     |   00:00   | 00:00  |
| Spark Parquet                  | Q21     |   00:00   | 00:00  |
| Spark Persist Memory           | Q21     |   00:00   | 00:00  |
| Spark Persist Disk             | Q21     |   00:00   | 00:00  |
| CH-Spark                       | Q22     |   06:09   | 00:00  |
| Spark Parquet                  | Q22     |   00:00   | 00:00  |
| Spark Persist Memory           | Q22     |   03:46   | 05:01  |
| Spark Persist Disk             | Q22     |   00:00   | 00:00  |
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
## TPCH-100 100G data scala
