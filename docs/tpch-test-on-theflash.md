# TPCH-Test
## TPCH compare test 
* time unit minute

| Test Type                      | Case    |  Time  1  | Time 2 | 
| --------                       | -----:  |   ----:   | :----: | 
| CH-Spark                       | Q1      |   01:44   | 01:50  |
| CH-Spark push down             | Q1      |   00:09   | 00:10  |
| ClickHouse                     | Q1      |   00:04   | 00:04  |
| Spark Parquet                  | Q1      |   01:58   | 02:06  |
| Spark Persist Memory           | Q1      |   01:37   | 01:35  |
| Spark Persist Disk             | Q1      |   01:42   | 01:44  |
| CH-Spark                       | Q2      |   01:34   | 01:29  |
| Spark Parquet                  | Q2      |   00.53   | 00.54  |
| Spark Persist Memory           | Q2      |   01:22   | 01:26  |
| Spark Persist Disk             | Q2      |   01:29   | 01:28  |
| CH-Spark                       | Q3      |   01:11   | 01:12  |
| Spark Parquet                  | Q3      |   02:57   | 02:57  |
| Spark Persist Memory           | Q3      |   01:11   | 01:11  |
| Spark Persist Disk             | Q3      |   01:10   | 01:10  |
| CH-Spark                       | Q4      |   05:56   | 06:05  |
| Spark Parquet                  | Q4      |   05:32   | 05:32  |
| Spark Persist Memory           | Q4      |   05:52   | 05:38  |
| Spark Persist Disk             | Q4      |   05:32   | 05:28  |
| CH-Spark                       | Q5      |   03:14   | 02:57  |
| Spark Parquet                  | Q5      |   03:45   | 03:50  |
| Spark Persist Memory           | Q5      |   02:53   | 02:48  |
| Spark Persist Disk             | Q5      |   02:46   | 02:55  |
| CH-Spark                       | Q6      |   00:04   | 00:04  |
| Spark Parquet                  | Q6      |   00:03   | 00:04  |
| Spark Persist Memory           | Q6      |   00:02   | 00:02  |
| Spark Persist Disk             | Q6      |   00:02   | 00:02  |
| CH-Spark                       | Q7      |   02:49   | 02:59  |
| Spark Parquet                  | Q7      |   04:54   | 05:16  |
| Spark Persist Memory           | Q7      |   02:51   | 03:09  |
| Spark Persist Disk             | Q7      |   03:06   | 02:45  |
| CH-Spark                       | Q8      |   02:18   | 02:21  |
| Spark Parquet                  | Q8      |   02:37   | 02:41  |
| Spark Persist Memory           | Q8      |   02:22   | 02:18  |
| Spark Persist Disk             | Q8      |   02:19   | 02:21  |
| CH-Spark                       | Q9      |   03:43   | 03:23  |
| Spark Parquet                  | Q9      |   02:33   | 02:40  |
| Spark Persist Memory           | Q9      |   03:23   | 03:13  |
| Spark Persist Disk             | Q9      |   03:55   | 03:27  |
| CH-Spark                       | Q10     |   01:09   | 01:08  |
| Spark Parquet                  | Q10     |   01:09   | 01:09  |
| Spark Persist Memory           | Q10     |   01:10   | 01:12  |
| Spark Persist Disk             | Q10     |   01:01   | 01:03  |
| CH-Spark                       | Q11     |   00:56   | 01:03  |
| Spark Parquet                  | Q11     |   00:33   | 00:32  |
| Spark Persist Memory           | Q11     |   00:54   | 00:59  |
| Spark Persist Disk             | Q11     |   00:59   | 00:53  |
| CH-Spark                       | Q12     |   00:41   | 00:46  |
| Spark Parquet                  | Q12     |   00:44   | 00:46  |
| Spark Persist Memory           | Q12     |   00:40   | 00:39  |
| Spark Persist Disk             | Q12     |   00:55   | 00:49  |
| CH-Spark                       | Q13     |   01:19   | 01:20  |
| Spark Parquet                  | Q13     |   01:19   | 01:17  |
| Spark Persist Memory           | Q13     |   01:14   | 01:13  |
| Spark Persist Disk             | Q13     |   01:17   | 01:20  |
| CH-Spark                       | Q14     |   00:14   | 00:13  |
| Spark Parquet                  | Q14     |   03:27   | 03:15  |
| Spark Persist Memory           | Q14     |   00:11   | 00:12  |
| Spark Persist Disk             | Q14     |   00:12   | 00:13  |
| CH-Spark                       | Q16     |   02:06   | 02:09  |
| Spark Parquet                  | Q16     |   02:33   | 02:30  |
| Spark Persist Memory           | Q16     |   02:12   | 02:06  |
| Spark Persist Disk             | Q16     |   02:52   | 02:02  |
| CH-Spark                       | Q19     |   00:53   | 00:56  |
| Spark Parquet                  | Q19     |   00:30   | 00:41  |
| Spark Persist Memory           | Q19     |   00:53   | 01:06  |
| Spark Persist Disk             | Q19     |   00:50   | 00:59  |
| CH-Spark                       | Q20     |   01:41   | 01:43  |
| Spark Parquet                  | Q20     |   04:31   | 04:23  |
| Spark Persist Memory           | Q20     |   01:41   | 01:24  |
| Spark Persist Disk             | Q20     |   01:33   | 01:48  |
| CH-Spark                       | Q22     |   02:02   | 01:44  |
| Spark Parquet                  | Q22     |   01:14   | 01:11  |
| Spark Persist Memory           | Q22     |   04:33   | 04:06  |
| Spark Persist Disk             | Q22     |   04:21   | 04:07  |

# Testing environment
## Hardware Config
* Intel Xeon E312xx 16 cores 2.4Ghz.
* 16G Memory 1600MHz .
* SSD 2.0GB+/s.

## Software Configuration
* CentOS Linux release 7.2.1511
* Spark version 2.1.1
* ClickHouse server version 1.1.54310

## Spark Config
* SPARK EXECUTOR MEMORY=12G
* SPARK WORKER MEMORY=12G
* SPARK WORKER CORES=16

# Data scale
## TPCH-100 100G data scala
