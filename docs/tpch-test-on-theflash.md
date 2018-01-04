#TPCH-100G-Q1-Q22
## TPCH compare test
    | Test Type                      | Case    |  Time  1   | Time 2  | 
    | --------                       | -----:  | :----:     | :----:  | 
    | CH-Spark                       | Q1      |   01:44m   | 01:50m  |
    | CH-Spark push down             | Q1      |   00:09m   | 00:10m  |
    | ClickHouse                     | Q1      |   00:04m   | 00:04m  |
    | Spark Parquet                  | Q1      |   01:58m   | 02:06m  |
    | Spark Persist Memory           | Q1      |   01:37m   | 01:35m  |
    | Spark Persist Disk             | Q1      |   01:42m   | 01:44m  |
    | CH-Spark                       | Q2      |   01:34m   | 01:29m  |
    | Spark Parquet                  | Q2      |   0.53m    | 0.54m   |
    | Spark Persist Memory           | Q2      |   01:22m   | 01:26m  |
    | Spark Persist Disk             | Q2      |   01:29m   | 01:28ｍ |
    | CH-Spark                       | Q3      |   01:11m   | 01:12m  |
    | Spark Parquet                  | Q3      |   02:57m   | 02:57m  |
    | Spark Persist Memory           | Q3      |   01:11m   | 01:11m  |
    | Spark Persist Disk             | Q3      |   01:10m   | 01:10m  |
    | CH-Spark                       | Q4      |   05:56m   | 06:05m  |
    | Spark Parquet                  | Q4      |   05:32m   | 05:32m  |
