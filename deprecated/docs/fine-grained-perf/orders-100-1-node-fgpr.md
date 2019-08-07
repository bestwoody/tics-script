# Fine grained perf report for table orders
* Time unit: sec.
## Single column
### Column O_ORDERKEY
* Type: Int32
* Count: 150000000
* Min: 1
* Max: 600000000

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY from orders | 4.147 | 4.551 | 2.1 |
| selraw count(O_ORDERKEY) from orders | 0.058 | 1.007 | 1.0 |
| selraw min(O_ORDERKEY) from orders | 0.092 | 0.986 | 0.6 |
| selraw max(O_ORDERKEY) from orders | 0.098 | 0.990 | 0.7 |
| selraw sum(O_ORDERKEY) from orders | 0.085 | 1.027 | 0.7 |
| selraw O_ORDERKEY from orders where O_ORDERKEY >= 1 and O_ORDERKEY <= 600000000 | 4.475 | 4.739 | 1.9 |
| selraw O_ORDERKEY from orders where O_ORDERKEY < 1 or O_ORDERKEY > 600000000 | 0.006 | 0.011 | 0.2 |
| selraw O_ORDERKEY + 1 > 10000 from orders | 2.167 | 2.336 | 1.7 |

### Column O_CUSTKEY
* Type: Int32
* Count: 150000000
* Min: 1
* Max: 14999999

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_CUSTKEY from orders | 4.728 | 4.998 | 1.5 |
| selraw count(O_CUSTKEY) from orders | 0.049 | 1.255 | 0.4 |
| selraw min(O_CUSTKEY) from orders | 0.084 | 1.357 | 0.4 |
| selraw max(O_CUSTKEY) from orders | 0.082 | 1.273 | 0.4 |
| selraw sum(O_CUSTKEY) from orders | 0.072 | 1.245 | 0.5 |
| selraw O_CUSTKEY from orders where O_CUSTKEY >= 1 and O_CUSTKEY <= 14999999 | 4.851 | 4.915 | 1.5 |
| selraw O_CUSTKEY from orders where O_CUSTKEY < 1 or O_CUSTKEY > 14999999 | 0.082 | 0.083 | 0.1 |
| selraw O_CUSTKEY + 1 > 10000 from orders | 1.990 | 2.120 | 1.3 |

### Column O_ORDERSTATUS
* Type: FixedString(1)
* Count: 150000000
* Min: F
* Max: P

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERSTATUS from orders | 5.209 | 5.386 | 3.1 |
| selraw count(O_ORDERSTATUS) from orders | 0.027 | 1.345 | 0.4 |
| selraw min(O_ORDERSTATUS) from orders | 0.467 | 1.609 | 2.2 |
| selraw max(O_ORDERSTATUS) from orders | 0.460 | 1.629 | 2.3 |
| selraw O_ORDERSTATUS from orders where O_ORDERSTATUS >= 'F' and O_ORDERSTATUS <= 'P' | 5.381 | 5.588 | 2.8 |
| selraw O_ORDERSTATUS from orders where O_ORDERSTATUS < 'F' or O_ORDERSTATUS > 'P' | 0.358 | 0.403 | 0.8 |
| selraw O_ORDERSTATUS = 'abc' from orders | 2.018 | 2.291 | 1.4 |

### Column O_TOTALPRICE
* Type: Float64
* Count: 150000000
* Min: 811.73
* Max: 591036.15

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_TOTALPRICE from orders | 29.229 | 29.796 | 1.8 |
| selraw count(O_TOTALPRICE) from orders | 0.125 | 1.168 | 0.6 |
| selraw min(O_TOTALPRICE) from orders | 0.162 | 1.225 | 0.6 |
| selraw max(O_TOTALPRICE) from orders | 0.157 | 1.223 | 0.6 |
| selraw sum(O_TOTALPRICE) from orders | 0.156 | 1.190 | 0.8 |
| selraw O_TOTALPRICE from orders where O_TOTALPRICE >= 811.73 and O_TOTALPRICE <= 591036.15 | 29.476 | 29.326 | 2.1 |
| selraw O_TOTALPRICE from orders where O_TOTALPRICE < 811.73 or O_TOTALPRICE > 591036.15 | 0.174 | 0.145 | 0.1 |
| selraw O_TOTALPRICE + 1 > 10000 from orders | 2.056 | 2.437 | 1.6 |

### Column O_ORDERDATE
* Type: Date
* Count: 150000000
* Min: 1992-01-01
* Max: 1998-08-02

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERDATE from orders | 4.294 | 4.431 | 1.7 |
| selraw count(O_ORDERDATE) from orders | 0.024 | 1.109 | 0.2 |
| selraw min(O_ORDERDATE) from orders | 0.059 | 1.137 | 0.2 |
| selraw max(O_ORDERDATE) from orders | 0.060 | 1.156 | 0.2 |
| selraw O_ORDERDATE from orders where O_ORDERDATE >= '1992-01-01' and O_ORDERDATE <= '1998-08-02' | 4.375 | 4.549 | 1.5 |
| selraw O_ORDERDATE from orders where O_ORDERDATE < '1992-01-01' or O_ORDERDATE > '1998-08-02' | 0.042 | 0.064 | 0.5 |
| selraw O_ORDERDATE + 1 > '1990-01-01' from orders | 1.991 | 2.269 | 4.8 |

### Column O_ORDERPRIORITY
* Type: String
* Count: 150000000
* Min: 1-URGENT
* Max: 5-LOW

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERPRIORITY from orders | 11.559 | 11.284 | 2.8 |
| selraw count(O_ORDERPRIORITY) from orders | 0.237 | 1.611 | 0.6 |
| selraw min(O_ORDERPRIORITY) from orders | 0.540 | 1.927 | 3.1 |
| selraw max(O_ORDERPRIORITY) from orders | 0.468 | 1.723 | 2.6 |
| selraw O_ORDERPRIORITY from orders where O_ORDERPRIORITY >= '1-URGENT' and O_ORDERPRIORITY <= '5-LOW' | 11.483 | 11.489 | 3.2 |
| selraw O_ORDERPRIORITY from orders where O_ORDERPRIORITY < '1-URGENT' or O_ORDERPRIORITY > '5-LOW' | 0.702 | 0.718 | 0.7 |
| selraw O_ORDERPRIORITY = 'abc' from orders | 2.005 | 2.377 | 1.4 |

### Column O_CLERK
* Type: String
* Count: 150000000
* Min: Clerk#000000001
* Max: Clerk#000100000

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_CLERK from orders | 10.704 | 11.431 | 4.9 |
| selraw count(O_CLERK) from orders | 0.241 | 1.725 | 1.5 |
| selraw min(O_CLERK) from orders | 0.489 | 2.033 | 4.8 |
| selraw max(O_CLERK) from orders | 0.480 | 1.863 | 4.7 |
| selraw O_CLERK from orders where O_CLERK >= 'Clerk#000000001' and O_CLERK <= 'Clerk#000100000' | 10.898 | 10.838 | 4.8 |
| selraw O_CLERK from orders where O_CLERK < 'Clerk#000000001' or O_CLERK > 'Clerk#000100000' | 0.624 | 0.796 | 2.0 |
| selraw O_CLERK = 'abc' from orders | 2.092 | 2.426 | 2.9 |

### Column O_SHIPPRIORITY
* Type: Int32
* Count: 150000000
* Min: 0
* Max: 0

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_SHIPPRIORITY from orders | 2.635 | 2.600 | 1.0 |
| selraw count(O_SHIPPRIORITY) from orders | 0.020 | 1.068 | 0.1 |
| selraw min(O_SHIPPRIORITY) from orders | 0.069 | 1.212 | 0.1 |
| selraw max(O_SHIPPRIORITY) from orders | 0.067 | 1.272 | 0.1 |
| selraw sum(O_SHIPPRIORITY) from orders | 0.061 | 1.253 | 0.2 |
| selraw O_SHIPPRIORITY from orders where O_SHIPPRIORITY >= 0 and O_SHIPPRIORITY <= 0 | 2.655 | 2.728 | 1.1 |
| selraw O_SHIPPRIORITY from orders where O_SHIPPRIORITY < 0 or O_SHIPPRIORITY > 0 | 0.048 | 0.062 | 0.1 |
| selraw O_SHIPPRIORITY + 1 > 10000 from orders | 1.990 | 2.152 | 1.1 |

### Column O_COMMENT
* Type: String
* Count: 150000000
* Min:  Tiresias about the
* Max: zzle? unusual requests w

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_COMMENT from orders | 28.923 | 31.515 | 9.0 |
| selraw count(O_COMMENT) from orders | 1.139 | 2.189 | 4.7 |
| selraw min(O_COMMENT) from orders | 1.341 | 2.448 | 8.8 |
| selraw max(O_COMMENT) from orders | 1.301 | 2.776 | 8.2 |
| selraw O_COMMENT from orders where O_COMMENT >= ' Tiresias about the' and O_COMMENT <= 'zzle? unusual requests w' | 31.042 | 31.541 | 10.4 |
| selraw O_COMMENT from orders where O_COMMENT < ' Tiresias about the' or O_COMMENT > 'zzle? unusual requests w' | 1.182 | 1.419 | 5.6 |
| selraw O_COMMENT = 'abc' from orders | 2.673 | 3.077 | 6.8 |

## Multi columns

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY from orders | 4.482 | 4.766 | 1.6 |
| selraw count(O_ORDERKEY) from orders | 0.051 | 1.155 | 0.6 |
| selraw min(O_ORDERKEY) from orders | 0.085 | 1.078 | 0.6 |
| selraw max(O_ORDERKEY) from orders | 0.090 | 1.028 | 0.5 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY, O_CUSTKEY from orders | 9.340 | 9.529 | 2.7 |
| selraw count(O_ORDERKEY), count(O_CUSTKEY) from orders | 0.152 | 1.217 | 1.0 |
| selraw min(O_ORDERKEY), min(O_CUSTKEY) from orders | 0.158 | 1.219 | 1.3 |
| selraw max(O_ORDERKEY), max(O_CUSTKEY) from orders | 0.166 | 1.277 | 1.3 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS from orders | 18.993 | 19.287 | 5.4 |
| selraw count(O_ORDERKEY), count(O_CUSTKEY), count(O_ORDERSTATUS) from orders | 0.211 | 1.648 | 1.5 |
| selraw min(O_ORDERKEY), min(O_CUSTKEY), min(O_ORDERSTATUS) from orders | 0.743 | 2.115 | 5.8 |
| selraw max(O_ORDERKEY), max(O_CUSTKEY), max(O_ORDERSTATUS) from orders | 0.649 | 2.261 | 5.6 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE from orders | 55.110 | 54.277 | 7.2 |
| selraw count(O_ORDERKEY), count(O_CUSTKEY), count(O_ORDERSTATUS), count(O_TOTALPRICE) from orders | 0.522 | 1.990 | 3.0 |
| selraw min(O_ORDERKEY), min(O_CUSTKEY), min(O_ORDERSTATUS), min(O_TOTALPRICE) from orders | 0.971 | 2.556 | 7.7 |
| selraw max(O_ORDERKEY), max(O_CUSTKEY), max(O_ORDERSTATUS), max(O_TOTALPRICE) from orders | 0.927 | 2.527 | 7.6 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE from orders | 61.428 | 60.527 | 8.1 |
| selraw count(O_ORDERKEY), count(O_CUSTKEY), count(O_ORDERSTATUS), count(O_TOTALPRICE), count(O_ORDERDATE) from orders | 0.505 | 2.227 | 4.3 |
| selraw min(O_ORDERKEY), min(O_CUSTKEY), min(O_ORDERSTATUS), min(O_TOTALPRICE), min(O_ORDERDATE) from orders | 1.102 | 2.896 | 11.2 |
| selraw max(O_ORDERKEY), max(O_CUSTKEY), max(O_ORDERSTATUS), max(O_TOTALPRICE), max(O_ORDERDATE) from orders | 1.051 | 2.940 | 8.0 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY from orders | 77.294 | 76.189 | 11.5 |
| selraw count(O_ORDERKEY), count(O_CUSTKEY), count(O_ORDERSTATUS), count(O_TOTALPRICE), count(O_ORDERDATE), count(O_ORDERPRIORITY) from orders | 1.090 | 2.973 | 19.5 |
| selraw min(O_ORDERKEY), min(O_CUSTKEY), min(O_ORDERSTATUS), min(O_TOTALPRICE), min(O_ORDERDATE), min(O_ORDERPRIORITY) from orders | 1.888 | 3.983 | 15.8 |
| selraw max(O_ORDERKEY), max(O_CUSTKEY), max(O_ORDERSTATUS), max(O_TOTALPRICE), max(O_ORDERDATE), max(O_ORDERPRIORITY) from orders | 1.870 | 3.956 | 11.4 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK from orders | 90.921 | 92.222 | 16.0 |
| selraw count(O_ORDERKEY), count(O_CUSTKEY), count(O_ORDERSTATUS), count(O_TOTALPRICE), count(O_ORDERDATE), count(O_ORDERPRIORITY), count(O_CLERK) from orders | 1.223 | 4.248 | 5.0 |
| selraw min(O_ORDERKEY), min(O_CUSTKEY), min(O_ORDERSTATUS), min(O_TOTALPRICE), min(O_ORDERDATE), min(O_ORDERPRIORITY), min(O_CLERK) from orders | 2.400 | 5.194 | 16.4 |
| selraw max(O_ORDERKEY), max(O_CUSTKEY), max(O_ORDERSTATUS), max(O_TOTALPRICE), max(O_ORDERDATE), max(O_ORDERPRIORITY), max(O_CLERK) from orders | 2.437 | 5.222 | 16.4 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY from orders | 95.959 | 96.506 | 17.8 |
| selraw count(O_ORDERKEY), count(O_CUSTKEY), count(O_ORDERSTATUS), count(O_TOTALPRICE), count(O_ORDERDATE), count(O_ORDERPRIORITY), count(O_CLERK), count(O_SHIPPRIORITY) from orders | 1.443 | 3.693 | 5.7 |
| selraw min(O_ORDERKEY), min(O_CUSTKEY), min(O_ORDERSTATUS), min(O_TOTALPRICE), min(O_ORDERDATE), min(O_ORDERPRIORITY), min(O_CLERK), min(O_SHIPPRIORITY) from orders | 2.618 | 5.153 | 14.6 |
| selraw max(O_ORDERKEY), max(O_CUSTKEY), max(O_ORDERSTATUS), max(O_TOTALPRICE), max(O_ORDERDATE), max(O_ORDERPRIORITY), max(O_CLERK), max(O_SHIPPRIORITY) from orders | 2.726 | 5.454 | 15.5 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT from orders | 123.834 | 120.873 | 26.0 |
| selraw count(O_ORDERKEY), count(O_CUSTKEY), count(O_ORDERSTATUS), count(O_TOTALPRICE), count(O_ORDERDATE), count(O_ORDERPRIORITY), count(O_CLERK), count(O_SHIPPRIORITY), count(O_COMMENT) from orders | 3.136 | 5.265 | 25.1 |
| selraw min(O_ORDERKEY), min(O_CUSTKEY), min(O_ORDERSTATUS), min(O_TOTALPRICE), min(O_ORDERDATE), min(O_ORDERPRIORITY), min(O_CLERK), min(O_SHIPPRIORITY), min(O_COMMENT) from orders | 4.806 | 7.969 | 25.4 |
| selraw max(O_ORDERKEY), max(O_CUSTKEY), max(O_ORDERSTATUS), max(O_TOTALPRICE), max(O_ORDERDATE), max(O_ORDERPRIORITY), max(O_CLERK), max(O_SHIPPRIORITY), max(O_COMMENT) from orders | 4.950 | 7.873 | 24.2 |

## Group by
### Column O_ORDERSTATUS
* Distinct count: 3

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from orders group by O_ORDERSTATUS | 0.336 | 1.657 | 2.0 |

### Column O_ORDERPRIORITY
* Distinct count: 5

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from orders group by O_ORDERPRIORITY | 0.571 | 1.974 | 2.4 |

### Column O_SHIPPRIORITY
* Distinct count: 1

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from orders group by O_SHIPPRIORITY | 0.069 | 1.188 | 1.3 |

