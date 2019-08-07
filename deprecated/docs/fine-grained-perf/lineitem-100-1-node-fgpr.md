# Fine grained perf report for table lineitem
* Time unit: sec.
## Single column
### Column L_ORDERKEY
* Type: Int32
* Count: 600037902
* Min: 1
* Max: 600000000

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY from lineitem | 18.929 | 18.908 | 6.5 |
| selraw count(L_ORDERKEY) from lineitem | 0.202 | 2.365 | 1.5 |
| selraw min(L_ORDERKEY) from lineitem | 0.340 | 2.615 | 2.1 |
| selraw max(L_ORDERKEY) from lineitem | 0.371 | 2.915 | 1.3 |
| selraw sum(L_ORDERKEY) from lineitem | 0.328 | 2.666 | 1.6 |
| selraw L_ORDERKEY from lineitem where L_ORDERKEY >= 1 and L_ORDERKEY <= 600000000 | 19.023 | 19.092 | 5.4 |
| selraw L_ORDERKEY from lineitem where L_ORDERKEY < 1 or L_ORDERKEY > 600000000 | 0.005 | 0.012 | 0.4 |
| selraw L_ORDERKEY + 1 > 10000 from lineitem | 8.008 | 8.520 | 5.3 |

### Column L_PARTKEY
* Type: Int32
* Count: 600037902
* Min: 1
* Max: 20000000

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_PARTKEY from lineitem | 19.230 | 20.323 | 5.1 |
| selraw count(L_PARTKEY) from lineitem | 0.152 | 1.599 | 1.7 |
| selraw min(L_PARTKEY) from lineitem | 0.301 | 2.063 | 1.4 |
| selraw max(L_PARTKEY) from lineitem | 0.310 | 2.545 | 1.2 |
| selraw sum(L_PARTKEY) from lineitem | 0.282 | 2.934 | 1.3 |
| selraw L_PARTKEY from lineitem where L_PARTKEY >= 1 and L_PARTKEY <= 20000000 | 20.100 | 20.356 | 6.5 |
| selraw L_PARTKEY from lineitem where L_PARTKEY < 1 or L_PARTKEY > 20000000 | 0.336 | 0.387 | 0.4 |
| selraw L_PARTKEY + 1 > 10000 from lineitem | 7.998 | 8.481 | 5.0 |

### Column L_SUPPKEY
* Type: Int32
* Count: 600037902
* Min: 1
* Max: 1000000

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_SUPPKEY from lineitem | 16.504 | 17.122 | 5.4 |
| selraw count(L_SUPPKEY) from lineitem | 0.180 | 2.187 | 1.3 |
| selraw min(L_SUPPKEY) from lineitem | 0.310 | 2.439 | 1.2 |
| selraw max(L_SUPPKEY) from lineitem | 0.296 | 2.452 | 1.2 |
| selraw sum(L_SUPPKEY) from lineitem | 0.273 | 3.183 | 1.4 |
| selraw L_SUPPKEY from lineitem where L_SUPPKEY >= 1 and L_SUPPKEY <= 1000000 | 16.589 | 17.448 | 6.6 |
| selraw L_SUPPKEY from lineitem where L_SUPPKEY < 1 or L_SUPPKEY > 1000000 | 0.371 | 0.297 | 0.4 |
| selraw L_SUPPKEY + 1 > 10000 from lineitem | 8.108 | 8.135 | 5.1 |

### Column L_LINENUMBER
* Type: Int32
* Count: 600037902
* Min: 1
* Max: 7

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_LINENUMBER from lineitem | 11.338 | 11.739 | 3.8 |
| selraw count(L_LINENUMBER) from lineitem | 0.327 | 2.512 | 0.8 |
| selraw min(L_LINENUMBER) from lineitem | 0.500 | 2.763 | 0.4 |
| selraw max(L_LINENUMBER) from lineitem | 0.487 | 2.194 | 0.6 |
| selraw sum(L_LINENUMBER) from lineitem | 0.451 | 2.508 | 0.7 |
| selraw L_LINENUMBER from lineitem where L_LINENUMBER >= 1 and L_LINENUMBER <= 7 | 11.584 | 11.535 | 3.4 |
| selraw L_LINENUMBER from lineitem where L_LINENUMBER < 1 or L_LINENUMBER > 7 | 0.486 | 3.285 | 0.3 |
| selraw L_LINENUMBER + 1 > 10000 from lineitem | 7.957 | 8.186 | 3.3 |

### Column L_QUANTITY
* Type: Float32
* Count: 600037902
* Min: 1
* Max: 50

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_QUANTITY from lineitem | 84.985 | 83.898 | 4.0 |
| selraw count(L_QUANTITY) from lineitem | 0.346 | 2.253 | 0.5 |
| selraw min(L_QUANTITY) from lineitem | 0.567 | 2.689 | 0.6 |
| selraw max(L_QUANTITY) from lineitem | 0.548 | 3.112 | 0.6 |
| selraw sum(L_QUANTITY) from lineitem | 0.496 | 3.064 | 0.8 |
| selraw L_QUANTITY from lineitem where L_QUANTITY >= 1 and L_QUANTITY <= 50 | 84.455 | 84.377 | 3.9 |
| selraw L_QUANTITY from lineitem where L_QUANTITY < 1 or L_QUANTITY > 50 | 0.500 | 0.472 | 0.8 |
| selraw L_QUANTITY + 1 > 10000 from lineitem | 8.213 | 8.117 | 3.5 |

### Column L_EXTENDEDPRICE
* Type: Float32
* Count: 600037902
* Min: 900.05
* Max: 104948.5

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_EXTENDEDPRICE from lineitem | 125.718 | 126.665 | 5.2 |
| selraw count(L_EXTENDEDPRICE) from lineitem | 0.221 | 2.005 | 1.4 |
| selraw min(L_EXTENDEDPRICE) from lineitem | 0.383 | 2.209 | 1.3 |
| selraw max(L_EXTENDEDPRICE) from lineitem | 0.363 | 2.555 | 1.3 |
| selraw sum(L_EXTENDEDPRICE) from lineitem | 0.375 | 2.507 | 1.6 |
| selraw L_EXTENDEDPRICE from lineitem where L_EXTENDEDPRICE >= 900.05 and L_EXTENDEDPRICE <= 104948.5 | 126.369 | 127.756 | 5.8 |
| selraw L_EXTENDEDPRICE from lineitem where L_EXTENDEDPRICE < 900.05 or L_EXTENDEDPRICE > 104948.5 | 0.270 | 0.435 | 1.7 |
| selraw L_EXTENDEDPRICE + 1 > 10000 from lineitem | 8.608 | 8.669 | 4.8 |

### Column L_DISCOUNT
* Type: Float64
* Count: 600037902
* Min: 0
* Max: 0.1

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_DISCOUNT from lineitem | 75.380 | 75.848 | 4.0 |
| selraw count(L_DISCOUNT) from lineitem | 0.355 | 2.403 | 0.4 |
| selraw min(L_DISCOUNT) from lineitem | 0.508 | 2.640 | 0.6 |
| selraw max(L_DISCOUNT) from lineitem | 0.541 | 2.933 | 0.6 |
| selraw sum(L_DISCOUNT) from lineitem | 0.504 | 2.248 | 0.8 |
| selraw L_DISCOUNT from lineitem where L_DISCOUNT >= 0 and L_DISCOUNT <= 0.1 | 75.890 | 76.218 | 4.2 |
| selraw L_DISCOUNT from lineitem where L_DISCOUNT < 0 or L_DISCOUNT > 0.1 | 0.456 | 0.587 | 0.3 |
| selraw L_DISCOUNT + 1 > 10000 from lineitem | 8.126 | 8.467 | 3.4 |

### Column L_TAX
* Type: Float64
* Count: 600037902
* Min: 0
* Max: 0.08

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_TAX from lineitem | 72.450 | 71.457 | 3.8 |
| selraw count(L_TAX) from lineitem | 0.264 | 1.917 | 0.4 |
| selraw min(L_TAX) from lineitem | 0.419 | 2.241 | 0.6 |
| selraw max(L_TAX) from lineitem | 0.418 | 2.272 | 0.6 |
| selraw sum(L_TAX) from lineitem | 0.425 | 2.829 | 0.4 |
| selraw L_TAX from lineitem where L_TAX >= 0 and L_TAX <= 0.08 | 71.818 | 71.367 | 4.4 |
| selraw L_TAX from lineitem where L_TAX < 0 or L_TAX > 0.08 | 0.421 | 0.517 | 0.3 |
| selraw L_TAX + 1 > 10000 from lineitem | 8.055 | 8.250 | 3.6 |

### Column L_RETURNFLAG
* Type: FixedString(1)
* Count: 600037902
* Min: A
* Max: R

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_RETURNFLAG from lineitem | 20.759 | 20.674 | 10.5 |
| selraw count(L_RETURNFLAG) from lineitem | 0.104 | 1.504 | 1.5 |
| selraw min(L_RETURNFLAG) from lineitem | 1.692 | 3.022 | 8.4 |
| selraw max(L_RETURNFLAG) from lineitem | 1.677 | 2.931 | 10.3 |
| selraw L_RETURNFLAG from lineitem where L_RETURNFLAG >= 'A' and L_RETURNFLAG <= 'R' | 20.763 | 21.162 | 10.3 |
| selraw L_RETURNFLAG from lineitem where L_RETURNFLAG < 'A' or L_RETURNFLAG > 'R' | 1.323 | 1.479 | 2.2 |
| selraw L_RETURNFLAG = 'abc' from lineitem | 7.934 | 8.001 | 4.6 |

### Column L_LINESTATUS
* Type: FixedString(1)
* Count: 600037902
* Min: F
* Max: O

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_LINESTATUS from lineitem | 20.830 | 21.183 | 9.7 |
| selraw count(L_LINESTATUS) from lineitem | 0.098 | 1.862 | 1.3 |
| selraw min(L_LINESTATUS) from lineitem | 1.637 | 2.933 | 9.0 |
| selraw max(L_LINESTATUS) from lineitem | 1.654 | 3.059 | 9.6 |
| selraw L_LINESTATUS from lineitem where L_LINESTATUS >= 'F' and L_LINESTATUS <= 'O' | 21.088 | 21.351 | 10.0 |
| selraw L_LINESTATUS from lineitem where L_LINESTATUS < 'F' or L_LINESTATUS > 'O' | 1.400 | 1.684 | 2.0 |
| selraw L_LINESTATUS = 'abc' from lineitem | 8.013 | 8.289 | 4.2 |

### Column L_SHIPDATE
* Type: Date
* Count: 600037902
* Min: 1992-01-02
* Max: 1998-12-01

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_SHIPDATE from lineitem | 17.171 | 17.275 | 5.3 |
| selraw count(L_SHIPDATE) from lineitem | 0.097 | 2.191 | 0.6 |
| selraw min(L_SHIPDATE) from lineitem | 0.242 | 2.303 | 0.6 |
| selraw max(L_SHIPDATE) from lineitem | 0.242 | 1.937 | 0.7 |
| selraw L_SHIPDATE from lineitem where L_SHIPDATE >= '1992-01-02' and L_SHIPDATE <= '1998-12-01' | 17.276 | 17.872 | 5.1 |
| selraw L_SHIPDATE from lineitem where L_SHIPDATE < '1992-01-02' or L_SHIPDATE > '1998-12-01' | 0.168 | 0.196 | 0.9 |
| selraw L_SHIPDATE + 1 > '1990-01-01' from lineitem | 7.975 | 8.141 | 16.5 |

### Column L_COMMITDATE
* Type: Date
* Count: 600037902
* Min: 1992-01-31
* Max: 1998-10-31

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_COMMITDATE from lineitem | 17.245 | 17.308 | 5.6 |
| selraw count(L_COMMITDATE) from lineitem | 0.074 | 1.618 | 0.6 |
| selraw min(L_COMMITDATE) from lineitem | 0.222 | 1.850 | 0.6 |
| selraw max(L_COMMITDATE) from lineitem | 0.212 | 2.092 | 0.7 |
| selraw L_COMMITDATE from lineitem where L_COMMITDATE >= '1992-01-31' and L_COMMITDATE <= '1998-10-31' | 17.178 | 17.528 | 5.1 |
| selraw L_COMMITDATE from lineitem where L_COMMITDATE < '1992-01-31' or L_COMMITDATE > '1998-10-31' | 0.157 | 0.204 | 0.9 |
| selraw L_COMMITDATE + 1 > '1990-01-01' from lineitem | 7.949 | 8.239 | 16.4 |

### Column L_RECEIPTDATE
* Type: Date
* Count: 600037902
* Min: 1992-01-03
* Max: 1998-12-31

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_RECEIPTDATE from lineitem | 17.203 | 17.246 | 5.4 |
| selraw count(L_RECEIPTDATE) from lineitem | 0.100 | 1.703 | 0.6 |
| selraw min(L_RECEIPTDATE) from lineitem | 0.246 | 1.828 | 0.6 |
| selraw max(L_RECEIPTDATE) from lineitem | 0.236 | 1.994 | 0.6 |
| selraw L_RECEIPTDATE from lineitem where L_RECEIPTDATE >= '1992-01-03' and L_RECEIPTDATE <= '1998-12-31' | 17.325 | 17.905 | 5.1 |
| selraw L_RECEIPTDATE from lineitem where L_RECEIPTDATE < '1992-01-03' or L_RECEIPTDATE > '1998-12-31' | 0.155 | 0.209 | 0.9 |
| selraw L_RECEIPTDATE + 1 > '1990-01-01' from lineitem | 8.065 | 8.036 | 15.9 |

### Column L_SHIPINSTRUCT
* Type: String
* Count: 600037902
* Min: COLLECT COD
* Max: TAKE BACK RETURN

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_SHIPINSTRUCT from lineitem | 49.771 | 49.762 | 10.9 |
| selraw count(L_SHIPINSTRUCT) from lineitem | 1.240 | 4.697 | 2.1 |
| selraw min(L_SHIPINSTRUCT) from lineitem | 2.459 | 6.022 | 9.1 |
| selraw max(L_SHIPINSTRUCT) from lineitem | 2.434 | 5.903 | 9.9 |
| selraw L_SHIPINSTRUCT from lineitem where L_SHIPINSTRUCT >= 'COLLECT COD' and L_SHIPINSTRUCT <= 'TAKE BACK RETURN' | 49.570 | 49.530 | 11.8 |
| selraw L_SHIPINSTRUCT from lineitem where L_SHIPINSTRUCT < 'COLLECT COD' or L_SHIPINSTRUCT > 'TAKE BACK RETURN' | 2.862 | 3.379 | 2.5 |
| selraw L_SHIPINSTRUCT = 'abc' from lineitem | 7.991 | 8.327 | 4.8 |

### Column L_SHIPMODE
* Type: String
* Count: 600037902
* Min: AIR
* Max: TRUCK

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_SHIPMODE from lineitem | 39.820 | 40.303 | 9.9 |
| selraw count(L_SHIPMODE) from lineitem | 0.674 | 2.568 | 1.7 |
| selraw min(L_SHIPMODE) from lineitem | 1.678 | 4.000 | 9.0 |
| selraw max(L_SHIPMODE) from lineitem | 1.897 | 4.073 | 9.4 |
| selraw L_SHIPMODE from lineitem where L_SHIPMODE >= 'AIR' and L_SHIPMODE <= 'TRUCK' | 39.604 | 40.279 | 10.5 |
| selraw L_SHIPMODE from lineitem where L_SHIPMODE < 'AIR' or L_SHIPMODE > 'TRUCK' | 2.185 | 2.608 | 2.5 |
| selraw L_SHIPMODE = 'abc' from lineitem | 8.879 | 7.705 | 5.2 |

### Column L_COMMENT
* Type: String
* Count: 600037902
* Min:  Tiresias
* Max: zzle? unusual

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_COMMENT from lineitem | 84.896 | 85.120 | 25.6 |
| selraw count(L_COMMENT) from lineitem | 2.094 | 4.190 | 10.1 |
| selraw min(L_COMMENT) from lineitem | 2.891 | 5.856 | 22.4 |
| selraw max(L_COMMENT) from lineitem | 3.024 | 5.280 | 25.1 |
| selraw L_COMMENT from lineitem where L_COMMENT >= ' Tiresias ' and L_COMMENT <= 'zzle? unusual' | 85.213 | 84.927 | 28.1 |
| selraw L_COMMENT from lineitem where L_COMMENT < ' Tiresias ' or L_COMMENT > 'zzle? unusual' | 3.626 | 3.250 | 14.5 |
| selraw L_COMMENT = 'abc' from lineitem | 8.207 | 8.188 | 15.0 |

## Multi columns

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY from lineitem | 19.000 | 19.648 | 9.7 |
| selraw count(L_ORDERKEY) from lineitem | 0.213 | 2.669 | 2.1 |
| selraw min(L_ORDERKEY) from lineitem | 0.355 | 3.324 | 1.9 |
| selraw max(L_ORDERKEY) from lineitem | 0.364 | 2.863 | 1.7 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY from lineitem | 39.417 | 39.946 | 9.8 |
| selraw count(L_ORDERKEY), count(L_PARTKEY) from lineitem | 0.666 | 2.464 | 3.2 |
| selraw min(L_ORDERKEY), min(L_PARTKEY) from lineitem | 0.730 | 2.875 | 3.2 |
| selraw max(L_ORDERKEY), max(L_PARTKEY) from lineitem | 0.763 | 2.834 | 3.3 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY from lineitem | 58.133 | 58.607 | 13.4 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY) from lineitem | 1.038 | 4.040 | 6.3 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY) from lineitem | 1.044 | 3.480 | 5.9 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY) from lineitem | 1.054 | 4.440 | 6.2 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER from lineitem | 67.990 | 69.752 | 14.8 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER) from lineitem | 1.429 | 3.291 | 7.1 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER) from lineitem | 1.503 | 3.094 | 5.8 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER) from lineitem | 1.532 | 2.842 | 6.0 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY from lineitem | 179.416 | 175.611 | 16.1 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY) from lineitem | 2.167 | 3.627 | 6.6 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY) from lineitem | 2.794 | 4.338 | 7.2 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY) from lineitem | 2.953 | 4.030 | 7.1 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE from lineitem | 317.530 | 314.394 | 18.9 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE) from lineitem | 2.336 | 3.644 | 9.8 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE) from lineitem | 3.511 | 4.974 | 10.3 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE) from lineitem | 3.441 | 4.975 | 11.0 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT from lineitem | 417.287 | 422.850 | 23.1 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT) from lineitem | 2.884 | 5.692 | 10.5 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT) from lineitem | 4.193 | 6.995 | 11.2 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT) from lineitem | 4.123 | 8.318 | 11.1 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX from lineitem | 497.324 | 495.119 | 25.5 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX) from lineitem | 4.714 | 7.150 | 11.2 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX) from lineitem | 8.364 | 9.604 | 11.9 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX) from lineitem | 8.721 | 10.477 | 12.0 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG from lineitem | 534.841 | 526.305 | 36.6 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX), count(L_RETURNFLAG) from lineitem | 6.270 | 8.526 | 13.0 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX), min(L_RETURNFLAG) from lineitem | 7.844 | 11.133 | 40.8 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX), max(L_RETURNFLAG) from lineitem | 7.818 | 10.715 | 43.9 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS from lineitem | 561.968 | 572.918 | 42.3 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX), count(L_RETURNFLAG), count(L_LINESTATUS) from lineitem | 6.626 | 6.896 | 15.8 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX), min(L_RETURNFLAG), min(L_LINESTATUS) from lineitem | 10.516 | 11.576 | 47.7 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX), max(L_RETURNFLAG), max(L_LINESTATUS) from lineitem | 10.520 | 12.151 | 45.8 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE from lineitem | 590.066 | 587.874 | 49.2 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX), count(L_RETURNFLAG), count(L_LINESTATUS), count(L_SHIPDATE) from lineitem | 7.004 | 9.603 | 15.7 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX), min(L_RETURNFLAG), min(L_LINESTATUS), min(L_SHIPDATE) from lineitem | 10.971 | 13.545 | 53.7 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX), max(L_RETURNFLAG), max(L_LINESTATUS), max(L_SHIPDATE) from lineitem | 10.617 | 13.535 | 49.5 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE from lineitem | 608.818 | 619.502 | 52.9 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX), count(L_RETURNFLAG), count(L_LINESTATUS), count(L_SHIPDATE), count(L_COMMITDATE) from lineitem | 7.470 | 10.490 | 16.6 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX), min(L_RETURNFLAG), min(L_LINESTATUS), min(L_SHIPDATE), min(L_COMMITDATE) from lineitem | 11.976 | 13.710 | 53.7 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX), max(L_RETURNFLAG), max(L_LINESTATUS), max(L_SHIPDATE), max(L_COMMITDATE) from lineitem | 11.555 | 14.072 | 53.7 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE from lineitem | 618.177 | 630.551 | 57.0 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX), count(L_RETURNFLAG), count(L_LINESTATUS), count(L_SHIPDATE), count(L_COMMITDATE), count(L_RECEIPTDATE) from lineitem | 8.418 | 11.075 | 19.0 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX), min(L_RETURNFLAG), min(L_LINESTATUS), min(L_SHIPDATE), min(L_COMMITDATE), min(L_RECEIPTDATE) from lineitem | 12.617 | 15.273 | 56.7 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX), max(L_RETURNFLAG), max(L_LINESTATUS), max(L_SHIPDATE), max(L_COMMITDATE), max(L_RECEIPTDATE) from lineitem | 12.546 | 15.096 | 57.3 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT from lineitem | 692.709 | 702.271 | 67.6 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX), count(L_RETURNFLAG), count(L_LINESTATUS), count(L_SHIPDATE), count(L_COMMITDATE), count(L_RECEIPTDATE), count(L_SHIPINSTRUCT) from lineitem | 8.625 | 12.112 | 21.3 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX), min(L_RETURNFLAG), min(L_LINESTATUS), min(L_SHIPDATE), min(L_COMMITDATE), min(L_RECEIPTDATE), min(L_SHIPINSTRUCT) from lineitem | 15.112 | 18.485 | 70.1 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX), max(L_RETURNFLAG), max(L_LINESTATUS), max(L_SHIPDATE), max(L_COMMITDATE), max(L_RECEIPTDATE), max(L_SHIPINSTRUCT) from lineitem | 15.846 | 18.829 | 76.4 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE from lineitem | 750.512 | 744.760 | 79.9 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX), count(L_RETURNFLAG), count(L_LINESTATUS), count(L_SHIPDATE), count(L_COMMITDATE), count(L_RECEIPTDATE), count(L_SHIPINSTRUCT), count(L_SHIPMODE) from lineitem | 13.171 | 22.454 | 24.3 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX), min(L_RETURNFLAG), min(L_LINESTATUS), min(L_SHIPDATE), min(L_COMMITDATE), min(L_RECEIPTDATE), min(L_SHIPINSTRUCT), min(L_SHIPMODE) from lineitem | 19.787 | 25.760 | 73.7 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX), max(L_RETURNFLAG), max(L_LINESTATUS), max(L_SHIPDATE), max(L_COMMITDATE), max(L_RECEIPTDATE), max(L_SHIPINSTRUCT), max(L_SHIPMODE) from lineitem | 20.472 | 26.504 | 74.7 |


| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT from lineitem | 841.954 | 854.582 | 106.5 |
| selraw count(L_ORDERKEY), count(L_PARTKEY), count(L_SUPPKEY), count(L_LINENUMBER), count(L_QUANTITY), count(L_EXTENDEDPRICE), count(L_DISCOUNT), count(L_TAX), count(L_RETURNFLAG), count(L_LINESTATUS), count(L_SHIPDATE), count(L_COMMITDATE), count(L_RECEIPTDATE), count(L_SHIPINSTRUCT), count(L_SHIPMODE), count(L_COMMENT) from lineitem | 18.643 | 20.697 | 38.5 |
| selraw min(L_ORDERKEY), min(L_PARTKEY), min(L_SUPPKEY), min(L_LINENUMBER), min(L_QUANTITY), min(L_EXTENDEDPRICE), min(L_DISCOUNT), min(L_TAX), min(L_RETURNFLAG), min(L_LINESTATUS), min(L_SHIPDATE), min(L_COMMITDATE), min(L_RECEIPTDATE), min(L_SHIPINSTRUCT), min(L_SHIPMODE), min(L_COMMENT) from lineitem | 26.992 | 27.831 | 108.4 |
| selraw max(L_ORDERKEY), max(L_PARTKEY), max(L_SUPPKEY), max(L_LINENUMBER), max(L_QUANTITY), max(L_EXTENDEDPRICE), max(L_DISCOUNT), max(L_TAX), max(L_RETURNFLAG), max(L_LINESTATUS), max(L_SHIPDATE), max(L_COMMITDATE), max(L_RECEIPTDATE), max(L_SHIPINSTRUCT), max(L_SHIPMODE), max(L_COMMENT) from lineitem | 27.814 | 26.026 | 99.7 |

## Group by
### Column L_LINENUMBER
* Distinct count: 7

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from lineitem group by L_LINENUMBER | 0.689 | 2.499 | 3.0 |

### Column L_QUANTITY
* Distinct count: 50

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from lineitem group by L_QUANTITY | 0.787 | 2.992 | 4.7 |

### Column L_DISCOUNT
* Distinct count: 11

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from lineitem group by L_DISCOUNT | 0.658 | 2.787 | 3.5 |

### Column L_TAX
* Distinct count: 9

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from lineitem group by L_TAX | 0.741 | 2.539 | 3.2 |

### Column L_RETURNFLAG
* Distinct count: 3

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from lineitem group by L_RETURNFLAG | 1.390 | 3.319 | 4.7 |

### Column L_LINESTATUS
* Distinct count: 2

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from lineitem group by L_LINESTATUS | 1.250 | 2.901 | 3.9 |

### Column L_SHIPINSTRUCT
* Distinct count: 4

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from lineitem group by L_SHIPINSTRUCT | 2.535 | 6.888 | 4.8 |

### Column L_SHIPMODE
* Distinct count: 7

| Query | CH(selraw) | CH | Parquet |
| ----- | ------- | ------- | ------- |
| selraw count(*) from lineitem group by L_SHIPMODE | 2.226 | 4.183 | 4.4 |

