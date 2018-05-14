# Fine-grained perf report
This document describes the fine-grained perf report strategy for theflash.

## Introduction
Fine-grained perf are conducted using simple queries like `select ... from ...` and/or with simple filter and/or aggregation, so that we can see the perf trend with column attributes (i.e. width/type/count/etc.) and/or filter ops (arithmetic/comparison/string compare/date compare/etc.) and/or aggregations (count/min/max/sum).

We use `ch-cli` for CH queries and `spark-shell` for Spark Parquet queries. Both execution go through the scan -> calculation -> serialization -> output to nullio (`ch-cli > /dev/null` and `spark-shell format("NullFileFormat")`) 4 stages so we consider the comparison is fair.

## Run
* Prerequisite:
  * Spark Parquet queries require third-party project [spark-nullio](https://github.com/animeshtrivedi/spark-nullio), please build and install it properly
* Run script:
  * `theflash/benchmark/fine-grained-perf-ch.sh` to for CH tables
  * `theflash/benchmark/fine-grained-perf-parquet.sh` for Spark Parquet queries
  * `theflash/benchmark/gen-fine-grained-perf-report.sh` to generate a pretty report

