# Spark-Connector

## Dir notes:
* `chraw-jni`
    * Command line debug tool for executing query with libch.so
* `spark`
    * The official repository
    * Git submodule of `theflash` repository
* `chspark`
    * Spark-Connector, jar package running in spark

## Progress
```
** Mock table in spark
** Bytes -> ArrowFormat -> SparkRows
** Execute query and fetch data from CH service
** Predicate pushdown
** CH Cluster support
** Integration test
** TPCH benchmark
-- Spark RDD retry handling
```
