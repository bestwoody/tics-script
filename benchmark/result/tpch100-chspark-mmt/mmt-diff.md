# Possible Reasons of TPCH result Error

## Different group of results
* TiSpark:
    * Spark on TiDB
* MergeTree:
    * Spark on CH
    * Use MergeTree engine, allow duplicated primary key
* MutableMergeTree:
    * Spark on CH
    * Use MutableMergeTree engine, use hash(primary key) partitioning instead of 'date' partitioning
    * No duplicated primary key, will be deduplicated on writing(dedup in block) and reading
* SelRaw on MutableMergeTree:
    * Spark on CH
    * Use MutableMergeTree Engine, use hash(primary key) partitioning instead of 'date' partitioning
    * Allow duplicated primary key, use the same reading plan as original MergeTree

# Possible reasons of unmatched results
* TiSpark `!=` MergeTree:
    * CHSpark plan error
    * Codec error
        * CH to Arrow
        * Arrow to Spark
* MergeTree `!=` MutableMergeTree:
    * if MutableMergeTree `==` SelRaw on MutableMergeTree:
        * Uncorrected partitioning when writing data
    * Uncorrected deduplicating when reading data
