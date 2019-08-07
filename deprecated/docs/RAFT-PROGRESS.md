# TiKV and TheFlash Integration progress

```
TiKV-TheFlash connecting
    - Protocol definition                     ****
    - Client/Server implement                 ****
    + Commands transfer and handling
        - Data commands (snapshot included)   ****
        - Admin commands of regions           **--
        - Admin commands of peers             ****
    - Raft apply state maintainess            ****

TheFlash data writing
    + Codec
        - TiDB key-value composition          ****
        - TiDBValue to values (with types)    ****
        - Values to TheFlash block (columns)  ****
    + Cache layer (KVStore/Region/Flusher)
        - Memory structures                   ****
        - Prewrite-Commit matching            ****
        - Raft apply state reporting          ***-
        - Cache persisting                    ****
        - Interval cache persisting           ****
        - Cache flushing: by size + deadline  ****
    + Schema syncing
        - Types mapping                       ****
        - Fetch schema from TiDB              ****
        - Convert To TheFlash format          ****
        - Extra info persiting (in TheFlash)  ****
        - Version check and update on writing ----
    + Partitioning
        - Data routing                        ****
        - Region split/merge                  ***-
        - Region-Partition map persiting      ****
    + Storage engine (TxnMergeTree)
        - Data writing                        ****
        - Data merging (compaction)           ****
        - Data GC (with TS from PD)           ****
    + Partition data operating
        - Purge region (range) in partition   ****
        - Copy region between partitions      ****
        - Async (online) operating            ----

TheFlash data reading
    - Spark and CH integration                ****
    - Cache layer reading                     ****
    - Resovle (percolator) locks              ****
    - MVCC supporting (snapshot read)
        - Storage engine layer supporting     ----
        - Cache layer supporting              ----
    - Learner read                            ----

Consistence guarantee
    - TheFlash data cache persisting          ****
    - TheFlash Cache layer data locks (mutex) ****
    - TheFlash extra meta data persisting     ****
        - Raft apply state
        - TiDB schema extra info
        - Partitioning info
    - TheFlash data idempotent writing        ****
    - TheFlash partition idempotent operating ----

Performance tuning
    - TiKV-TheFlash data transfer             ----
    - Codec                                   ----
    - TheFlash cache writing
        - Batch size, locks, ETC              ----
        - Async flushing                      ----
    - Storage engine (TxnMergeTree)
        - MVCC supporting                     ----
        - Data merging (compaction)           ----
        - Data GC                             ----
    - Resovle locks on reading                ----

Detail test
    - TiKV-TheFlash connecting                ----
    - TheFlash data cache persisting          ----
    - TheFlash meta data persisting           ----
    - Storage engine (TxnMergeTree)
        - Data reading                        ----
        - Data merging (compaction)           ----
```
