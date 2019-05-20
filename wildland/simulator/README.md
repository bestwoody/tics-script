# The Storage Engine Simulator

## The Simulator

### Goals
Create a simulator to help designing new engine, currently for the lazy-compact engine
* Easy to design and adjust the algorithms of storage engine in the simulator
* Predicate performance in realtime in the simulator

### Key Features
* Read/write simulating with patterns
* Data store(persist) and compaction simulating
* Resource simulating: CPU, IOPS, IOBW, Locks
* Performance calculating
* Visualize data distrubition in the storage engine

## The Lazy-Compact Engine

### Goals
* Lowest write amplification
* Highest write performance for AP(means only support UPSERT)
* High read performance for AP

### Key Features:
* Auto adjust for any write and read patterns
    * Write then read
    * Append/reverse-append
    * TODO: more
* Delay the compaction as late as it can
* Keep data grain-order to deliver workable read performance

### Progress
Simulator
```
***--   Frame work
**---     Operators: read, write, scan range, scan all
****-     Base patterns: append, uniform
**---     Combined patterns: round robin, probability distribution
****-     Pattern/operator executor
-----     Put args into config file
***--   Resource simulating
*****     Disk: IOPS, IOBW
****-     CPU: compress, sort, sorted merge, locks, key compare
-----     Memory: curren used, peak used
*****   Performance calculating
*****     Disk: IOPS, IOBW
***--     CPU: compress, sort, sorted merge. TODO: locks, key compare
-----     Memory: curren used, peak used
-----     Parallel operating
```

Lazy engine
```
***--   Frame work
****-     DB common modules: segment, distribution, file, cache, DB base class
*----   Algorithm
***--     Write cache. TODO: natural batch
-----     Read cache. TODO: kv cache, partition cache, file cache
*----     Partition split. TODO: use distribution info for split
-----     Compact. TODO: data select, trigger timing
*----   Pattern match
*----     Write then scan
***--       Append write, reverse append write
*----       Multi thread append write
*----       Uniform write
-----       Hot zone write
-----     Interval write read
-----     Cache policy select
-----     Compact policy select: cold/warm/hot
```
