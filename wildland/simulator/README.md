# The Storage Engine Simulator

## The Simulator

### Goals
Create a simulator to help design new engine, currently for the lazy engine
* Easy to design and adjust the algorithms of storage engine in the simulator
* Predict performance in realtime in the simulator

### Key Features
* Read/write simulating with patterns
* Data store(persist) and compaction simulating
* Resource simulating: CPU, IOPS, IOBW, Locks
* Performance calculating
* Visualize data distrubition in the storage engine

## The Lazy Engine

### Goals
* Lowest write amplification
* Highest write performance for AP(meaning only support UPSERT)
* High read performance for AP

### Key Features:
* Auto-adjust for any write and read patterns
* Delay the compaction as long as it can
* Keep data in a rough order to deliver workable read performance

### Progress
#### Simulator
```
*****   Frame work
*****     Operators: read, write, scan range, scan all
*****     Base patterns: append, uniform
*****     Combined patterns: round robin, probability distribution
*****     Pattern/operator executor
*****     Put args into config file
-----     Put hardware module into config file
*****   Resource simulating
*****     Disk: IOPS, IOBW. TODO: more types of IOPS (eg: seq write with fsync)
*****     CPU: compress, sort, sorted merge, locks, key compare
*****     Memory: current used, peak used
*****   Performance calculating
*****     Disk: IOPS, IOBW
****-     CPU: compress, sort, sorted merge, locks. TODO: key compare
*****     Memory: current used, peak used
****-     Parallel operating
*****     Simulated baseline
```

#### Lazy engine
```
*****   Frame work
*****     DB common modules: segment, distribution, file, cache
****-     DB base class for lazy engine family. TODO: partition sorted flag
****-   Lazy Engine simulating
*****     Base operatings: write, read, scan range, scan all
****-     Partition split. TODO: use distribution info for split
*****     Latency simulating, avg latency calculating
*****     OLTP simulating
*****     OLAP simulating
*****     Write cache with WAL
*****       Collect batch by partition
*****       Collect batch globally
*****       Auto adjusted multi-partition write cache
*****     Implement compaction
-----     Delta-base data storage implement
***--     Read cache
*****       File cache
*****       Kv cache
-----       Partition cache
-----   Policy selecting
-----     Data storage policy: unsorted/multi sorted parts/...
-----     Read/scan policy: unsorted/multi sorted parts/...
***--     Compaction on read policy: multi sorted parts/fully sorted
```

#### Pattern simulating and match
```
*****   Append, reversed append
***--   Multi thread append
***--   Uniform
***--   Hotzone
-----   Hotzone with hotkey
-----   NoUpsert (not only upsert)
-----   MVCC
```

### Next TODO
* Half-sort
* Delta-base data storage
* Better cache info display
