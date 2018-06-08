Moonshine

A research project about storage engine and more.

## Plan and progress
```
*----   - Storage
**---     - Supporting framework
*****       - Type system
***--       - Read/write/compact framework
*----     - Different storage layouts
****-       - SortedPlain
-----       - SortedBlock: like CH without partitioning
-----       - SortedBlock + HashPartition: MutableMergeTree
-----       - SortedBlock + MultiLevelHashPartition
-----       - SortedBlock + RangePartition: use Kudu's keep-order-hash
-----       - SortedBlock + MultiLevelRangePartition
-----       - SortedBlock + B-Tree
-----   - Calculation
-----     - Supporting framework
-----       - Filter
-----       - Express
-----       - Aggregations
-----       - Join
-----     - Optimizations
-----       - Vectorize
-----       - JIT
```
