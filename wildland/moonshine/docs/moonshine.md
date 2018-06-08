# Moonshine design doc


## Terms:
* S-Size (SS)
    * The min data size that can reach max IO brandwidth
    * ~= `max brandwidth` / `max IOPS`
    * For HDD: ~256KB
    * For SSD: depends on hardware
* S-Paral (SP)
    * The min partition number that can reach max IO brandwidth and max CPU usage
    * IO
        * Sync IO: min threads that can reach max brandwidth (with big blocks)
        * Async IO: min iodepth
    * CPU: `>=` total cores
    * S-Paral is the bigger number of the above two
* PK
    * Primary key
* Fragment
    * = Partition
* P-Size
    * Partition size limit
    * If a partition over grown this size, it should divide into next level
* Expect-Capacity (EC)
    * DB (or DB node) has a expect capactity at anytime
    * It set to a small number, then grows when data is added
    * This number use to determine strategies, eg: when to compact
    * Growth curve should be recorded to predicte EC change

## Data format: in memory and in disk
```
Data                         - Level 0
  DeltaLog * N             D -
    Delta * N              D - Corresponding to data of one write transaction
      RowSet * SP          D -
        RowData * N        D - Sorted by PK
        RowHash * N        D - Prepared PK hash of each row
      FragmentIndex        D - Entrys in DeltaLog file of each fragment data belongs to this delta
      DeltaEntrys          D - Entrys in DeltaLog file of all above deltas
  DeltaCache               M - The same content of delta logs
    DeltaInfo * N          M -
      FragmentIndex        M - Entrys in DeltaLog file of each fragment data belongs to this delta
      DeltaEntry           M - Entry in DeltaLog file of this delta
      RowSet               M - May be not exists if DB relaunch
  Fragment * SP              - Level 1
    DeltaLog * N           D -
      Delta * N            D -
        RowSet * SP        D -
          RowData * N      D - Sorted by PK
          RowHash * N      D - Prepared PK hash of each row
        FragmentIndex      D - Entrys in DeltaLog file of each fragment data belongs to this delta
        DeltaEntrys        D - Entrys in DeltaLog file of all above deltas
    DeltaCache             M - The same content of delta logs
      DeltaInfo * N        M -
        FragmentIndex      M - Entrys in DeltaLog file of each fragment data belongs to this delta
        DeltaEntry         M - Entry in DeltaLog file of this delta
        RowSet             M - May be not exists if DB relaunch
    DeltaPatch             D - A modification list against another delta
      Modification * N     D -
        Row                D - Row data
        Offset             D - Position offset of original delta with the same PK

N: arbitrary number
M: in memonry
D: in disk
C: number by config
```

## Compact
* Run in background
* When: `total delta size / partition number of this level > SS`
* Compact to fragments of the same level
* Two types of compaction:
    * Several deltas of similar size, compaction to a new delta, the old ones can be removed
    * If a fregment has a large delta, and the others are small, the small ones can compaction to a DeltaPatch file

## Figues
* Level number = Log SP (`total bytes of current DB` / `partition bytes limit`)
