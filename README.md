# Analysys Reward Calculator

## Process
* We got:
    - `[data]`
        - `201701`
        - `201702`
        - `...`
* Load data, thus we got:
    - `[db]`
        - `201701.data`: sorted data
        - `201701.idx`: sorted data index
        - `...`
* Use db for query

## Data format
```
All
    Partition * N
        DataFile
            Block * N              - align by 512
                MagicFlag          - uint16
                CompressType       - uint16
                RowCount           - uint32, default: 8192
                Row * N
                    Timestamp      - uint32
                    UserId         - uint32
                    EventId        - uint16
                    EventPropsLen  - uint16
                    EventProps     - []byte, json format (TODO: map<k,v>)
                BlockChecksum      - crc32
        IndexFile
            EntryCount             - uint32
            IndexEntry * N
                Timestamp          - uint32
                BlockOffset        - uint32
            IndexChecksum          - uint32, crc
```

