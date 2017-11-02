# Apache Arrow Notes

## Logic Structure
```
Table
  RecordBatch * N                      - util: RecordBatchBuilder, RecordBatch*Writer, RecordBatchSerializer
    Schema
      Field * N
    Column * N
      ChuckedArray                     - or Array
        Array * N                      - immutable
          Length
          Type
          Values
            Buf
              Int | Float .. * N
          NullBitmap
            Buf
```
