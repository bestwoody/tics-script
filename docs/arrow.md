# Apache Arrow Notes

## Logic Structure (C++)
```
Table
  RecordBatch * N                      - util: RecordBatchBuilder, RecordBatch*Writer, RecordBatchSerializer
    Schema
      Field * N
        Name                           - string
        DataType                       - base types and nested types
        Nullable                       - bool
        Metadata
            (Key, Value) * N             - both are string
    Column * N
      ChuckedArray                     - or Array
        Array * N                      - immutable, util: ArrayBuilder, ArrayVisitor
          DataType
          Length
          Data                         - access: *Array->Value(i)
            Buffer
          NullBitmap
            Buffer
```
