# Apache Arrow Notes

## Logic Structure

### C++
```
Table
  RecordBatch * N              - util: RecordBatchBuilder, RecordBatch*Writer, RecordBatchSerializer
    Schema
      Metadata
        KeyValue * N           - both are string
      Field * N
        Name                   - string
        DataType               - base types and nested types
        Nullable               - bool
        Metadata
        Field * N              - nested children
    Column * N
      ChuckedArray             - or Array
        Array * N              - immutable, util: ArrayBuilder, ArrayVisitor
          DataType
          Length
          Data                 - access: *Array->Value(i)
          NullBitmap
```

### Java
```
VectorSchemaRoot               - load from ipc message: VectorLoader
  Schema
    Metadata
      KeyValue * N             - both are string
    Field * N
      Name                     - string
      FieldType
        ArrowType              - base types and nested types
        Nullable               - bool
        Metadata
      Field * N                - nested children
  FieldVector * N
    ValueVector                - access: *Vector.getAccessor().get(i)
```
