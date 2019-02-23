drop table if exists default.test
DBGInvoke drop_tidb_table(default, test)

DBGInvoke set_flush_rows(0)
DBGInvoke mock_schema_syncer('true')

DBGInvoke mock_tidb_table(default, test, 'col_0 Int64, col_1 Int32, col_2 Int16, col_3 Int8, col_4 String, col_5 UInt64, col_6 UInt32, col_7 UInt16, col_8 UInt8, col_9 Float64, col_10 Float32')

DBGInvoke raft_insert_rows(default, test, 4, 10, 20000, 20, 20)
