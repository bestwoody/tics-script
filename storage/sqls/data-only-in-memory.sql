drop table if exists default.test
DBGInvoke drop_tidb_table(default, test)

DBGInvoke set_deadline_seconds(100000)
DBGInvoke set_flush_rows(2000000)

DBGInvoke mock_schema_syncer('true')

DBGInvoke mock_tidb_table(default, test, 'col_0 Int64, col_1 String')

DBGInvoke raft_insert_rows(default, test, 4, 30, 10000, 1, 1)
