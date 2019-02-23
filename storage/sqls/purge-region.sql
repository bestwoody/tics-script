drop table if exists default.test
DBGInvoke drop_tidb_table(default, test)

DBGInvoke set_flush_rows(1)

DBGInvoke mock_tidb_table(default, test, 'col_1 Int64, col_2 String')
DBGInvoke put_region(4, 0, 100, default, test)

DBGInvoke raft_insert_row(default, test, 3, 1, 'Hello')
selraw * from default.test
select * from default.test

DBGInvoke rm_region_data(4)
selraw * from default.test
select * from default.test
