drop table if exists default.test
DBGInvoke drop_tidb_table(default, test)

DBGInvoke set_flush_rows(0)
DBGInvoke mock_schema_syncer('true')

DBGInvoke mock_tidb_table(default, test, 'col_1 String, col_2 Int64')

#DBGInvoke put_region(4, 0, 100, default, test)
DBGInvoke region_snapshot(4, 0, 100, default, test)

#DBGInvoke mock_insert_row(default, test, 4, 50, 'Hello world', 666)
DBGInvoke raft_insert_row(default, test, 4, 50, 'Hello world', 666)

select * from default.test

DBGInvoke raft_insert_rows(default, test, 4, 10, 20000, 10, 20)
#DBGInvoke mock_insert_rows(default, test, 4, 10, 20000, 10, 20)

selraw count(*) from default.test

DBGInvoke raft_delete_row(default, test, 4, 50)
selraw count(*) from default.test
select count(*) from default.test
