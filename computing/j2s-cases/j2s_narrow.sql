create table if not exists j2s_narrow (
col_1 Int8,
col_2 Int16,
col_3 Int32,
col_4 Int64,
col_5 String
) engine=MutableMergeTree((col_4), 8192)
