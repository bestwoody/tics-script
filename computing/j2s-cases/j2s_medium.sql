create table if not exists j2s_medium (
col_1 Int8,
col_2 Int16,
col_3 Int32,
col_4 Int64,
col_5 String,
col_6 Int8,
col_7 Int16,
col_8 Int32,
col_9 Int64,
col_10 String,
col_11 Int8,
col_12 Int16,
col_13 Int32,
col_14 Int64,
col_15 String,
col_16 Int8,
col_17 Int16,
col_18 Int32,
col_19 Int64,
col_20 String
) engine=MutableMergeTree((col_3, col_4), 8192)