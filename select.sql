set @@tidb_isolation_read_engines='tiflash';
select count(*) from lineitem;
