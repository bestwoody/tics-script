#!/bin/bash

source "./_env_build.sh"

pd_addrs_tiflash=${pd_addrs//,/;}

cat<<EOF>./$config/config.xml
<?xml version="1.0"?>
<yandex>
    <application>
        <runAsDaemon>true</runAsDaemon>
    </application>

    <logger>
        <level>${tiflash_log_level}</level>
        <log>${deploy_dir}${data}theflash/server.log</log>
        <errorlog>${deploy_dir}${data}theflash/error.log</errorlog>
        <size>1000M</size>
        <count>10</count>
        <!-- <console>1</console> --> <!-- Default behavior is autodetection (log to console if not daemon mode and is tty) -->
    </logger>

    <display_name>the flash</display_name>

    <tidb>
        <service_ip>${ip}</service_ip>
        <status_port>${tidb_status_port}</status_port>
        <ignore_databases>${ignore_databases}</ignore_databases>
    </tidb>

    <http_port>${tiflash_http_port}</http_port>
    <tcp_port>${tiflash_tcp_port}</tcp_port>
    <interserver_http_port>9909</interserver_http_port>
    <!--listen_host>::1</listen_host-->
    <listen_host>0.0.0.0</listen_host>

    <raft>
        <service_addr>0.0.0.0:${raft_port}</service_addr>
        <kvstore_path>${deploy_dir}${data}theflash/kvstore</kvstore_path>
        <pd_addr>${pd_addrs_tiflash}</pd_addr>
    </raft>

    <!-- Don't exit if ipv6 or ipv4 unavailable, but listen_host with this protocol specified -->
    <!-- <listen_try>0</listen_try> -->

    <!-- Allow listen on same address:port -->
    <!-- <listen_reuse_port>0</listen_reuse_port> -->

    <!-- <listen_backlog>64</listen_backlog> -->

    <max_connections>4096</max_connections>
    <keep_alive_timeout>3</keep_alive_timeout>

    <!-- Maximum number of concurrent queries. -->
    <max_concurrent_queries>100</max_concurrent_queries>

    <!-- Set limit on number of open files (default: maximum). This setting makes sense on Mac OS X because getrlimit() fails to retrieve
         correct maximum value. -->
    <!-- <max_open_files>262144</max_open_files> -->

    <!-- Size of cache of uncompressed blocks of data, used in tables of MergeTree family.
         In bytes. Cache is single for server. Memory is allocated only on demand.
         Cache is used when 'use_uncompressed_cache' user setting turned on (off by default).
         Uncompressed cache is advantageous only for very short queries and in rare cases.
      -->
    <uncompressed_cache_size>3589934592</uncompressed_cache_size>

    <!-- Approximate size of mark cache, used in tables of MergeTree family.
         In bytes. Cache is single for server. Memory is allocated only on demand.
         You should not lower this value.
      -->
    <mark_cache_size>5368709120</mark_cache_size>

    <!-- Path of persisted mapping cache in fast(er) disk device.
         Empty means disabled.
         Seperated with ';' if there are more than one path.
      -->
    <persisted_mapping_cache_path></persisted_mapping_cache_path>
    <!-- Quota size in bytes of persisted mapping cache, in bytes. 0 means unlimited. -->
    <persisted_mapping_cache_size>0</persisted_mapping_cache_size>

    <!-- Path to data directory, with trailing slash. -->
    <path>${deploy_dir}${data}theflash/data</path>

    <!-- Path to temporary data for processing hard queries. -->
    <tmp_path>${deploy_dir}${data}theflash/tmp</tmp_path>

    <l0_optimize>false</l0_optimize>

    <!-- Path to configuration file with users, access rights, profiles of settings, quotas. -->
    <users_config>users.xml</users_config>

    <!-- Default profile of settings. -->
    <default_profile>default</default_profile>

    <!-- System profile of settings. This settings are used by internal processes (Buffer storage, Distibuted DDL worker and so on). -->
    <!-- <system_profile>default</system_profile> -->

    <!-- Default database. -->
    <default_database>default</default_database>

    <!-- Server time zone could be set here.

         Time zone is used when converting between String and DateTime types,
          when printing DateTime in text formats and parsing DateTime from text,
          it is used in date and time related functions, if specific time zone was not passed as an argument.

         Time zone is specified as identifier from IANA time zone database, like UTC or Africa/Abidjan.
         If not specified, system time zone at server startup is used.

         Please note, that server could display time zone alias instead of specified name.
         Example: W-SU is an alias for Europe/Moscow and Zulu is an alias for UTC.
    -->
    <!-- <timezone></timezone> -->

    <!-- You can specify umask here (see "man umask"). Server will apply it on startup.
         Number is always parsed as octal. Default umask is 027 (other users cannot read logs, data files, etc; group can only read).
    -->
    <!-- <umask>022</umask> -->

    <!-- Reloading interval for embedded dictionaries, in seconds. Default: 3600. -->
    <builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>

    <!-- Maximum session timeout, in seconds. Default: 3600. -->
    <max_session_timeout>3600</max_session_timeout>

    <!-- Default session timeout, in seconds. Default: 60. -->
    <default_session_timeout>600</default_session_timeout>

    <!-- Query log. Used only for queries with setting log_queries = 1. -->
    <query_log>
        <!-- What table to insert data. If table is not exist, it will be created.
             When query log structure is changed after system update,
              then old table will be renamed and new table will be created automatically.
        -->
        <database>system</database>
        <table>query_log</table>
        <!--
            PARTITION BY expr https://clickhouse.yandex/docs/en/table_engines/custom_partitioning_key/
            Example:
                event_date
                toMonday(event_date)
                toYYYYMM(event_date)
                toStartOfHour(event_time)
        -->
        <partition_by>toYYYYMM(event_date)</partition_by>
        <!-- Interval of flushing data. -->
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </query_log>

    <!-- Uncomment if use part_log
    <part_log>
        <database>system</database>
        <table>part_log</table>

        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    </part_log>
    -->

    <dictionaries_config>*_dictionary.xml</dictionaries_config>

    <!-- Uncomment if you want data to be compressed 30-100% better.
         Don't do that if you just started using ClickHouse.
      -->
    <compression>
    <!--
        <!- - Set of variants. Checked in order. Last matching case wins. If nothing matches, lz4 will be used. - ->
        <case>

            <!- - Conditions. All must be satisfied. Some conditions may be omitted. - ->
            <min_part_size>10000000000</min_part_size>        <!- - Min part size in bytes. - ->
            <min_part_size_ratio>0.01</min_part_size_ratio>   <!- - Min size of part relative to whole table size. - ->

            <!- - What compression method to use. - ->
            <method>zstd</method>
        </case>
    -->
    </compression>

    <merge_tree>
        <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
        <mutable_mergetree_partition_number>40</mutable_mergetree_partition_number>
    </merge_tree>

    <!-- Protection from accidental DROP.
         If size of a MergeTree table is greater than max_table_size_to_drop (in bytes) than table could not be dropped with any DROP query.
         If you want do delete one table and don't want to restart clickhouse-server, you could create special file <clickhouse-path>/flags/force_drop_table and make DROP once.
         By default max_table_size_to_drop is 50GB, max_table_size_to_drop=0 allows to DROP any tables.
         Uncomment to disable protection.
    -->
    <max_table_size_to_drop>0</max_table_size_to_drop>
</yandex>
EOF
