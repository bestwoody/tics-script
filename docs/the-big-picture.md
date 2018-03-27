## The big picture
```
+----------------+         +--------------+         +----------------+
|                |         |              |         |                |
|  TiDB Service  +--------->  PD Service  <--------->  TiKV Service  |
|                |         |              |         |                |
|                |         +--------------+         |                |
|                |                                  |                |
|                +---------------------------------->                |
+--+-------------+           Write/Query            +----------------+
   |
   |
+--v-------------------------+
|                            |
|  Binlog:                   |
|    File or service         |
|                            |
+--^-------------------------+
   |
   |
+--+-------------------------+
|                            |  TheFlash Writer and Engine can be in the
|  TheFlash Writer:          |  same process
|    Poll and write data to  |
|    store engine.           |
|    In a specify interval   |  Interval can be changed at runtime
|                            |
+--+-------------------------+
   |
   | TheFlash API (Write/Scan)
   |
   | (Transaction safe)
   |
   |
   |   +- - - - - - - - - - - - - - - - - - - - - - - -+
   |   |                                                  Different engines
   |      Store Engine:                                |  can use different
   |   |    Persist and index data, no replication        data layout and
   |      Connector:                                   |  indexes, so we
   |   |     Provide TheFlash API procotal                keep the ability of
   |         Provide Caculator Engine procotal         |  switching engines
   |   |
   |      Engine and Connector can be in the           |
   |   |  same process, or not
   |                                                   |
   |   |  +------------------+  +-------------------+
   |      |                  |  |                   |  |
   |   |  |  Magic Connector |  |  CH Connector     |
   |      |                  |  |                   |  |
   |   |  +--------+---------+  +--------+----------+
   |               |                     |             |
   |   |  +--------+---------+  +--------+----------+
   |      |                  |  |                   |  |
   |   |  |  Magic Engine    |  |  CH Engine        |
   |      |                  |  |                   |  |
   |   |  +------------------+  +-------------------+
   |                                                   |
   |   +- - ^ - - - - - - - - - - - - - - - - - - - - -+
   |        |
   |        |
   +--------+
   |
   |
   | Caculator Engine protocol, eg: Spark RDD
   |
   |
+- + - - - - - - - - - - - - - - - - - - - - - -+
|                                               |
   Caculator Cluster:
|                                               |
   +------------------+  +------------------+
|  |                  |  |                  |   |
   |  Spark Cluster   |  |  Impala Cluster  |
|  |                  |  |                  |   |
   +------------------+  +------------------+
|                                               |
+ - - - - - - - - - - - - - - - - - - - - - - - +


-->: Calling direction

```
