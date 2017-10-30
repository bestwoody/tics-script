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
|                            |  Data buffer:
|  Bin log:                  |    Fetch data as quick as possible,
|    File or service         |    buffering data
|                            |
+--^-------------------------+
   |
   |
+--+-------------------------+
|                            |  Magic Writer and Engine can be in the
|  Magic Writer:             |  same process
|    Poll and write data to  |
|    store engine.           |
|    In a specify interval   |  Interval can be changed at runtime
|                            |
+--+-------------------------+
   |
   | Magic API (Write/Scan)
   |
   | (Transaction safe)
   |
   |
   |   +- - - - - - - - - - - - - - - - - - - - - - - -+
   |   |                                                  Different engines
   |      Store Engine:                                |  can use different
   |   |    Persist and index data, no replication        data layout and
   |      Connector:                                   |  indexes, so we
   |   |     Provide Magic API procotal                   keep the ability of
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
   |        |
+--|--------|----------------------------+
|  |        |                            |  Latch Service and Engine can be in
|  |        |   Latch Service:           |  the same process
|  +--------+     Coordinator of reader  |
|  |              an writer(s)           |
|  |                                     |
+--|-------------------------------------+
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
