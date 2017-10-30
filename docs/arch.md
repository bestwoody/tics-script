```
+----------------+         +--------------+         +----------------+
|                |         |              |         |                |
|  TiDB Service  +--------->  PD Service  <--------->  TiKV Service  |
|                |         |              |         |                |
|                |         +--------------+         |                |
|                |                                  |                |
|                +---------------------------------->                |
+-------+--------+           Write/Query            +----------------+
        |
        |
+-------v--------------------+
|                            |  Data buffer:
|  Bin log:                  |    Fetch data as quick as possible,
|    File or service         |    buffering data
|                            |
+-------^--------------------+
        |
        |
+----------------------------+
|                            |  Magic Writer and Engine can be in the
|  Magic Writer:             |  same process
|    Poll and write data to  |
|    store engine.           |
|    In a specify interval   |  Interval can be changed at runtime
|                            |
+-------+--------------------+
        |
        | Magic API (Write/Scan)
        |
        | (Transaction safe)
        |
        |
+ - - - v - - - - - - - - - - - - - - - - - - - +
|                                                  Different engines can
   Store Engine:                                |  use different data
|    Persist and index data, no replication        layout and indexes,
   Connector:                                   |  so we keep the ability
|    Provide Magic API procotal                    of switch engines
     Provide Caculator Engine procotal          |
|
   Engine and Connector can be in the           |
|  same process, or not
                                                |
|  +------------------+  +-------------------+
   |                  |  |                   |  |
|  |  Magic Connector |  |  CH Connector     |
   |                  |  |                   |  |
|  +------------------+  +-------------------+
            |                     |             |
|  +------------------+  +-------------------+
   |                  |  |                   |  |
|  |  Magic Engine    |  |  CH Engine        |
   |                  |  |                   |  |
|  +------------------+  +-------------------+
                                                |
+ - - - ^ - - - - - - - - - - - - - - - - - - - +
        |
        |
        | Caculator Engine protocol, eg: Spark RDD
        |
        |
+ - - - + - - - - - - - - - - - - - - - - - - - +
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
