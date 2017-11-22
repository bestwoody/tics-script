# Relationship of Code bases and modules
```
+---------------------------------------------+
|                                             |
|  Community version modules                  |
|                                             |
|  +-----------------+   +-----------------+  |
|  |  CH             |   |  Spark          |  |
|  +---^-------------+   +-----------------+  |
|      |                                      |
+------|--------------------------------------+
       |
+------|--------------------------------------+
|      |                                      |
|      | Keep syncing                         |
|      |                                      |
|  +---v-----------------------------------+  |
|  |  Patched CH:                          |  |
|  |    Update/delete support              |  |
|  |    Wire SQL layer to Connectors       |  |
|  +---------------------------------------+  |
|                                             |
|  +---------------------------------------+  |
|  |  Magic API Connector:                 |  |
|  |    Magic protocol (write/scan)        |  |
|  |    Arrow format encoding              |  |
|  |    Export API to '.so' or TCP server  |  |
|  +---------------------------------------+  |
|                                             |
|  Repository of CH engine                    |
|  (ch.so or Patched-CH-Server)               |
|                                             |
+---------------------------------------------+

+---------------------------------------------+
|                                             |
|  Common modules for all engines             |
|                                             |
|  +---------------------------------------+  |
|  |  Magic Writer:                        |  |
|  |    Link to engine                     |  |
|  |    Read Binlog                        |  |
|  |    Use Magic API, write to engine     |  |
|  +---------------------------------------+  |
|                                             |
|  +---------------------------------------+  |
|  |  Spark-Magic Connector:               |  |
|  |    Jar package for Spark              |  |
|  |    Link to engine                     |  |
|  |    Wire Spark to Magic protocol       |  |
|  |    Decode Arrow format to RDD         |  |
|  +---------------------------------------+  |
|                                             |
|  +---------------------------------------+  |
|  |  Latch Service:                       |  |
|  |    Can be called by rpc               |  |
|  |    Acquire/release data latches       |  |
|  |    TBD                                |  |
|  +---------------------------------------+  |
|                                             |
+---------------------------------------------+
```
