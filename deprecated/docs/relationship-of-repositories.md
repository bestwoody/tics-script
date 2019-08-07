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
|  |  TheFlash API Connector:              |  |
|  |    TheFlash protocol (write/scan)     |  |
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
|  |  TheFlash Writer:                     |  |
|  |    Link to engine                     |  |
|  |    Read Binlog                        |  |
|  |    Use TheFlash API, write to engine  |  |
|  +---------------------------------------+  |
|                                             |
|  +---------------------------------------+  |
|  |  Spark-CH Connector:                  |  |
|  |    Jar package for Spark              |  |
|  |    Link to engine                     |  |
|  |    Wire Spark to TheFlash protocol    |  |
|  |    Decode Arrow format to RDD         |  |
|  +---------------------------------------+  |
|                                             |
+---------------------------------------------+
```
