# Using Thrift Server on Spark 2.3
TheFlash can be used through Thrift Server supplied in Spark 2.3 directly. Any operation to TheFlash could be done in a Spark SQL or our extended SQL (see document `TheFlash SQL Extensions on Spark 2.3` for details) fashion.

Thrift Server can be started using the following command:

    <SPARK_DIR>/sbin/start-thriftserver.sh

And stopped  by:

    <SPARK_DIR>/sbin/stop-thriftserver.sh

Then user can use Spark-supplied beeline or any JDBC-based client to connect. Beeline can be started using the following command:

    <SPARK_DIR>/bin/beeline -u 127.0.0.1

**Important**: before running, make sure you have properties `spark.sql.extensions` and `spark.driver.extraClassPath` set properly in `spark-defaults.conf`.
