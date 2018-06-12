## Integration Test
### Usage
1. Prepare your Spark and theFlash cluster environment.
2. You can modify test configuration placed in [clickhouse_config.properties.template](./resources/clickhouse_config.properties.template), copy this template as tidb_config.properties and config this file according to your environment.
3. Run `mvn test` to run all tests.

### Configuration
You can change your test configuration in `clickhouse_config.properties`

You may `cp clickhouse_config.properties.template clickhouse_config.properties` and modify your configurations there

```bash
# ClickHouse address
# ch.addr=127.0.0.1
# ClickHouse port
# ch.port=9000
# TPCH database name, if you already have a tpch database in ClickHouse, specify the db name so that TPCH tests will run on this database
# tpch.db=tpch_test
# Whether to load test data before running tests. If you haven't load CHSpark or TPCH data, set this to true. The next time you run tests, you can set this to false.
# test.data.load=false
```