# TheFlash SQL Extensions on Spark 2.3
After moving Spark dependency to 2.3, any operation to theFlash could be done in a Spark SQL or theFlash-extended SQL fashion. One can issue a SQL statement to Thrift Server via either beeline or any JDBC-based client (see document `Using Thrift Server on Spark 2.3` for details).

TheFlash is designed to be able to live with or leverage a legacy Spark cluster, with external database entities (database/table) existing in a legacy catalog (mostly Hive). Most of the SQL extensions are motivated by this ability, so that theFlash is able to resolve catalog entities either in theFlash concrete catalog (theFlash catalog for short) or legacy catalog.

## Living with Legacy Catalog
When living with an existing Spark cluster binding to a legacy external catalog (say Hive), theFlash will resolve database entities in a given **policy**.

There are two catalog policies:
* `legacyfirst` (default)
* `flashfirst`

and can be specified via property:
`spark.flash.catalog_policy`

### Principle of Catalog Policy
The basic idea of catalog policy is that theFlash manages a composition of both theFlash catalog and legacy catalog as a whole, and route the catalog operation to the corresponding underlying catalog based on which **database** this operation is on.

#### <a name="db"></a>Database Operations
When creating a database, we don't know if the database should be created in theFlash or legacy system (say Hive, for instance) without extra supervision in `CREATE DATABASE` statement. Therefore we extended `CREATE DATABASE` statement with keyword `FLASH` as `CREATE FLASH DATABASE` (also explained in [SQL Extensions](#sql)) to declare that the database should be created in theFlash.

When getting/modifying/dropping a database, we search the underlying catalogs in order of that the policy implies (as they are named, `legacyfirst` or `flashfirst`). The first catalog that matches the search will do the actual operation, saying that the database in legacy catalog will hide the one having the same name in theFlash catalog (`legacyfirst`, and vice-versa).

When listing databases, we simply combine the databases in both underlying catalogs.

#### Table Operations
When creating a table, theFlash table can only be created using an extended `CREATE TABLE` statement (see [SQL Extensions](#sql)) therefore we are able to distinguish theFlash table creation and legacy table creation.

When getting/modifying/dropping/resolving a table in a DDL or DML, the actual operation will happen in the catalog that the table's **database** belongs to (as [Database Operations](#db) describes). We can use some examples to demonstrate.
* Example 1

  Catalog policy is `legacyfirst`, database `db` only exists in Hive, table `t` exists in Hive's `db`. Statement `DROP TABLE db.t` will drop Hive table `db.t`.
* Example 2

  Catalog policy is `legacyfirst`, database `db` only exists in theFlash, table `t` exists in theFlash's `db`. Statement `DROP TABLE db.t` will drop theFlash table `db.t`.
* Example 3

  Catalog policy is `legacyfirst`, database `db` exists in both Hive and theFlash, table `t` exists in both Hive and theFlash's `db`. Statement `DROP TABLE db.t` will drop Hive table `db.t`.
* Example 4

  Catalog policy is `legacyfirst`, database `db` exists in both Hive and theFlash, table `t` only exists in theFlash's `db`. Statement `DROP TABLE db.t` will report error `No such table`.

Example 3 & 4 emphasis the description in section `Database Operations`:
> the database in legacy catalog will hide the one having the same name
> in theFlash catalog (`legacyfirst`, and vice-versa).

When listing tables in a specified database, the thing goes the same as above, tables of the given database in the corresponding underlying catalog (not both) will be shown, along with all the temp views in current Spark session (complies Spark's behavior).

**Note** that as a side-effect, `legacyfirst` policy will arbitrarily hide the `default` database in theFlash - Hive always has `default` database, and vice-versa.

#### Temp View Operations
All temp view operations, including creating/getting/modifying/dropping/resolving comply Spark's default behavior.

## <a name="sql"></a>SQL Extensions
We extended Spark SQL in several ways to satisfy theFlash, including database creation, table creation, data loading, etc.

### Create TheFlash Database
Creating theFlash database requires extra keyword `FLASH`, grammar is:

    CREATE FLASH DATABASE [IF NOT EXISTS] <DATABASE_NAME>

### Create TheFlash Table
Creating theFlash table requires extra column properties `PRIMARY KEY` and `NOT NULL` (though most databases other than Spark have them already):

    CREATE TABLE [IF NOT EXISTS] <TAB_NAME> (
    <COL_NAME> <COL_TYPE> [PRIMARY KEY] [NOT NULL],
    ...)
    USING <FLASH_ENGINE>

In which:
* The `PRIMARY KEY` and `NOT NULL` properties are optional, but at least one column should be specified as `PRIMARY KEY`
* `FLASH_ENGINE` are one of the followings:
  * `(MMT | MutableMergeTree) [([PARTITION_NUMBER ,] BUCKET_NUMBER)]`, in which `PARTITION_NUMBER` and `BUCKET_NUMBER` are both optional (but one cannot specify only `PARTITION_NUMBER` without specifying `BUCKET_NUMBER`)
  * Other engines are yet to be supported
* Other part of the `CREATE TABLE` statement follows the Spark SQL grammar

### Create TheFlash Table from TiDB
Users can create theFlash table directly from an existing TiDB table, this is mostly used for creating semi-synchronized table of TiDB:

    CREATE TABLE [IF NOT EXISTS] <TAB_NAME>
    FROM TIDB
    USING <FLASH_ENGINE>

In which, `FLASH_ENGINE` is the same as creating a normal theFlash table.

**Note** that user should make sure that the database of the table being created exists already - the database won't be created along with the table. Further more, it is invalid to create theFlash table under legacy database or vice-versa.

### Load Data from TiDB
Users can load data into theFlash table from the corresponding TiDB table, this is mostly used for loading data from semi-synchronized table of TiDB:

    LOAD DATA FROM TIDB [OVERWRITE] INTO TABLE <TAB_NAME>
