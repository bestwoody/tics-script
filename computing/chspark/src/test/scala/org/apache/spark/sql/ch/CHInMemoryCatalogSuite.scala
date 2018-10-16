package org.apache.spark.sql.ch

import org.apache.spark.sql._
import org.apache.spark.util.Utils

class CHInMemoryCatalogSuite extends BaseCHCatalogSuite {
  override var extended: SparkSession = _
  override val testDb = "in_memory_test_test_test_db"
  override val testT = "in_memory_test_test_test_t"

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // Used for Hive external catalog, which is the default.
    System.setProperty("test.tmp.dir", Utils.createTempDir().toURI.getPath)
    System.setProperty("test.warehouse.dir", Utils.createTempDir().toURI.getPath)

    extended = CHExtendedSparkSessionBuilder
      .builder()
      .withCHFirstPolicy()
      .withInMemoryCH()
      .withInMemoryLegacyCatalog()
      .getOrCreate()

    extended.sparkContext.setLogLevel("WARN")

    init()
  }

  override protected def afterAll(): Unit = {
    cleanUp()

    extended.stop()

    super.afterAll()
  }

  override def verifyShowDatabases(expected: Array[String]) = {
    var r = extended.sql("show databases").collect().map(_.getString(0))
    assert(r.sorted(Ordering.String).deep == expected.sorted(Ordering.String).deep)
    r = extended.sql("""show databases ".efault*"""").collect().map(_.getString(0))
    assert(r.sorted(Ordering.String).deep == Array("default").sorted(Ordering.String).deep)
    r = extended.sql(s"""show databases "^((?!default).)*"""").collect().map(_.getString(0))
    assert(
      r.sorted(Ordering.String)
        .deep == expected.filterNot(_ == "default").sorted(Ordering.String).deep
    )
  }

  override def verifyShowTables(db: String,
                                expected: Array[String],
                                otherDb: String,
                                otherExpected: Array[String]) = {
    var r = extended.sql("show tables").collect().map(_.getString(1))
    assert(r.sorted(Ordering.String).deep == expected.sorted(Ordering.String).deep)
    r = extended.sql(s"show tables from $otherDb").collect().map(_.getString(1))
    assert(r.sorted(Ordering.String).deep == otherExpected.sorted(Ordering.String).deep)
  }

  override def verifyDescTable(table: String, expected: Array[Array[String]]) = {
    val rNoExtended = extended
      .sql(s"desc $table")
      .collect()
      .map(row => Array(row.getString(0), row.getString(1), row.getString(2)))
    val eNoExtended = expected.zipWithIndex.filter(_._2 < expected.length - 2).map(_._1)
    assert(rNoExtended.zip(eNoExtended).forall(p => p._1.deep == p._2.deep))
    val r = extended
      .sql(s"desc extended $table")
      .collect()
      .map(row => Array(row.getString(0), row.getString(1), row.getString(2)))
      .filterNot(a => a(0).startsWith("#") || a(0).isEmpty)
    assert(r.length == expected.length && r.zip(expected).forall(p => p._1.deep == p._2.deep))
  }

  test("databases") {
    runDatabaseTest()
  }

  test("tables") {
    runTableTest()
  }

  test("explains") {
    runExplainTest()
  }

  test("inserts") {
    runInsertTest()
  }

  test("caches") {
    runCacheTest()
  }

  test("queries") {
    runQueryTest()
  }

  test("with-as-es") {
    runWithAsTest()
  }

  test("temp views") {
    runTempViewTest()
  }
}
