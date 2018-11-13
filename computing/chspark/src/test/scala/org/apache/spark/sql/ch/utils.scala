package org.apache.spark.sql.ch

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.{CHContext, SparkSession, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.extensions.{CHDDLRule, CHParser, CHResolutionRule}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable
import scala.collection.JavaConversions._

case class CHInMemoryRelation(sparkSession: SparkSession,
                              tableIdentifier: TableIdentifier,
                              schema: StructType)
    extends BaseRelation
    with InsertableRelation {
  def data: DataFrame =
    CHInMemoryRelation.dataRegistry
      .getOrElse(tableIdentifier, sparkSession.createDataFrame(List.empty[Row], schema))

  override def sizeInBytes: Long =
    data.count() * 4

  override def insert(df: DataFrame, overwrite: Boolean): Unit =
    CHInMemoryRelation.dataRegistry(tableIdentifier) = if (overwrite) {
      df
    } else {
      df.collect()
      data.union(df)
    }

  override def sqlContext: SQLContext = sparkSession.sqlContext
}

object CHInMemoryRelation {
  val dataRegistry: mutable.Map[TableIdentifier, DataFrame] =
    mutable.Map[TableIdentifier, DataFrame]()
}

class CHInMemoryExternalCatalog(chContext: CHContext)
    extends InMemoryCatalog
    with CHExternalCatalog {
  override protected def doCreateFlashDatabase(databaseDesc: CatalogDatabase,
                                               ignoreIfExists: Boolean): Unit =
    doCreateDatabase(databaseDesc, ignoreIfExists)

  override protected def doCreateFlashTable(tableDesc: CatalogTable,
                                            query: Option[LogicalPlan],
                                            ignoreIfExists: Boolean): Unit =
    if (query.nonEmpty) {
      val df = Dataset.ofRows(chContext.sparkSession, query.get)
      val schema = df.schema
      doCreateTable(tableDesc.copy(schema = schema), ignoreIfExists)
      val chRelation = CHInMemoryRelation(chContext.sparkSession, tableDesc.identifier, schema)
      chRelation.insert(Dataset.ofRows(chContext.sparkSession, query.get), true)
    } else {
      doCreateTable(tableDesc, ignoreIfExists)
    }

  override protected def doCreateFlashTableFromTiDB(database: String,
                                                    tiTableInfo: TiTableInfo,
                                                    engine: CHEngine,
                                                    ignoreIfExists: Boolean): Unit = ???

  override def loadTableFromTiDB(db: String, tiTable: TiTableInfo, isOverwrite: Boolean): Unit = ???
}

class CHResolutionRuleWithInMemoryRelation(getOrCreateCHContext: SparkSession => CHContext)(
  sparkSession: SparkSession
) extends CHResolutionRule(getOrCreateCHContext)(sparkSession) {
  override val resolveRelation: TableIdentifier => LogicalPlan =
    (tableIdentifier: TableIdentifier) => {
      val catalogTable = chContext.chCatalog.getTableMetadata(tableIdentifier)
      val alias = formatTableName(tableIdentifier.table)
      SubqueryAlias(
        alias,
        LogicalRelation(new CHInMemoryRelation(sparkSession, tableIdentifier, catalogTable.schema))
      )
    }

  override def apply(plan: LogicalPlan): LogicalPlan = super.apply(plan).transformUp {
    case LogicalRelation(r, _, _, _) if r.isInstanceOf[CHInMemoryRelation] =>
      r.asInstanceOf[CHInMemoryRelation].data.logicalPlan
  }
}

object CHResolutionRuleWithInMemoryRelation {
  def apply(getOrCreateCHContext: SparkSession => CHContext): SparkSession => CHResolutionRule = {
    sparkSession: SparkSession =>
      new CHResolutionRuleWithInMemoryRelation(getOrCreateCHContext)(sparkSession)
  }
}

class CHTestContext(sparkSession: SparkSession, inMemory: Boolean) extends CHContext(sparkSession) {
  override lazy val chConcreteCatalog: CHSessionCatalog =
    new CHConcreteSessionCatalog(this)(
      if (inMemory) new CHInMemoryExternalCatalog(this) else new CHDirectExternalCatalog(this)
    )
}

class CHTestExtensions(inMemory: Boolean) extends CHExtensions {
  override def getOrCreateCHContext(sparkSession: SparkSession): CHContext = {
    if (chContext == null) {
      chContext = new CHTestContext(sparkSession, inMemory)
    }
    chContext
  }

  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(CHParser(getOrCreateCHContext))
    e.injectResolutionRule(CHDDLRule(getOrCreateCHContext))
    if (inMemory) {
      e.injectResolutionRule(CHResolutionRuleWithInMemoryRelation(getOrCreateCHContext))
    } else {
      e.injectResolutionRule(CHResolutionRule(getOrCreateCHContext))
    }
    e.injectPlannerStrategy(CHStrategy(getOrCreateCHContext))
  }
}

class CHExtendedSparkSessionBuilder {
  var root: SparkSession.Builder = SparkSession.builder().master("local[1]")

  var inMemory = false

  def withLegacyFirstPolicy(): CHExtendedSparkSessionBuilder = {
    root = root
      .config(CHConfigConst.CATALOG_POLICY, "legacyfirst")
    this
  }

  def withCHFirstPolicy(): CHExtendedSparkSessionBuilder = {
    root = root
      .config(CHConfigConst.CATALOG_POLICY, "flashfirst")
    this
  }

  def withInMemoryCH(): CHExtendedSparkSessionBuilder = {
    inMemory = true
    this
  }

  def withHiveLegacyCatalog(): CHExtendedSparkSessionBuilder = {
    root = root
      .config(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
      .config(StaticSQLConf.WAREHOUSE_PATH.key, "/tmp/warehouse")
    this
  }

  def withInMemoryLegacyCatalog(): CHExtendedSparkSessionBuilder = {
    root = root
      .config(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "in-memory")
      .config(StaticSQLConf.WAREHOUSE_PATH.key, "/tmp/warehouse")
    this
  }

  def getOrCreate(): SparkSession =
    root.withExtensions(new CHTestExtensions(inMemory)).getOrCreate()
}

object CHExtendedSparkSessionBuilder {
  def builder(): CHExtendedSparkSessionBuilder = new CHExtendedSparkSessionBuilder()
}
