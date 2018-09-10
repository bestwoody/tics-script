package org.apache.spark.sql.ch

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.sql.{CHContext, SparkSession, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.extensions.{CHDDLRule, CHParser, CHResolutionRule}
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class CHInMemoryExternalCatalog(chContext: CHContext)
    extends InMemoryCatalog
    with CHExternalCatalog {
  override protected def doCreateCHDatabase(databaseDesc: CatalogDatabase,
                                            ignoreIfExists: Boolean): Unit =
    doCreateDatabase(databaseDesc, ignoreIfExists)

  override protected def doCreateCHTable(tableDesc: CatalogTable, ignoreIfExists: Boolean): Unit =
    doCreateTable(tableDesc, ignoreIfExists)

  override protected def doCreateTableFromTiDB(database: String,
                                               tiTableInfo: TiTableInfo,
                                               engine: CHEngine,
                                               ignoreIfExists: Boolean): Unit = ???

  override def loadTableFromTiDB(db: String, tiTable: TiTableInfo, isOverwrite: Boolean): Unit = ???
}

case class CHInMemoryRelation(tableIdentifier: TableIdentifier, _schema: StructType)
    extends CHRelation(Array.empty[CHTableRef], 0)(null)
    with InsertableRelation {
  def data: Option[DataFrame] =
    CHInMemoryRelation.dataRegistry.get(tableIdentifier)

  override lazy val schema: StructType = _schema

  override def sizeInBytes: Long =
    if (data.isEmpty) {
      0
    } else {
      data.get.count() * 4
    }

  override def insert(df: DataFrame, overwrite: Boolean): Unit =
    CHInMemoryRelation.dataRegistry(tableIdentifier) = if (overwrite || data.isEmpty) {
      df
    } else {
      data.get.union(df)
    }
}

object CHInMemoryRelation {
  val dataRegistry: mutable.Map[TableIdentifier, DataFrame] =
    mutable.Map[TableIdentifier, DataFrame]()
}

class CHResolutionRuleWithInMemoryRelation(getOrCreateCHContext: SparkSession => CHContext)(
  sparkSession: SparkSession
) extends CHResolutionRule(getOrCreateCHContext)(sparkSession) {
  override val resolveCHRelation: TableIdentifier => CHInMemoryRelation =
    (tableIdentifier: TableIdentifier) => {
      val catalogTable = chContext.chCatalog.getTableMetadata(tableIdentifier)
      CHInMemoryRelation(tableIdentifier, catalogTable.schema)
    }

  override def apply(plan: LogicalPlan): LogicalPlan = super.apply(plan).transformUp {
    case LogicalRelation(r @ CHInMemoryRelation(_, _), output, _, _) =>
      r.data.get.logicalPlan
  }
}

object CHResolutionRuleWithInMemoryRelation {
  def apply(getOrCreateCHContext: SparkSession => CHContext): SparkSession => CHResolutionRule = {
    sparkSession: SparkSession =>
      new CHResolutionRuleWithInMemoryRelation(getOrCreateCHContext)(sparkSession)
  }
}

class CHTestContext(sparkSession: SparkSession,
                    sessionCatalog: CHContext => CHExternalCatalog => CHSessionCatalog,
                    externalCatalog: CHContext => CHExternalCatalog)
    extends CHContext(sparkSession) {
  override lazy val chConcreteCatalog: CHSessionCatalog =
    new CHConcreteSessionCatalog(this)(externalCatalog(this))
  override lazy val chCatalog: CHSessionCatalog =
    sessionCatalog(this)(chConcreteCatalog.externalCatalog.asInstanceOf[CHExternalCatalog])
}

class CHTestExtensions(sessionCatalog: CHContext => CHExternalCatalog => CHSessionCatalog,
                       externalCatalog: CHContext => CHExternalCatalog,
                       rule: (SparkSession => CHContext) => (SparkSession => CHResolutionRule))
    extends CHExtensions {
  override def getOrCreateCHContext(sparkSession: SparkSession): CHContext = {
    if (chContext == null) {
      chContext = new CHTestContext(sparkSession, sessionCatalog, externalCatalog)
    }
    chContext
  }

  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(CHParser(getOrCreateCHContext))
    e.injectResolutionRule(CHDDLRule(getOrCreateCHContext))
    e.injectResolutionRule(rule(getOrCreateCHContext))
    e.injectPlannerStrategy(CHStrategy(getOrCreateCHContext))
  }
}

class CHExtendedSparkSessionBuilder {
  var root: SparkSession.Builder = SparkSession.builder().master("local[1]")

  var sessionCatalog: CHContext => CHExternalCatalog => CHSessionCatalog = _
  var externalCatalog: CHContext => CHExternalCatalog = _
  var rule: (SparkSession => CHContext) => (SparkSession => CHResolutionRule) =
    (f: SparkSession => CHContext) => CHResolutionRule(f)

  def withConcreteSessionCatalog(): CHExtendedSparkSessionBuilder = {
    sessionCatalog = (chContext: CHContext) =>
      (externalCatalog: CHExternalCatalog) =>
        new CHConcreteSessionCatalog(chContext)(externalCatalog)
    this
  }

  def withCompositeSessionCatalog(): CHExtendedSparkSessionBuilder = {
    sessionCatalog = (chContext: CHContext) => _ => new CHCompositeSessionCatalog(chContext)
    this
  }

  def withDirectExternalCatalog(): CHExtendedSparkSessionBuilder = {
    externalCatalog = (chContext: CHContext) => new CHDirectExternalCatalog(chContext)
    this
  }

  def withInMemoryExternalCatalog(): CHExtendedSparkSessionBuilder = {
    externalCatalog = (chContext: CHContext) => new CHInMemoryExternalCatalog(chContext)
    rule = (f: SparkSession => CHContext) => CHResolutionRuleWithInMemoryRelation(f)
    this
  }

  def withHiveLegacyCatalog(): CHExtendedSparkSessionBuilder = {
    root = root
      .config(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
      .config(StaticSQLConf.WAREHOUSE_PATH.key, "/data/warehouse")
    this
  }

  def withInMemoryLegacyCatalog(): CHExtendedSparkSessionBuilder = {
    root = root
      .config(StaticSQLConf.CATALOG_IMPLEMENTATION.key, "in-memory")
      .config(StaticSQLConf.WAREHOUSE_PATH.key, "/data/warehouse")
    this
  }

  def getOrCreate(): SparkSession =
    root.withExtensions(new CHTestExtensions(sessionCatalog, externalCatalog, rule)).getOrCreate()
}

object CHExtendedSparkSessionBuilder {
  def builder(): CHExtendedSparkSessionBuilder = new CHExtendedSparkSessionBuilder()
}
