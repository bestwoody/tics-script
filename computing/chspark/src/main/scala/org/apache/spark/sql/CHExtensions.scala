package org.apache.spark.sql

import org.apache.spark.sql.ch.CHStrategy
import org.apache.spark.sql.extensions.{CHDDLRule, CHParser, CHResolutionRule}

class CHExtensions extends (SparkSessionExtensions => Unit) {
  var chContext: CHContext = _

  def getOrCreateCHContext(sparkSession: SparkSession): CHContext = {
    if (chContext == null) {
      chContext = new CHContext(sparkSession)
    }
    chContext
  }

  override def apply(e: SparkSessionExtensions): Unit = {
    e.injectParser(CHParser(getOrCreateCHContext))
    e.injectResolutionRule(CHDDLRule(getOrCreateCHContext))
    e.injectResolutionRule(CHResolutionRule(getOrCreateCHContext))
    e.injectPlannerStrategy(CHStrategy(getOrCreateCHContext))
  }
}
