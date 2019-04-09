package org.apache.spark.sql.ch

import com.pingcap.tikv.region.TiRegion

case class CHRegionPartitionRef(table: CHTableRef, region: Array[TiRegion]) {}
