package com.travel.programApp.hbaseSource

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.StructType

class HBaseDataSourceReader(tableName:String,schema:String,cfcc:String) extends DataSourceReader /*with SupportsPushDownFilters with SupportsPushDownRequiredColumns*/{

  val structType: StructType = StructType.fromDDL(schema)

  override def readSchema(): StructType = {
    structType
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {

    import scala.collection.JavaConverters._
    Seq(
    new HBaseDataReaderFactory(tableName,cfcc).asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }

 /* override def pushFilters(filters: Array[Filter]): Array[Filter] = ???

  override def pushedFilters(): Array[Filter] = ???

  override def pruneColumns(requiredSchema: StructType): Unit = ???*/

}
