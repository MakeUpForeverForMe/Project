package com.travel.programApp.hbaseSink

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory}

class HBaseDataWriterFactory(tableName:String,rowkey:String,hbaseFields:String)  extends DataWriterFactory[Row]{
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new HBaseDataWriter(tableName,rowkey,hbaseFields)
  }
}
