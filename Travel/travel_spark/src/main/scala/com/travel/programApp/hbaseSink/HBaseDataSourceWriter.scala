package com.travel.programApp.hbaseSink

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}

class HBaseDataSourceWriter(tableName:String,rowkey:String,hbaseFields:String)  extends DataSourceWriter{
  override def createWriterFactory(): DataWriterFactory[Row] = {
    new HBaseDataWriterFactory(tableName,rowkey,hbaseFields)

  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {

  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {

  }
}
