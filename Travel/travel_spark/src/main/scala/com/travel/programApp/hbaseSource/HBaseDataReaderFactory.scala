package com.travel.programApp.hbaseSource

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

class HBaseDataReaderFactory(tableName:String,cfcc:String) extends DataReaderFactory[Row]{
  override def createDataReader(): DataReader[Row] = {


    new HBaseDataReader(tableName,cfcc)


  }
}
