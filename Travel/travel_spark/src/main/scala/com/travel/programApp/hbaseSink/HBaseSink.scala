package com.travel.programApp.hbaseSink

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, WriteSupport}
import org.apache.spark.sql.types.StructType

class HBaseSink extends DataSourceV2 with WriteSupport{
  override def createWriter(jobId: String, schema: StructType, mode: SaveMode, options: DataSourceOptions): Optional[DataSourceWriter] = {

    val tableName: String = options.get("hbase.table.name").get()
    val rowkey: String = options.get("hbase.rowkey").get()
    val hbaseFields: String = options.get("hbase.fields").get()
    Optional.of(new HBaseDataSourceWriter(tableName,rowkey,hbaseFields))
  }
}
