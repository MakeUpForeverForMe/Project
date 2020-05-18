package com.travel.programApp.hbaseSource

import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class HBaseSource extends DataSourceV2 with ReadSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = {
    val tableName: String = options.get("hbase_table_name").get()
    val schema: String = options.get("sparksql_table_schema").get()
    val cfcc: String = options.get("hbase_table_schema").get()

    new HBaseDataSourceReader(tableName,schema,cfcc)
  }
}
