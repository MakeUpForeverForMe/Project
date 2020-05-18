package com.travel.programApp.hbaseSource

import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.DataReader

class HBaseDataReader(tableName:String,cfcc:String) extends DataReader[Row] /*with SupportsPushDownRequiredColumns*/{

  var hbaseConnection:Connection = null

  var resultScanner:ResultScanner = null

  def getIterator:Iterator[Result] = {
    hbaseConnection = HbaseTools.getHbaseConn
    val table: Table = hbaseConnection.getTable(TableName.valueOf(tableName.trim))
    val scan = new Scan()
    val cfccArr: Array[String] = cfcc.split(",")
    for(eachCfcc <- cfccArr){
      val cfAndcc: Array[String] = eachCfcc.split(":")
      scan.addColumn(cfAndcc(0).trim.getBytes(),cfAndcc(1).trim.getBytes())
    }
    val scanner: ResultScanner = table.getScanner(scan)
    import scala.collection.JavaConverters._
    scanner.iterator().asScala
  }


  val datas:Iterator[Result] = getIterator


  override def next(): Boolean = {
    datas.hasNext
  }

  override def get(): Row = {
    val result: Result = datas.next()
    val strings: Array[String] = cfcc.split(",").map(eachCfcc => {
      val strings: Array[String] = eachCfcc.trim.split(":")
      Bytes.toString(result.getValue(strings(0).trim.getBytes(), strings(1).trim.getBytes()))
    })
    Row.fromSeq(strings)
  }
  override def close(): Unit = {
    hbaseConnection.close()
  }

}
