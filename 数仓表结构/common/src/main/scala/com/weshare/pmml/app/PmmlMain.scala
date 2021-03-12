package com.weshare.pmml.app

import java.util.UUID

import org.apache.spark.sql.{ SparkSession}

import scala.util.Random

/**
 * created by chao.guo on 2021/2/25
 **/
object PmmlMain {
  def main(args: Array[String]): Unit = {
    val batch_date = args(0)
    val tablename=args(1)
    val partition_key=args(2)
    val spark = SparkSession.builder()
      //.master("local[*]")
      //.master("yarn")
      .config("log4j.rootLogger","error")
     /* .config("hive.metastore.uris","thrift://10.83.0.47:9083")*/
      .appName("PmmlMain"+UUID.randomUUID())
      .enableHiveSupport()
      .getOrCreate()
      RunJob.runJob(spark,batch_date,tablename,partition_key)
      spark.close()
  }
}
