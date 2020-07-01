package com.mine

import org.apache.spark.sql.SparkSession

/**
  * @author ximing.wei
  */
object ReadCsvWriteParquet {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().master("local[*]")
      .appName(this.getClass.getSimpleName.filter(!_.equals('$')))
      .getOrCreate()
    session
      .read
      .option("header", value = true)
      .csv("D:\\Users\\ximing.wei\\Desktop\\query-impala-25542.csv")
      .repartition(1)
      .write
      .option("header", value = false)
      .parquet("D:\\Users\\ximing.wei\\Desktop\\query.parquet")
  }
}
