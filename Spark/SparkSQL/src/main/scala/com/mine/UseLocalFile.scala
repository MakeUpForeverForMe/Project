package com.mine

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object UseLocalFile {

  Logger.getRootLogger.setLevel(Level.WARN)

  private val path = "hdfs://mycluster:8020/tmp"

  private val df_my_path = "D:\\Users\\ximing.wei\\Desktop\\cc\\app_662.csv"
  private val df_td_path = "D:\\Users\\ximing.wei\\Desktop\\cc\\11.txt"

  private val session = SparkSession
    .builder()
    .appName(this.getClass.getSimpleName.filter(!_.equals('$')))
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", 1000)
    .config("spark.default.parallelism", 1000)
    .config("spark.cores.max", 8)
    .config("spark.executor.cores", 1)
    .config("spark.executor.memory", "1500M")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    // session.conf.getAll.foreach(println)
    session.read.option("header", value = false)
      .csv(df_my_path)
      .createOrReplaceTempView("df_my")

    session.sql("select _c0 as app_name from df_my").createOrReplaceTempView("df_my")

    session.read.option("header", value = true)
      .schema(StructType(Seq(
        StructField("app_name", StringType),
        StructField("metaid", StringType),
        StructField("company", StringType)
      )))
      .option("delimiter", "\t")
      .csv(df_td_path)
      .persist(StorageLevel.DISK_ONLY)
      .createOrReplaceTempView("df_td")

    // session.sql("select * from df_my").show(false)
    // session.sql("select * from df_td").show(false)

    // 做联表
    session.sql("select df_td.app_name, metaid, company from df_my join df_td on df_my.app_name = df_td.app_name")
      .createOrReplaceTempView("sql_join")

    // 统计关联表行数
    // println(session.sql("select * from sql_join").count())

    // 查看关联数据格式
    session.sql("select * from sql_join").orderBy(desc("app_name")).show(false)
  }
}
