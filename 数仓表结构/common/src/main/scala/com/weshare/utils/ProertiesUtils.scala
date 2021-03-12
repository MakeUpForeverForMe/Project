package com.weshare.utils

import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
 * created by chao.guo on 2020/10/15
 **/
object ProertiesUtils {
def propertiesLoad(spark:SparkSession,config_path:String): Properties ={
  val pro = new Properties()
  val inputSteam = FileSystem.get(spark.sparkContext.hadoopConfiguration).open(new Path(config_path))
  pro.load(inputSteam)
  inputSteam.close()
  pro
}
}
