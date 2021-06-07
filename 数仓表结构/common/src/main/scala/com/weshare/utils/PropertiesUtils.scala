package com.weshare.utils

import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
 * created by chao.guo on 2020/10/15
 **/
object PropertiesUtils {
def propertiesLoad(spark:SparkSession,config_path:String): Properties ={
  val pro = new Properties()
  val inputSteam = FileSystem.get(spark.sparkContext.hadoopConfiguration).open(new Path(config_path))
  pro.load(inputSteam)
  inputSteam.close()
  pro
}

  /**
   * 在项目环境下加载配置文件
   * @param `type`
   * @return
   */
  def propertiesLoad(`type`:String): Properties ={
    val pro =new Properties()
    `type` match {
      case "PRO" =>
        pro.load(PropertiesUtils.getClass.getClassLoader.getResourceAsStream("pro_properties.properties"))
      case "EMR"=>
        pro.load(PropertiesUtils.getClass.getClassLoader.getResourceAsStream("emr_properties.properties"))
      case "TEST"=>
        pro.load(PropertiesUtils.getClass.getClassLoader.getResourceAsStream("test_properties.properties"))
    }
    pro
  }




}
