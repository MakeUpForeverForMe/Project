package com.travel.utils

import com.typesafe.config.ConfigFactory

/**
  * Created by laowang
  */
class GlobalConfigUtils {
  private def conf = ConfigFactory.load()
  def heartColumnFamily = "MM"//conf.getString("heart.table.columnFamily")
  val getProp = (argv:String) => conf.getString(argv)

}

object GlobalConfigUtils extends GlobalConfigUtils{

  def main(args: Array[String]): Unit = {
    val str = getProp("spark.worker.timeout")
    println(str)
  }


}







