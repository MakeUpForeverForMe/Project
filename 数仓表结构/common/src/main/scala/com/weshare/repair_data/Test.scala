package com.weshare.repair_data

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * created by chao.guo on 2020/11/26
 **/
object Test {
  def main(args: Array[String]): Unit = {

    val start_Date = "2019-11-01"
    val end_date = "2019-11-10"
    //println(start_Date.compareTo(end_date))
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val start_date_ = Calendar.getInstance()
    val pmt_date_ = Calendar.getInstance()
    start_date_.setTime(format.parse(start_Date))
    pmt_date_.setTime(format.parse("2019-11-01"))
    pmt_date_.add(Calendar.MONTH, 1)
    println(format.format(pmt_date_.getTime))

    if (start_date_.getTime.compareTo(pmt_date_.getTime) < 0) {
      println("test")

    }
  }
}
