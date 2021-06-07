package com.weshare.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
 * created by chao.guo on 2021/5/11
 **/
object DateUtils {

  def clcalLastBatchDate(string: String):String={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(string)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    val lastdate = calendar.getTime
    format.format(lastdate)
  }
}
