package com.mine.dateutil

import java.text.{ParseException, SimpleDateFormat}

/**
  * @author 魏喜明
  */
object DateFormat {

    /**
      * 由 yyyy-MM-dd HH:mm:ss 类型的日期转为 yyyyMMddHHmmss 类型的日期
      *
      * @param string 传入的 yyyy-MM-dd HH:mm:ss 类型的日期
      * @return 返回 yyyyMMddHHmmss 类型的日期
      */
    def ft_2_dt(string: String): String = {
        if (null == string || string.isEmpty) return null
        try
            return new SimpleDateFormat("yyyyMMddHHmmss").format(
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(string))
        catch {
            case parseException: ParseException => parseException.printStackTrace()
        }
        null
    }

    /**
      * 由 yyyyMMddHHmmss 类型的日期转为 yyyy-MM-dd HH:mm:ss 类型的日期
      *
      * @param string 传入的 yyyyMMddHHmmss 类型的日期
      * @return 返回 yyyy-MM-dd HH:mm:ss 类型的日期
      */
    def dt_2_ft(string: String): String = {
        if (null == string || string.isEmpty) return null
        try
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(
                new SimpleDateFormat("yyyyMMddHHmmss").parse(string))
        catch {
            case parseException: ParseException => parseException.printStackTrace()
        }
        null
    }
}
