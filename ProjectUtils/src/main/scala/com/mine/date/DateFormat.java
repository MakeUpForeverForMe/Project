package com.mine.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @version 1.0
 * @Date 2019-09-19
 * @auther ximing.wei
 */
public class DateFormat {

    /**
     * 由 yyyy-MM-dd HH:mm:ss 类型的日期转为 yyyyMMddHHmmss 类型的日期
     *
     * @param string 传入的 yyyy-MM-dd HH:mm:ss 类型的日期
     * @return 返回 yyyyMMddHHmmss 类型的日期
     * @throws ParseException 抛出异常
     */
    public static String ft_2_dt(String string) throws ParseException {
        if (null == string || string.isEmpty())
            return null;
        else
            return new SimpleDateFormat("yyyyMMddHHmmss").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(string));
    }

    /**
     * 由 yyyyMMddHHmmss 类型的日期转为 yyyy-MM-dd HH:mm:ss 类型的日期
     *
     * @param string 传入的 yyyyMMddHHmmss 类型的日期
     * @return 返回 yyyy-MM-dd HH:mm:ss 类型的日期
     * @throws ParseException 抛出异常
     */
    public static String dt_2_ft(String string) throws ParseException {
        if (null == string || string.isEmpty())
            return null;
        else
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new SimpleDateFormat("yyyyMMddHHmmss").parse(string));
    }
}
