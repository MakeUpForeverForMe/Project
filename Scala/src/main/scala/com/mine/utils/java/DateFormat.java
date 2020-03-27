package com.mine.utils.java;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @version 1.0
 * @Date 2019-09-19
 * @auther ximing.wei
 */
public class DateFormat {

  public String ft_2_dt(String string) throws ParseException {
    return new SimpleDateFormat("yyyyMMddHHmmss").format(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(string));
  }

  public String dt_2_ft(String string) throws ParseException {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new SimpleDateFormat("yyyyMMddHHmmss").parse(string));
  }
}
