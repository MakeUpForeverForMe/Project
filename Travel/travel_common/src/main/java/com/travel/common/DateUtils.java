package com.travel.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    private  static SimpleDateFormat  simpleDateFormat= null;

    private  static Calendar calendar = Calendar.getInstance();

    public static String formateDate(Date date,String formatPattern){
        simpleDateFormat = new SimpleDateFormat(formatPattern);
        String format = simpleDateFormat.format(date);
        return format;
    }

    public static String formateDate(String seconds,String formatPattern){
        simpleDateFormat = new SimpleDateFormat(formatPattern);
        String format = simpleDateFormat.format(Long.parseLong(seconds)*1000);
        return format;
    }


    public static int getHour(String timestamp) {
        Date date = new Date(Long.parseLong(timestamp) * 1000);
        int hours = date.getHours();
        return hours;

    }
}
