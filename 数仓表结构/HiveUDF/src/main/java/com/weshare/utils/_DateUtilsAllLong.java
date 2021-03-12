package com.weshare.utils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.regex.Pattern;

/**
 * 日期工具类
 * @author xiaomudong
 *
 */
public class _DateUtilsAllLong {
    protected static Logger logger = LoggerFactory.getLogger(_DateUtilsAllLong.class);
    private static final int DATE_8 = 8;
    private static final int DATE_10 = 10;
    public static String           SEPARATOR_1         = "-";
    public static String           SEPARATOR_2         = "/";

    public static String           SCALE_U        = "U";
    public static String           SCALE_D       = "D";
    public static String           DEFAULTDATEPATTERN         = "yyyy-MM-dd HH:mm:ss";
    public static String           DATEPATTERN_YYYY_MM_DD     = "yyyy-MM-dd";
    public static String           DATEPATTERN_YYYY_MM        = "yyyy-MM";
    public static String           DATEPATTERN_YYYYMM        = "yyyyMM";
    public static final String     DATEPATTERN_YYYYMMDD       = "yyyyMMdd";
    public static final String     DATEPATTERN_YYMMDD       = "yyMMdd";
    public static final String     DATEPATTERN_YYYYMMDDHHMM   = "yyyyMMddHHmm";
    public static final String     DATEPATTERN_YYYYMMDDHHMMSS = "yyyyMMddHHmmss";
    public static final String     DATEPATTERN_YYMMDDHHMMSS = "yyMMddHHmmss";
    public static final String     DATEPATTERN_HHMM = "HHmm";
    public static final String     DATEPATTERN_HHMMSS = "HHmmss";
    private static final String REG_YYYYMMDD = "(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]{1}|[0-9]{1}[1-9][0-9]{2}|[1-9][0-9]{3})(((0[13578]|1[02])(0[1-9]|[12][0-9]|3[01]))" +
            "|((0[469]|11)(0[1-9]|[12][0-9]|30))|(02(0[1-9]|[1][0-9]|2[0-8]))))" +
            "|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))0229)";

    private static final String REG_YYYY_MM_DD = "^(([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]|[0-9][1-9][0-9]{2}|[1-9][0-9]{3})(\\-|\\/)(((0[13578]|[13578]|1[02])(\\-|\\/)" +
            "(0[1-9]|[1-9]|[12][0-9]|3[01]))|((0[469]|[469]|11)(\\-|\\/)(0[1-9]|[1-9]|[12][0-9]|30))|((02|2)(\\-|\\/)(0[1-9]|[1-9]|[1][0-9]|2[0-8]))))" +
            "|((([0-9]{2})(0[48]|[2468][048]|[13579][26])|((0[48]|[2468][048]|[3579][26])00))(\\-|\\/)(02|2)(\\-|\\/)29)";

    public static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern(DATEPATTERN_HHMMSS);
    public static final DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern(DATEPATTERN_YYYYMM);
    public static final DateTimeFormatter SHORT_DATE_FORMATTER = DateTimeFormatter.ofPattern(DATEPATTERN_YYMMDD);
    public static final DateTimeFormatter SHORT_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATEPATTERN_YYMMDDHHMMSS);
    public static final DateTimeFormatter DATETIME_FORMATTER =  DateTimeFormatter.ofPattern(DEFAULTDATEPATTERN);
    public static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATEPATTERN_YYYYMMDD);
    public static final DateTimeFormatter LONG_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DATEPATTERN_YYYYMMDDHHMMSS);


    public static boolean isYYYYMMDD(String date){
        return Pattern.compile(REG_YYYYMMDD).matcher(date).matches();
    }

    public static boolean isYYYY_MM_DD(String date){
        return Pattern.compile(REG_YYYY_MM_DD).matcher(date).matches();
    }

    public static boolean isYYYYMMDDHHMMSS(String date){
        Pattern.compile(REG_YYYY_MM_DD).matcher(date).matches();
        return true;
    }

    public static boolean isSameDay(Date date1, Date date2) {
        if (date1 == null || date2 == null) {
            throw new IllegalArgumentException("The date must not be null");
        }
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);
        return org.apache.commons.lang3.time.DateUtils.isSameDay(cal1, cal2);
    }


    /**
     * 字符串日期转换成java.util.Date对象
     *
     * @param strDate 日期字符串
     * @return java.util.Date对象
     */
    public static Date string2Date(String strDate) {
        Date date = null;
        SimpleDateFormat fmt = new SimpleDateFormat();
        try {
            strDate = StringUtils.trim(strDate);
            if (StringUtils.isNotEmpty(strDate)) {
                if (strDate.indexOf(SEPARATOR_1) != -1) {
                    fmt.applyPattern("yyyy-MM-dd");
                } else if (strDate.indexOf(SEPARATOR_2) != -1) {
                    fmt.applyPattern("yyyy/MM/dd");
                } else if (strDate.length() == DATE_8) {
                    fmt.applyPattern("yyyyMMdd");
                }
                date = fmt.parse(strDate);

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 字符串日期转换成java.util.Date对象
     *
     * @param strDate 日期字符串
     * @return java.util.Date对象
     */
    public static Date string2Date(String strDate,String pattern) {
        Date date = null;
//        Assert.notNull(pattern,"日期格式不能为空");
        SimpleDateFormat fmt = new SimpleDateFormat(pattern);
        try {
            date = fmt.parse(strDate);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return date;
    }


    /**
     * java.util.Date对象转换成字符串日期
     *
     * @param date 日期字符串(yyyyMMdd)
     * @return 字符串日期
     */
    public static String date2String(Date date) {
        SimpleDateFormat fmt = new SimpleDateFormat();
        fmt.applyPattern(_DateUtilsAllLong.DATEPATTERN_YYYYMMDD);
        String strDate = fmt.format(date);
        return strDate;
    }

    /**
     * java.util.Date对象转换成字符串日期
     *
     * @param date 日期字符串
     * @param pattern 转换模式字符串
     * @return 字符串日期
     */
    public static String date2String(Date date, String pattern) {
        SimpleDateFormat fmt  = new SimpleDateFormat();
        fmt.applyPattern(pattern);
        String strDate = fmt.format(date);
        return strDate;
    }

    /**
     * 获取当前日期
     *
     * @return 当前日期
     */
    public static Date getCurrentDate() {
        Calendar calDate = Calendar.getInstance();
        return calDate.getTime();
    }

    /**
     * 获取当前字符串日期
     *
     * @param pattern 转换模式字符串
     * @return
     */
    public static String getCurrentDate(String pattern) {

        String strCurDate = _DateUtilsAllLong.date2String(getCurrentDate(), pattern);
        return strCurDate;
    }


    public static String getRequestTime(String pattern){
        //自定义时间
        Date date = addYear(Calendar.getInstance().getTime(),2);
        return _DateUtilsAllLong.date2String(date, pattern);
    }

    /**
     * 日期相减返回相差天数
     *
     * @param date1
     * @param date2
     * @return
     */
    public static int subDay(Date date1, Date date2) {

        long timeNow = date1.getTime();
        long timeOld = date2.getTime();
        long lngSub = (timeNow - timeOld) / (1000 * 60 * 60 * 24);

        if (lngSub < 0) {
            return 0;
        } else {
            return (int) lngSub;
        }
    }

    /**
     * 日期相减返回相差月数（零头天数时进1）
     *
     * @param date1
     * @param date2
     * @return date1与date2相差月份数
     */
    public static int subMonth(Date date1, Date date2) {

        return subMonth(date1, date2, "U");
    }

    /**
     * 日期相减返回相差月数
     *
     * @param date1
     * @param date2
     * @param dayScale 日期标度<br>
     *            U:date1天数 > date2天数月份加1;<br>
     *            date1天数 <= date2天数月份加0;<br>
     *            D:date1天数 >= date2天数月份加0;<br>
     *            date1天数 < date2天数月份加-1;<br>
     *            <p>
     * @return date1与date2相差月份数
     */
    public static int subMonth(Date date1, Date date2, String dayScale) {

        Calendar c1 = Calendar.getInstance();
        c1.setTime(date1);

        Calendar c2 = Calendar.getInstance();
        c2.setTime(date2);

        //年份相差
        int y = c1.get(Calendar.YEAR) - c2.get(Calendar.YEAR);

        //月份相差数
        int m = c1.get(Calendar.MONTH) - c2.get(Calendar.MONTH);

        //天数相差
        int day1 = c1.get(Calendar.DAY_OF_MONTH);
        int maxDay1 = c1.getActualMaximum(Calendar.DAY_OF_MONTH);

        int day2 = c2.get(Calendar.DAY_OF_MONTH);
        int maxDay2 = c2.getActualMaximum(Calendar.DAY_OF_MONTH);

        int d = 0;
        if (day1 == maxDay1 && day2 == maxDay2) {
            d = 0;
        } else if (day1 == maxDay1 && day1 < day2) {

        } else if (day2 == maxDay2 && day2 == day1) {
            d = 0;
        } else {
            d = day1 - day2;
        }

        int n = y * 12 + m;

        if (SCALE_U.equals(dayScale)) {
            d = d <= 0 ? 0 : 1;
            n = n + d;
        } else if (SCALE_D.equals(dayScale)) {
            d = d >= 0 ? 0 : -1;
            n = n + d;
        }

        return n;
    }

    /**
     * 日期月份增加
     *
     * @param date 日期
     * @param month 添加月份数
     * @return 返回月份增加后日期
     */
    public static Date addMonth(Date date, int month) {
        //
        Calendar cld = Calendar.getInstance();
        cld.setTime(date);
        //月加
        cld.add(Calendar.MONTH, month);

        return cld.getTime();
    }


    /**
     * 日期年份增加
     *
     * @param date 日期
     * @param year 添加年份数
     * @return 返回月份增加后日期
     */
    public static Date addYear(Date date, int year) {
        //
        Calendar cld = Calendar.getInstance();
        cld.setTime(date);
        //月加
        cld.add(Calendar.YEAR, year);

        return cld.getTime();
    }

    /**
     * 日期增加天数
     *
     * @param date 日期
     * @param day 增加的天数
     * @return 增加天数后的日期Date
     */
    public static Date addDay(Date date, int day) {

        Calendar cld = Calendar.getInstance();
        cld.setTime(date);
        //天加
        cld.add(Calendar.DAY_OF_MONTH, day);

        return cld.getTime();
    }

    /**
     * 获取date的天数
     *
     * @param date java.util.Date对象
     * @return 月份中的天数
     */
    public static int getDateDay(Date date) {

        Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.DAY_OF_MONTH);

    }

    /**
     * 获取date的月份
     *
     * @param date java.util.Date对象
     * @return 月份
     */
    public static int getDateMonth(Date date) {

        Calendar c = Calendar.getInstance();
        c.setTime(date);
        return c.get(Calendar.MONTH);
    }

    /**
     * 获取几分钟前的时间
     *
     * @param format
     * @param times
     * @return
     */
    public static String getTimePreNow(String format, int times) {
        SimpleDateFormat df = new SimpleDateFormat(format);
        String timeStr = df.format(System.currentTimeMillis() - times * 60 * 1000);
        return timeStr;

    }
    /**
     * 获取几分钟前的时间
     *
     * @param times
     * @return
     */
    public static Date getTimePreNow( int times) {
        Date now = new Date();
        now.setTime(now.getTime() - (60* 1000) * times);
        return now;

    }


    /**
     * 获取几分钟之前或之后的时间戳
     */
    public static long getTime(int minute){
        return System.currentTimeMillis() - ((60 * 1000) * minute);
    }


    /**
     * 得到个跟现在相差几天的日
     *
     * @param count
     *          几天前为负，几天后为
     * @return
     */
    public static Date getDiffDate(int count) {
        Date now = new Date();
        now.setTime(now.getTime() + (24L * 3600 * 1000) * count);
        return now;
    }
    /**
     * 获取当前时间时间
     *
     * @param format
     * @return
     */
    public static String getCurrentTime(String format) {
        SimpleDateFormat df = new SimpleDateFormat(format);
        String timeStr = df.format(System.currentTimeMillis());
        return timeStr;

    }

    public static String getDatePattern() {
        return DEFAULTDATEPATTERN;
    }

    public static String parseDateStr(String date) {
        if (date == null || "".equals(date)) {
            return null;
        }
        String d = date;
        if (date.length() > DATE_10) {
            d = date.substring(0, 10);
        }
        return d;
    }

    /**
     * Date类型转化为String类型
     *
     * @param date
     * @return
     */
    public static String dateConvert2String(Date date) {
        if (null == date) {
            return null;
        }
        DateFormat formatter = new SimpleDateFormat(DEFAULTDATEPATTERN);
        return formatter.format(date);
    }

    /**
     * Date类型转化为String类型
     *
     * @param date
     * @param pattern
     * @return
     */
    public static String dateConvert2String(Date date, String pattern) {
        if (null == date) {
            return null;
        }
        DateFormat formatter = new SimpleDateFormat(pattern);
        return formatter.format(date);
    }

    /**
     * Date类型转化为String类型
     *
     * @param date
     * @param pattern
     * @return
     */
    public static String dateObjectConvert2String(Object date, String pattern) {
        if (null == date) {
            return null;
        }
        if (date instanceof Date) {
            return dateConvert2String((Date) date, pattern);
        }
        return date.toString();
    }

    /**
     * String类型转化为Date类型
     *
     * @param strDate
     * @return
     */
    public static Date stringConvert2Date(String strDate) {
        if (strDate == null || "".equals(strDate)) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULTDATEPATTERN);
        Date date = null;
        try {
            date = sdf.parse(strDate);
        } catch (ParseException e) {
            return null;
        }
        return date;
    }

    public static Date stringConvert2Date(String strDate, String format) {
        if (strDate == null || "".equals(strDate)) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sdf.parse(strDate);
        } catch (ParseException e) {
            logger.error("stringConvertFormat error!",e);
            return null;
        }
        return date;
    }

    public static int compareToStringDate(String startTime,String endTime, String formart) {
        if(StringUtils.isBlank(startTime) || StringUtils.isBlank(endTime)) {
            return -1 ;
        }
        return _DateUtilsAllLong.stringConvert2Date(startTime,formart).compareTo(_DateUtilsAllLong.stringConvert2Date(endTime,formart)) ;
    }

    public static String stringConvertFormat(String strDate, String format1,String format2) {
        if (strDate == null || "".equals(strDate)) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format1);
        String formatDate = null;
        try {
            Date date = sdf.parse(strDate);
            formatDate = dateConvert2String(date,format2);
        } catch (ParseException e) {
            logger.error("stringConvertFormat error!",e);
            return null;
        }
        return formatDate;
    }

    public static String getFinishTime(String finishTime){
        return stringConvertFormat(finishTime,DEFAULTDATEPATTERN,DATEPATTERN_YYYYMMDDHHMMSS);
    }

    public static String transferCreditDateFormat(String date){
        return stringConvertFormat(date,DATEPATTERN_YYYY_MM_DD,DATEPATTERN_YYYYMMDDHHMMSS);
    }

    public static String transferBaoliDateFormat(String date){
        return stringConvertFormat(date,DATEPATTERN_YYYYMMDDHHMMSS,DATEPATTERN_YYYY_MM_DD);
    }
    /**
     * 获取当前日期类型日期
     * @param formart
     * @return
     */
    public static Date getCurrentDates(String formart){
        return string2Date(getCurrentDate(formart));
    }

    /**
     * 时间戳毫秒值转化为String类型
     * @param timestamp
     * @param pattern
     * @return
     */
    public static String timestamp2String(String timestamp, String pattern){
        Long value = Long.valueOf(timestamp);
        Date date = new Date(value);
        GregorianCalendar gc = new GregorianCalendar();
        gc.setTime(date);
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        return format.format(gc.getTime());
    }
}
