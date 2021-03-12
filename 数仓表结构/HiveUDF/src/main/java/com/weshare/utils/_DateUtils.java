package com.weshare.utils;


import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_YEAR;
import static java.time.temporal.ChronoUnit.MONTHS;
import static java.time.temporal.ChronoUnit.YEARS;
import static java.time.temporal.TemporalAdjusters.*;

/**
 * 基于 JDK 8 time包的时间工具类
 *
 * @Date 20190102
 */
public class _DateUtils {
    /**
     * 获取默认时间格式: yyyy-MM-dd HH:mm:ss
     */
    private static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER = _DateFormatEnum.LONG_DATE_PATTERN_LINE.formatter;

    private _DateUtils() {
    }

    /**
     * String 转时间
     *
     * @param timeStr
     * @return
     */
    public static LocalDateTime parseTime(String timeStr) {
        return LocalDateTime.parse(timeStr, DEFAULT_DATETIME_FORMATTER);
    }

    /**
     * String 转时间
     *
     * @param timeStr
     * @param format  时间格式
     * @return
     */
    public static LocalDateTime parseTime(String timeStr, _DateFormatEnum format) {
        return LocalDateTime.parse(timeStr, format.formatter);
    }


    public static LocalDate parseLocalDate(String timeStr, _DateFormatEnum format) {
        return LocalDate.parse(timeStr, format.formatter);
    }

    /**
     * 时间转 String
     *
     * @param time
     * @return
     */
    public static String parseTime(LocalDate time) {
        return DEFAULT_DATETIME_FORMATTER.format(time);
    }

    /**
     * 时间转 String
     *
     * @param time
     * @param format 时间格式
     * @return
     */
    public static String parseTime(LocalDateTime time, _DateFormatEnum format) {
        return format.formatter.format(time);
    }


    public static String parseLocalDate(LocalDate time, _DateFormatEnum format) {
        return format.formatter.format(time);
    }

    /**
     * 获取当前时间
     *
     * @return
     */
    public static String getCurrentDatetime() {
        return DEFAULT_DATETIME_FORMATTER.format(LocalDateTime.now());
    }

    /**
     * 获取当前时间
     *
     * @param format 时间格式
     * @return
     */
    public static String getCurrentDatetime(_DateFormatEnum format) {
        return format.formatter.format(LocalDateTime.now());
    }

    /**
     * 获取Period（时间段）
     *
     * @param lt 较小时间
     * @param gt 较大时间
     * @return
     */
    public static Period LocalDateDiff(LocalDate lt, LocalDate gt) {
        Period p = Period.between(lt, gt);
        return p;
    }


    /**
     * 获取时间间隔，并格式化为XXXX年XX月XX日
     *
     * @param lt 较小时间
     * @param gt 较大时间
     * @return
     */
    public static String localDateDiffFormat(LocalDate lt, LocalDate gt) {
        Period p = Period.between(lt, gt);
        String str = String.format(" %d年 %d月 %d日", p.getYears(), p.getMonths(), p.getDays());
        return str;
    }


    /**
     * 获取Duration（持续时间）
     *
     * @param lt 较小时间
     * @param gt 较大时间
     * @return
     */
    public static Duration localTimeDiff(LocalTime lt, LocalTime gt) {
        Duration d = Duration.between(lt, gt);
        return d;
    }


    /**
     * 获取时间间隔（毫秒）
     *
     * @param lt 较小时间
     * @param gt 较大时间
     * @return
     */
    public static long millisDiff(LocalTime lt, LocalTime gt) {
        Duration d = Duration.between(lt, gt);
        return d.toMillis();
    }


    /**
     * 获取时间间隔（秒）
     *
     * @param lt 较小时间
     * @param gt 较大时间
     * @return
     */
    public static long secondDiff(LocalTime lt, LocalTime gt) {
        Duration d = Duration.between(lt, gt);
        return d.getSeconds();
    }


    /**
     * 获取时间间隔（天）
     *
     * @param lt 较小时间
     * @param gt 较大时间
     * @return
     */
    public static long daysDiff(LocalDate lt, LocalDate gt) {
        long daysDiff = ChronoUnit.DAYS.between(lt, gt);
        return daysDiff;
    }

    /**
     * 获取时间间隔（月）
     *
     * @param lt 较小时间
     * @param gt 较大时间
     * @return
     */
    public static long monthsDiff(LocalDate lt, LocalDate gt) {
        long monthsDiff = ChronoUnit.MONTHS.between(lt, gt);
        return monthsDiff;
    }

    /**
     * 日期加减天数
     *
     * @param date
     * @param days
     * @return
     */
    public static LocalDate getDateAddOrSub(LocalDate date, int days) {
        Period period = Period.ofDays(days);
        return date.plus(period);
    }

    /**
     * 日期加减月数
     *
     * @param date
     * @param months
     * @return
     */
    public static LocalDate getDateAddOrSubByMonths(LocalDate date, int months) {
        Period period = Period.ofMonths(months);
        return date.plus(period);
    }

    /**
     * 创建一个新的日期，它的值为上月的最后一天
     *
     * @param date
     * @return
     */
    public static LocalDate getLastDayOfLastMonth(LocalDate date) {
        return date.with((temporal) -> temporal.with(DAY_OF_MONTH, temporal.range(DAY_OF_MONTH).getMaximum()).plus(-1, MONTHS));
    }

    /**
     * 创建一个新的日期，它的值为上月的第一天
     *
     * @param date
     * @return
     */
    public static LocalDate getFirstDayOfLastMonth(LocalDate date) {
        return date.with((temporal) -> temporal.with(DAY_OF_MONTH, 1).plus(-1, MONTHS));
    }

    /**
     * 创建一个新的日期，它的值为当月的第一天
     *
     * @param date
     * @return
     */
    public static LocalDate getFirstDayOfMonth(LocalDate date) {
        return date.with(firstDayOfMonth());
    }

    /**
     * 创建一个新的日期，它的值为当月的最后一天
     *
     * @param date
     * @return
     */
    public static LocalDate getLastDayOfMonth(LocalDate date) {
        return date.with(lastDayOfMonth());
    }

    /**
     * 创建一个新的日期，它的值为下月的第一天
     *
     * @param date
     * @return
     */
    public static LocalDate getFirstDayOfNextMonth(LocalDate date) {
        return date.with(firstDayOfNextMonth());
    }

    /**
     * 创建一个新的日期，它的值为下月的最后一天
     *
     * @param date
     * @return
     */
    public static LocalDate getLastDayOfNextMonth(LocalDate date) {
        return date.with((temporal) -> temporal.with(DAY_OF_MONTH, temporal.range(DAY_OF_MONTH).getMaximum()).plus(1, MONTHS));
    }

    /**
     * 创建一个新的日期，它的值为上年的第一天
     *
     * @param date
     * @return
     */
    public static LocalDate getFirstDayOfLastYear(LocalDate date) {
        return date.with((temporal) -> temporal.with(DAY_OF_YEAR, 1).plus(-1, YEARS));
    }

    /**
     * 创建一个新的日期，它的值为上年的最后一天
     *
     * @param date
     * @return
     */
    public static LocalDate getLastDayOfLastYear(LocalDate date) {
        return date.with((temporal) -> temporal.with(DAY_OF_YEAR, temporal.range(DAY_OF_YEAR).getMaximum()).plus(-1, YEARS));
    }


    /**
     * 创建一个新的日期，它的值为当年的第一天
     *
     * @param date
     * @return
     */
    public static LocalDate getFirstDayOfYear(LocalDate date) {
        return date.with(firstDayOfYear());
    }


    /**
     * 创建一个新的日期，它的值为今年的最后一天
     *
     * @param date
     * @return
     */
    public static LocalDate getLastDayOfYear(LocalDate date) {
        return date.with(lastDayOfYear());
    }


    /**
     * 创建一个新的日期，它的值为明年的第一天
     *
     * @param date
     * @return
     */
    public static LocalDate getFirstDayOfNextYear(LocalDate date) {
        return date.with(firstDayOfNextYear());
    }


    /**
     * 创建一个新的日期，它的值为明年的最后一天
     *
     * @param date
     * @return
     */
    public static LocalDate getLastDayOfNextYear(LocalDate date) {
        return date.with((temporal) -> temporal.with(DAY_OF_YEAR, temporal.range(DAY_OF_YEAR).getMaximum()).plus(1, YEARS));
    }


    /**
     * 创建一个新的日期，它的值为同一个月中，第一个符合星期几要求的值
     *
     * @param date
     * @return
     */
    public static LocalDate getFirstInMonth(LocalDate date, DayOfWeek dayOfWeek) {
        return date.with(firstInMonth(dayOfWeek));
    }


    /**
     * 创建一个新的日期，并将其值设定为日期调整后或者调整前，第一个符合指定星 期几要求的日期
     *
     * @param date
     * @return
     */
    public static LocalDate getNext(LocalDate date, DayOfWeek dayOfWeek) {
        return date.with(next(dayOfWeek));
    }


    /**
     * 创建一个新的日期，并将其值设定为日期调整后或者调整前，第一个符合指定星 期几要求的日期
     *
     * @param date
     * @return
     */
    public static LocalDate getPrevious(LocalDate date, DayOfWeek dayOfWeek) {
        return date.with(previous(dayOfWeek));
    }


    /**
     * 创建一个新的日期，并将其值设定为日期调整后或者调整前，第一个符合指定星 期几要求的日期，如果该日期已经符合要求，直接返回该对象
     *
     * @param date
     * @return
     */
    public static LocalDate getNextOrSame(LocalDate date, DayOfWeek dayOfWeek) {
        return date.with(nextOrSame(dayOfWeek));
    }


    /**
     * 创建一个新的日期，并将其值设定为日期调整后或者调整前，第一个符合指定星 期几要求的日期，如果该日期已经符合要求，直接返回该对象
     *
     * @param date
     * @return
     */
    public static LocalDate getPreviousOrSame(LocalDate date, DayOfWeek dayOfWeek) {
        return date.with(previousOrSame(dayOfWeek));
    }

    public static YearMonth getYearMonth(LocalDate date) {
        return YearMonth.from(date);
    }

    public static long monthsDiff(YearMonth lt, YearMonth gt) {
        return ChronoUnit.MONTHS.between(lt, gt);
    }

    public static Date localDateTimeToDate(LocalDateTime localDateTime) {
        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zdt = localDateTime.atZone(zoneId);
        return Date.from(zdt.toInstant());
    }

    public static Date localDateToDate(LocalDate localDate) {
        ZoneId zoneId = ZoneId.systemDefault();
        ZonedDateTime zdt = localDate.atStartOfDay(zoneId);
        return Date.from(zdt.toInstant());
    }

    public static LocalDateTime dateToLocalDateTime(Date date) {
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();
        return instant.atZone(zoneId).toLocalDateTime();
    }

    public static LocalDate dateToLocalDate(Date date) {
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();
        return instant.atZone(zoneId).toLocalDate();
    }

    /**
     * 获取时间段的所有月份(取月份最大日期为统一天数)
     *
     * @param startDate 格式必须为'2018-01-25'
     * @param endDate   格式必须为'2018-01-25'
     * @return
     */
    public static List<LocalDate> getBetweenMonthDate(LocalDate startDate, LocalDate endDate) {
        List<LocalDate> list = new ArrayList<>();

        long distance = ChronoUnit.MONTHS.between(startDate, endDate);
        if (distance < 1) {
            return list;
        }
        Stream.iterate(startDate, d -> {
            return d.plusMonths(1);
        }).limit(distance + 1).forEach(f -> {
            list.add(f);
        });
        return list;
    }

    /**
     * 获取两个日期间隔的所有日期
     *
     * @param startDate 格式必须为'2018-01-25'
     * @param endDate   格式必须为'2018-01-25'
     * @return
     */
    public static List<Date> getBetweenDayDate(LocalDate startDate, LocalDate endDate) {
        List<Date> list = new ArrayList<>();
        long distance = ChronoUnit.DAYS.between(startDate, endDate);
        if (distance < 1) {
            return list;
        }
        Stream.iterate(startDate, d -> {
            return d.plusDays(1);
        }).limit(distance + 1).forEach(f -> {
            list.add(_DateUtils.localDateToDate(f));
        });
        return list;
    }
    
    /**
     * @Description 从日期(yyyyMMdd)中获取日期(dd)数字
     * @param date
     * @return
     * @author tianyang.zhang
     * @date 2019年3月5日 下午8:28:10
     */
    public static Integer getDayFromDate(Date date) {
        LocalDate localDate = dateToLocalDate(date);
    	return localDate.getDayOfMonth();
    }

    /**
     * 判断时间是否在时间段内
     *
     * @param beginDate
     * @param endDate
     * @param localDate
     * @return
     */
    public static boolean belongCalendar(LocalDate beginDate, LocalDate endDate, LocalDate localDate) {
        if (localDate.isAfter(beginDate) && localDate.isBefore(endDate)) {
            return true;
        } else {
            return localDate.isEqual(beginDate) || localDate.isEqual(endDate);
        }
    }

    /**
     * date to String yyyyMMdd
     * @param date
     * @return
     */
    public static String dateToString(Date date){
        return dateToString(date, _DateFormatEnum.SHORT_DATE_PATTERN_NONE);
    }

    /**
     * date to String format
     * @param date
     * @return
     */
    public static String dateToString(Date date,_DateFormatEnum dateFormatEnum){
        LocalDateTime localDateTime = _DateUtils.datetoLocalDateTime(date);
        return parseTime(localDateTime, dateFormatEnum);
    }

    /**
     * date to LocalDateTime
     *
     * @param date
     * @return
     */
    public static LocalDateTime datetoLocalDateTime(Date date) {
        Instant instant = date.toInstant();
        ZoneId zone = ZoneId.systemDefault();
        return LocalDateTime.ofInstant(instant, zone);
    }

    /**
     * 比较两个日期的是否相同忽略时差
     * @auther: v_chenjun
     * @date: 2019/5/29 14:58
     * @return:
     */
    public static boolean sameDate(Date d1, Date d2) {
        LocalDate localDate1 = ZonedDateTime.ofInstant(d1.toInstant(), ZoneId.systemDefault()).toLocalDate();
        LocalDate localDate2 = ZonedDateTime.ofInstant(d2.toInstant(), ZoneId.systemDefault()).toLocalDate();
        return localDate1.isEqual(localDate2);

    }

    /**
     * @param dateStr 日期字符串
     * @return
     * @Description 校验日期的格式是否为yyyy/MM/dd或yyyy-MM-dd
     * @author tianyang.zhang
     * @date 2019年2月26日 下午9:15:32
     */
    public static boolean checkDateFormat(String dateStr) {
        String el = "^((\\d{2}(([02468][048])|([13579][26]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])))))|(\\d{2}(([02468][1235679])|([13579][01345789]))[\\-\\/\\s]?((((0?[13578])|(1[02]))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(3[01])))|(((0?[469])|(11))[\\-\\/\\s]?((0?[1-9])|([1-2][0-9])|(30)))|(0?2[\\-\\/\\s]?((0?[1-9])|(1[0-9])|(2[0-8]))))))";
        Pattern p = Pattern.compile(el);
        Matcher m = p.matcher(dateStr);
        return m.matches();
    }
}
