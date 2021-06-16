package cn.mine.util;

import lombok.extern.slf4j.Slf4j;

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * @author ximing.wei 2021-06-07 20:43:00
 */
@Slf4j
public enum DateUtils {
    /**
     * 短时间格式
     */
    DATE_CHINESE("yyyy年MM月dd日"),
    DATE_LINE("yyyy-MM-dd"),
    DATE_SLASH("yyyy/MM/dd"),
    DATE_SLASH_TWO("yyyy/M/d"),
    DATE_DOUBLE_SLASH("yyyy\\MM\\dd"),
    DATE_NONE("yyyyMMdd"),

    /**
     * 长时间格式
     */
    DATE_TIME_LINE("yyyy-MM-dd HH:mm:ss"),
    DATE_TIME_SLASH("yyyy/MM/dd HH:mm:ss"),
    DATE_TIME_DOUBLE_SLASH("yyyy\\MM\\dd HH:mm:ss"),
    DATE_TIME_NONE("yyyyMMdd HH:mm:ss"),
    DATE_TIME_STAMP("yyyyMMddHHmmss"),

    /**
     * 长时间格式 带毫秒
     */
    DATE_TIME_MILES_LINE("yyyy-MM-dd HH:mm:ss.SSS"),
    DATE_TIME_MILES_SLASH("yyyy/MM/dd HH:mm:ss.SSS"),
    DATE_TIME_MILES_DOUBLE_SLASH("yyyy\\MM\\dd HH:mm:ss.SSS"),
    DATE_TIME_MILES_NONE("yyyyMMdd HH:mm:ss.SSS"),
    // DATE_TIME_MILES_STAMP("yyyyMMddHHmmssSSS"), // java8 this.formatter.parse 会报错
    ;

    // transient 是序列化的意思。类继承序列化会将类中所有属性和方法都序列化，而只需要序列化一个属性时用 transient。
    public transient DateTimeFormatter formatter;

    DateUtils(String pattern) {
        this.formatter = DateTimeFormatter.ofPattern(pattern, Locale.CHINA);
    }

    public String transform(String date, DateTimeFormatter dateTimeFormatter) {
        log.debug("传入类型 '{}'，传入值 '{}'，转向类型 '{}'", this.name(), date, dateTimeFormatter);

        TemporalAccessor parse = this.formatter.parse(date);
        log.debug("parse '{}'", parse);

        String format = dateTimeFormatter.format(parse);
        log.debug("format '{}'", format);

        return format;
    }

    public String toDefaultDateTime(String string) {
        return this.transform(string, DATE_LINE.formatter);
    }
}
