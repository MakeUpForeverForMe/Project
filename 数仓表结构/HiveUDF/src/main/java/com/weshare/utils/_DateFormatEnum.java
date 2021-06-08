package com.weshare.utils;

import java.time.format.DateTimeFormatter;

/**
 * 时间格式
 *
 * @Date 20190102
 */
public enum _DateFormatEnum {
    /**
     * 短时间格式
     */
    SHORT_DATE_PATTERN_CHINESE("yyyy年MM月dd日"),
    SHORT_DATE_PATTERN_LINE("yyyy-MM-dd"),
    SHORT_DATE_PATTERN_SLASH("yyyy/MM/dd"),
    SHORT_DATE_PATTERN_SLASH_TWO("yyyy/M/d"),
    SHORT_DATE_PATTERN_DOUBLE_SLASH("yyyy\\MM\\dd"),
    SHORT_DATE_PATTERN_NONE("yyyyMMdd"),
    SHORT_DATE_PATTERN_NONE_TWO("HHmmssSSS"),

    /**
     * 长时间格式
     */
    LONG_DATE_PATTERN_LINE("yyyy-MM-dd HH:mm:ss"),
    LONG_DATE_PATTERN_SLASH("yyyy/MM/dd HH:mm:ss"),
    LONG_DATE_PATTERN_DOUBLE_SLASH("yyyy\\MM\\dd HH:mm:ss"),
    LONG_DATE_PATTERN_NONE("yyyyMMdd HH:mm:ss"),
    LONG_DATE_PATTERN_STAMP("yyyyMMddHHmmss"),

    /**
     * 长时间格式 带毫秒
     */
    LONG_DATE_PATTERN_WITH_MILSEC("yyyyMMddHHmmssSSS"),
    LONG_DATE_PATTERN_WITH_MILSEC_LINE("yyyy-MM-dd HH:mm:ss.SSS"),
    LONG_DATE_PATTERN_WITH_MILSEC_SLASH("yyyy/MM/dd HH:mm:ss.SSS"),
    LONG_DATE_PATTERN_WITH_MILSEC_DOUBLE_SLASH("yyyy\\MM\\dd HH:mm:ss.SSS"),
    LONG_DATE_PATTERN_WITH_MILSEC_NONE("yyyyMMdd HH:mm:ss.SSS");

    public transient DateTimeFormatter formatter; // transient 是序列化的意思。类继承序列化会将类中所有属性和方法都序列化，而只需要序列化一个方法时用 transient。

    _DateFormatEnum(String pattern) {
        this.formatter = DateTimeFormatter.ofPattern(pattern);
    }
}
