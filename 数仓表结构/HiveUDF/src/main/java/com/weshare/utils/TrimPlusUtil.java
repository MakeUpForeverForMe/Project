package com.weshare.utils;

/**
 * @author ximing.wei 2021-01-26 10:46:43
 */
public class TrimPlusUtil {
    public static String trim(String string) {
        if (EmptyUtil.isEmpty(string)) return null;
        return string.replaceAll("([\\\\t\\\\\\\\t\\t\t\\\\])*$", "");
    }
}
