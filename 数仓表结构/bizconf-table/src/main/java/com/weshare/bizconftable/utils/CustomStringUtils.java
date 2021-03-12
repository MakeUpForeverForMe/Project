package com.weshare.bizconftable.utils;

import java.util.HashMap;

/**
 * Created by mouzwang on 2021-01-20 16:36
 */
public class CustomStringUtils {
    private final static String UNDERLINE = "_";

    /**
     * 驼峰转下划线
     * @param para
     * @return
     */
    public static String humpToUnderline(String para) {
        StringBuilder sb = new StringBuilder(para);
        int temp = 0;//定位
        if (!para.contains(UNDERLINE)) {
            for (int i = 0; i < para.length(); i++) {
                if (Character.isUpperCase(para.charAt(i))) {
                    sb.insert(i + temp, UNDERLINE);
                    temp += 1;
                }
            }
        }
        return sb.toString().toLowerCase();
    }

}
