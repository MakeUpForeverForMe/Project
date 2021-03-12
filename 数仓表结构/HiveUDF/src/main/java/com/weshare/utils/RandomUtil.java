package com.weshare.utils;

/**
 * @author ximing.wei 2021-01-27 19:28:55
 */
public class RandomUtil {
    public static double getRandom(double addend, double multiplier) {
        return Math.random() * multiplier + addend;
    }
}
