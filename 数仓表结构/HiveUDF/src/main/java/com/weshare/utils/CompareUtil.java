package com.weshare.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @author ximing.wei
 */
public class CompareUtil {
  public static long getMaxInt(long a, long b) {
    return a >= b ? a : b;
  }

  public static long getMinInt(long a, long b) {
    return a <= b ? a : b;
  }

  public static LocalDate getMaxDate(LocalDate a, LocalDate b) {
    return a.compareTo(b) > 0 ? a : b;
  }

  public static LocalDateTime getMaxDate(LocalDateTime a, LocalDateTime b) {
    return a.compareTo(b) > 0 ? a : b;
  }

  public static LocalDate getMinDate(LocalDate a, LocalDate b) {
    return a.compareTo(b) < 0 ? a : b;
  }

  public static LocalDateTime getMinDate(LocalDateTime a, LocalDateTime b) {
    return a.compareTo(b) < 0 ? a : b;
  }
}
