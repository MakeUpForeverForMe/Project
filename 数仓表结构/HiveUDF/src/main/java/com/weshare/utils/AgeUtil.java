package com.weshare.utils;

import java.time.LocalDate;

/**
 * @author ximing.wei
 */
public class AgeUtil {

  public static int getAge(String birthday, String expireDate) {
    if (EmptyUtil.isEmpty(birthday) || EmptyUtil.isEmpty(expireDate)) return -1;

    LocalDate expDate = LocalDate.parse(expireDate);
    LocalDate birDate = LocalDate.parse(birthday);

    LocalDate maxDate = CompareUtil.getMaxDate(expDate, birDate);
    LocalDate minDate = CompareUtil.getMinDate(expDate, birDate);

    int ageYear = maxDate.getYear() - minDate.getYear();
    int ageMonth = maxDate.getMonthValue() - minDate.getMonthValue();
    int ageDay = maxDate.getDayOfMonth() - minDate.getDayOfMonth();

    if (ageMonth > 0) return ageYear;
    else if (ageMonth < 0) return ageYear - 1;
    else {
      if (ageDay >= 0) return ageYear;
      else return ageYear - 1;
    }
  }
}
