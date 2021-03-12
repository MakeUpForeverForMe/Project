package com.weshare.udf;

import com.weshare.utils.CompareUtil;
import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * @author ximing.wei
 */
@Description(
    name = "date_max",
    value = "_FUNC_(String aDate[Time], String bDate[Time]) - Returns the max date[Time] of String"
)
public class GetDateMax extends UDF {
  public String evaluate(String aDate, String bDate) {
    boolean a = EmptyUtil.isEmpty(aDate);
    boolean b = EmptyUtil.isEmpty(bDate);
    if (a && b) return null;
    if (a) return bDate;
    if (b) return aDate;

    if (aDate.length() == 10)
      return CompareUtil.getMaxDate(LocalDate.parse(aDate), LocalDate.parse(bDate)).toString();

    String dateTime = CompareUtil.getMaxDate(LocalDateTime.parse(aDate.replace(' ', 'T')), LocalDateTime.parse(bDate.replace(' ', 'T'))).toString().replace('T', ' ');
    return dateTime.length() == 19 ? dateTime : dateTime + ":00";
  }
}
