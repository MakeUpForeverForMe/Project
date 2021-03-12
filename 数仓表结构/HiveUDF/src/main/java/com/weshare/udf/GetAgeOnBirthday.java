package com.weshare.udf;

import com.weshare.utils.AgeUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.time.LocalDate;

/**
 * @author ximing.wei
 */
@Description(
    name = "age_birth",
    value = "_FUNC_(String birthday[, String expireDate])",
    extended = "" +
        "Example:\n" +
        "  SELECT _FUNC_('2000-02-01'[, '2020-01-01']);\n" +
        "    19"
)
public class GetAgeOnBirthday extends UDF {
  public int evaluate(String birthday) {
    return AgeUtil.getAge(birthday, LocalDate.now().toString());
  }

  public int evaluate(String birthday, String expireDate) {
    return AgeUtil.getAge(birthday, expireDate);
  }
}
