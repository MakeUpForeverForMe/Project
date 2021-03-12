package com.weshare.udf;

import com.weshare.utils.AgeUtil;
import com.weshare.utils.DateUtil;
import com.weshare.utils.EmptyUtil;
import com.weshare.utils.IdNumberUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.time.LocalDate;

/**
 * @author ximing.wei
 */
@Description(
    name = "age_idno",
    value = "_FUNC_(String idNo[, String expireDate])",
    extended = "" +
        "Example:\n" +
        "  SELECT _FUNC_('522528198407040826'[, '2020-06-08']);\n" +
        "    36"
)
public class GetAgeOnIdNo extends UDF {
  public int evaluate(String idNo) throws Exception {
    return getAge(idNo, LocalDate.now().toString());
  }

  public int evaluate(String idNo, String expireDate) throws Exception {
    return getAge(idNo, expireDate);
  }

  private int getAge(String idNo, String expireDate) throws Exception {
    if (EmptyUtil.isEmpty((idNo = IdNumberUtil.get18IdNo(idNo)))) return -1;
    String birthday = DateUtil.getDate(idNo.substring(6, 14), "yyyyMMdd", "yyyy-MM-dd");

    return AgeUtil.getAge(birthday, expireDate);
  }
}
