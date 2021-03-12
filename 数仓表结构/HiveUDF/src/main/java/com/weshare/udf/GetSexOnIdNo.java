package com.weshare.udf;

import com.weshare.utils.EmptyUtil;
import com.weshare.utils.IdNumberUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author ximing.wei
 */
@Description(
    name = "sex_idno",
    value = "_FUNC_(String idNo)",
    extended = "" +
        "Example:\n" +
        "  SELECT _FUNC_('522528198407040826');\n" +
        "    '女'"
)
public class GetSexOnIdNo extends UDF {
  public String evaluate(String idNo) throws Exception {
    idNo = IdNumberUtil.get18IdNo(idNo);
    if (EmptyUtil.isEmpty(idNo)) return null;
    return Integer.valueOf(idNo.substring(16, 17)) % 2 == 0 ? "女" : "男";
  }
}
