package com.weshare.udf;

import com.weshare.utils.DateUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;

/**
 * @author ximing.wei
 */
@Description(
        name = "datefmt",
        value = "_FUNC_(String string, String fromFmt, String toFmt)",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('20200101000000','yyyyMMddHHmmss','yyyy-MM-dd HH:mm:ss');\n" +
                "    '2020-04-29 20:33:12'"
)
public class DateFormat extends UDF {
    public String evaluate(String string, String fromFmt, String toFmt) throws ParseException {
        return DateUtil.getDate(string, fromFmt, toFmt);
    }
}
