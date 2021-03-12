package com.weshare.udf;

import com.weshare.utils.TrimPlusUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author ximing.wei 2021-01-26 10:40:25
 */
@Description(
        name = "ptrim",
        value = "_FUNC_(String string),{\t,\\t,\\}",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('abc\\t') as t;\n" +
                "    abc"
)
public class TrimPlus extends UDF {
    public String evaluate(String string) {
        return TrimPlusUtil.trim(string);
    }
}
