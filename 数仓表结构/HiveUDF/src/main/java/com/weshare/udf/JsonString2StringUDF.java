package com.weshare.udf;

import com.weshare.utils.JsonUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author ximing.wei
 */
@Description(
        name = "js2str",
        value = "_FUNC_(String stringJson)",
        extended = "" +
                "Example:\n" +
                "  JsonString To String\n" +
                "  SELECT _FUNC_('{\"aa\":\"bb\"},{\"cc\":\"dd\"}') as js;\n" +
                "    {\"aa\":\"bb\"},{\"cc\":\"dd\"}"
)
public class JsonString2StringUDF extends UDF {
    public String evaluate(String jsonString) {
        return JsonUtil.toJson(jsonString);
    }
}
