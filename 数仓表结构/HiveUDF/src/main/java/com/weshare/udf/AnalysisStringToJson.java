package com.weshare.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ximing.wei
 */
@Description(
        name = "map_from_str",
        value = "_FUNC_(String stringJson)",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('{\"aa\":\"bb\"},{\"cc\":\"dd\"}') as `map`;\n" +
                "    {\"aa\":\"bb\"},{\"cc\":\"dd\"}"
)
public class AnalysisStringToJson extends UDF {
    public Map<String, String> evaluate(String stringJson) {
        if (EmptyUtil.isEmpty(stringJson)) return null;

        Map<String, String> map = new HashMap<>();
        JSON.parseObject(stringJson).forEach((key, value) -> map.put(key, String.valueOf(value)));

        return map;
    }
}
