package com.weshare.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ximing.wei 2021-05-10 11:29:16
 */
@Description(
        name = "json_array_to_array",
        value = "_FUNC_(String jsonArray)",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('[{\"aa\":\"bb\"},{\"aa\":\"cc\"}]') as array;\n" +
                "    [{\"aa\":\"bb\"},{\"aa\":\"cc\"}]"
)
public class AnalysisJsonArray extends UDF {
    public List<Map<String, String>> evaluate(String jsonArray) {
        if (EmptyUtil.isEmpty(jsonArray)) return null;

        List<Map<String, String>> list = new ArrayList<>();

        JSON.parseArray(jsonArray).forEach(item -> {
            Map<String, String> hashMap = new HashMap<>();
            ((JSONObject) item).forEach((key, value) -> hashMap.put(key, String.valueOf(value)));
            list.add(hashMap);
        });

        return list;
    }
}
