package com.weshare.udf;

import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;


/**
 * @author ximing.wei
 */
@Description(
        name = "is_empty",
        value = "_FUNC_(Object... objects)\n{'',null,'null','nil','na','n/a','\\n'}",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('null', '', 'a') as t;\n" +
                "    a"
)
public class IsEmpty extends UDF {
    public String evaluate(Object... objects) {
        for (Object object : objects) if (!EmptyUtil.isEmpty(object)) return object.toString();
        return null;
    }
}
