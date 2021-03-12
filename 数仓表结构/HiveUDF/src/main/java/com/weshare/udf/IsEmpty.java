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
        value = "_FUNC_(Object string [, Object... defaultValue])\n{'',null,'null','nil','na','n/a','\\n'}",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('null', '', 'a') as t;\n" +
                "    a"
)
public class IsEmpty extends UDF {
    public String evaluate(Object object, Object... defaultValue) {
        if (EmptyUtil.isEmpty(object)) {
            if (defaultValue.length == 0) {
                return null;
            } else {
                return evaluate(defaultValue[0], Arrays.copyOfRange(defaultValue, 1, defaultValue.length));
            }
        }
        return object.toString();
    }
}
