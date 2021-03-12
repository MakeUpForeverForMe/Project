package com.weshare.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import static com.weshare.utils.RandomUtil.getRandom;

/**
 * @author ximing.wei 2021-01-27 17:44:14
 */
@Description(
        name = "random",
        value = "_FUNC_([double/int begin,double/int end])",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_() as random;\n" +
                "    0.9587416327855072\n" +
                "  SELECT _FUNC_(1, 10) as random;\n" +
                "    5\n" +
                "  SELECT _FUNC_(1.0, 5.0) as random;\n" +
                "    3.9587416327855072"
)
public class RandomPlus extends UDF {
    public int evaluate(int begin, int end) {
        return ((int) getRandom(begin, end));
    }

    public double evaluate(double begin, double end) {
        return getRandom(begin, end - begin);
    }

    public double evaluate() {
        return getRandom(0, 1);
    }
}
