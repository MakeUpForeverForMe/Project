package com.weshare.generic;

import com.weshare.utils.EmptyUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * @author ximing.wei
 */
@Description(
        name = "is_empty",
        value = "_FUNC_(Object object [, Object... defaultValue])\n{'',null,'null','nil','na','n/a','\\n'}",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('null', '', 'a') as t;\n" +
                "    a"
)
public class IsEmptyGenericUDF extends GenericUDF {
    private transient ObjectInspector[] argumentOIs;
    private transient GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        argumentOIs = objectInspectors;

        if (argumentOIs.length < 1) {
            throw new UDFArgumentLengthException("Arguments need at least one, but: " + argumentOIs.length);
        }

        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        boolean update = returnOIResolver.update(objectInspectors[0]);
        String typeName = objectInspectors[0].getTypeName();
        for (int i = 0; i < objectInspectors.length; i++) {
            if (!(update && returnOIResolver.update(objectInspectors[i]))) {
                throw new UDFArgumentTypeException(i, "" +
                        "The first and the " + i + " arguments of function is_empty should have the same type, " +
                        "but they are different: \"" + typeName +
                        "\" and \"" + objectInspectors[i].getTypeName() + "\""
                );
            }
        }
        return returnOIResolver.get();
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        for (int i = 0; i < deferredObjects.length; i++) {
            Object retVal = returnOIResolver.convertIfNecessary(deferredObjects[i].get(), argumentOIs[i]);
            if (!EmptyUtil.isEmpty(retVal)) return retVal;
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] strings) {
        int length = strings.length;
        StringBuilder display = new StringBuilder("is_empty(");
        for (int i = 0; i < strings.length; i++) {
            display.append(strings[i]);
            if (i + 1 < length) display.append(", ");
            else display.append(")");
        }
        return display.toString();
    }
}
