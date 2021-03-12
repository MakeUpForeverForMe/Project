package com.weshare.udf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.weshare.utils.InspectorHandle;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.io.IOException;

/**
 * @author ximing.wei
 */
@Description(
        name = "json_map",
        value = "_FUNC_(String stringJson)",
        extended = "" +
                "Example:\n" +
                "  SELECT _FUNC_('{\"aa\":\"bb\"},{\"cc\":\"dd\"}') as `map`;\n" +
                "    {\"aa\":\"bb\"},{\"cc\":\"dd\"}"
)
public class AnalysisStringToJsonGenericUDF extends GenericUDF {
    private StringObjectInspector stringInspector;
    private InspectorHandle inspectorHandle;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) throw new UDFArgumentLengthException("Arguments length must be equal to 1");
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE) || ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING)
            throw new UDFArgumentTypeException(0, "Arguments should be STRING. Usage : json_map(stringJson)");

        stringInspector = (StringObjectInspector) arguments[0];

        ObjectInspector keyOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        MapObjectInspector mapOI = ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);

        inspectorHandle = InspectorHandle.InspectorHandleFactory.GenerateInspectorHandle(mapOI);

        assert inspectorHandle != null;
        return inspectorHandle.getReturnType();
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            String jsonString = this.stringInspector.getPrimitiveJavaObject(arguments[0].get());
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            return inspectorHandle.parseJson(jsonNode);
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return "json_map( " + children[0] + " )";
    }
}
