package com.weshare.utils;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author ximing.wei
 */
public interface InspectorHandle {

    Object parseJson(JsonNode jsonNode);

    ObjectInspector getReturnType();

    final class InspectorHandleFactory {
        static public InspectorHandle GenerateInspectorHandle(ObjectInspector inspector) throws UDFArgumentException {
            ObjectInspector.Category cat = inspector.getCategory();
            switch (cat) {
                case MAP:
                    return new InspectorHandle.MapHandle((MapObjectInspector) inspector);
                case LIST:
                    return new InspectorHandle.ListHandle((ListObjectInspector) inspector);
                case STRUCT:
                    return new InspectorHandle.StructHandle((StructObjectInspector) inspector);
                case PRIMITIVE:
                    return new InspectorHandle.PrimitiveHandle((PrimitiveObjectInspector) inspector);
            }
            return null;
        }
    }

    class MapHandle implements InspectorHandle {
        private InspectorHandle mapValHandle;
        private StandardMapObjectInspector standardMapObjectInspector;

        MapHandle(MapObjectInspector standardMapObjectInspector) throws UDFArgumentException {
            if (!(standardMapObjectInspector.getMapKeyObjectInspector() instanceof StringObjectInspector)) {
                throw new RuntimeException("JSON maps can only have strings as keys");
            }
            mapValHandle = InspectorHandleFactory.GenerateInspectorHandle(standardMapObjectInspector.getMapValueObjectInspector());
        }

        @Override
//        @SuppressWarnings("unchecked")
        public Object parseJson(JsonNode jsonNode) {
            if (jsonNode.isNull()) return null;

            // 做了类型检查
            Object object = standardMapObjectInspector.create();
            Map<String, Object> newMap = null;
            if (object instanceof Map<?, ?>) {
                Map<?, ?> mapObject = (Map<?, ?>) object;
                Class keyClass = mapObject.entrySet().stream().findFirst().map(entry -> entry.getKey().getClass()).orElse(null);
                Class valueClass = mapObject.entrySet().stream().findFirst().map(entry -> entry.getValue().getClass()).orElse(null);

                if (String.class.equals(keyClass) && Object.class.equals(valueClass)) {
                    newMap = new HashMap<>();
                    for (Map.Entry entry : mapObject.entrySet()) {
                        String key = (String) entry.getKey();
                        Object value = entry.getValue();
                        newMap.put(key, value);
                    }
                }
            }
            return newMap;
        }

        @Override
        public ObjectInspector getReturnType() {
            standardMapObjectInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                    PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                    mapValHandle.getReturnType());
            return standardMapObjectInspector;
        }

    }

    class ListHandle implements InspectorHandle {
        private StandardListObjectInspector standardListObjectInspector;
        private InspectorHandle elemHandle;

        ListHandle(ListObjectInspector listObjectInspector) throws UDFArgumentException {
            elemHandle = InspectorHandleFactory.GenerateInspectorHandle(listObjectInspector.getListElementObjectInspector());
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object parseJson(JsonNode jsonNode) {
            if (jsonNode.isNull()) return null;

            List newList = (List) standardListObjectInspector.create(0);

            Iterator<JsonNode> listNodes = jsonNode.elements();
            while (listNodes.hasNext()) {
                JsonNode elemNode = listNodes.next();
                if (elemNode != null) {
                    Object elemObj = elemHandle.parseJson(elemNode);
                    newList.add(elemObj);
                } else {
                    newList.add(null);
                }
            }
            return newList;
        }

        @Override
        public ObjectInspector getReturnType() {
            standardListObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(elemHandle.getReturnType());
            return standardListObjectInspector;
        }

    }

    class StructHandle implements InspectorHandle {
        private List<String> fieldNames;
        private List<InspectorHandle> handleList;

        StructHandle(StructObjectInspector structInspector) throws UDFArgumentException {
            fieldNames = new ArrayList<>();
            handleList = new ArrayList<>();

            List<? extends StructField> refs = structInspector.getAllStructFieldRefs();
            for (StructField ref : refs) {
                fieldNames.add(ref.getFieldName());
                InspectorHandle fieldHandle = InspectorHandleFactory.GenerateInspectorHandle(ref.getFieldObjectInspector());
                handleList.add(fieldHandle);
            }
        }

        @Override
        public Object parseJson(JsonNode jsonNode) {
            if (jsonNode.isNull()) return null;

            List<Object> valList = new ArrayList<>();

            for (int i = 0; i < fieldNames.size(); ++i) {
                String key = fieldNames.get(i);
                JsonNode valNode = jsonNode.get(key);
                InspectorHandle valHandle = handleList.get(i);

                Object valObj = valHandle.parseJson(valNode);
                valList.add(valObj);
            }

            return valList;
        }

        @Override
        public ObjectInspector getReturnType() {
            List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
            for (InspectorHandle fieldHandle : handleList) {
                structFieldObjectInspectors.add(fieldHandle.getReturnType());
            }
            return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, structFieldObjectInspectors);
        }

    }

    class PrimitiveHandle implements InspectorHandle {
        private PrimitiveObjectInspector.PrimitiveCategory category;
        private DateTimeFormatter isoFormatter = ISODateTimeFormat.dateTimeNoMillis();

        PrimitiveHandle(PrimitiveObjectInspector primitiveObjectInspector) {
            category = primitiveObjectInspector.getPrimitiveCategory();
        }

        @Override
        public Object parseJson(JsonNode jsonNode) {
            if (jsonNode == null || jsonNode.isNull()) return null;

            switch (category) {
                case STRING:
                    if (jsonNode.isTextual())
                        return jsonNode.textValue();
                    else
                        return jsonNode.toString();
                case LONG:
                    return jsonNode.longValue();
                case SHORT:
                    return jsonNode.shortValue();
                case BYTE:
                    return (byte) jsonNode.intValue();
                case BINARY:
                    try {
                        return jsonNode.binaryValue();
                    } catch (IOException ioExc) {
                        return jsonNode.toString();
                    }
                case INT:
                    return jsonNode.intValue();
                case FLOAT:
                    return jsonNode.floatValue();
                case DOUBLE:
                    return jsonNode.doubleValue();
                case BOOLEAN:
                    return jsonNode.booleanValue();
                case TIMESTAMP:
                    long time = isoFormatter.parseMillis(jsonNode.textValue());
                    return new Timestamp(time);
            }
            return null;
        }

        @Override
        public ObjectInspector getReturnType() {
            return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(category);
        }
    }
}
