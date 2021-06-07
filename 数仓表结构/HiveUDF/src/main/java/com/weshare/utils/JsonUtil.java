package com.weshare.utils;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateDeserializer;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * JSON工具类(使用Jackson做为实现)
 *
 * @author qujiayuan
 */
public class JsonUtil {

    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final String DEFAULT_KEY = "json_node_default_key";

    static {
//        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        // 匹配不上的字段忽略，宽松模式
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 允许出现特殊字符和转义符
//        objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        // 允许出现单引号
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(LocalDate.class, new LocalDateSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        javaTimeModule.addSerializer(Date.class, new DateSerializer(false, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")));

        javaTimeModule.addDeserializer(LocalDate.class, new LocalDateDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        javaTimeModule.addDeserializer(Date.class, new JsonDeserializer<Date>() {
            @Override
            public Date deserialize(JsonParser p, DeserializationContext cxt) throws IOException {
                try {
                    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(p.getText());
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        objectMapper.registerModule(javaTimeModule);
    }

    /**
     * 把对象转成 Json 格式的字符串
     *
     * @param jsonString 传入的字符型 Json 字符串
     * @return 返回 Json 型字符串
     */
    public static String toJson(String jsonString) {
        return toJson(jsonString, false);
    }

    /**
     * 把对象转成 Json 格式的字符串
     *
     * @param jsonString  传入的字符型 Json 字符串
     * @param prettyPrint 是否格式化输出（打印查看时可以为 true ）
     * @return 返回 Json 型字符串
     */
    public static String toJson(String jsonString, boolean prettyPrint) {
        try {
            if (prettyPrint) {
                return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(parseJson(jsonString));
            } else {
                return objectMapper.writeValueAsString(parseJson(jsonString));
            }
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    /**
     * 通过本方法转化字符型 Json 为 Json 对象
     *
     * @param jsonString 传入的字符型 Json 字符串
     * @return 返回 Json 对象
     */
    private static JsonNode parseJson(String jsonString) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        System.out.println("jsonString\t" + jsonString);
        try {
            Iterator<Map.Entry<String, JsonNode>> fields = objectMapper.readTree(jsonString).fields();
            System.out.println("parseJson - objectMapper.readTree - fields\t" + fields);
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> next = fields.next();
                System.out.println("parseJson - fields.next() - next\t" + next);
                String nextK = next.getKey();
                JsonNode value = next.getValue();
                String valueAsString = objectMapper.writeValueAsString(value);
                System.out.println("parseJson\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                switch (value.getNodeType()) {
                    case OBJECT:
                        System.out.println("parseJson - OBJECT\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                        objectNode.set(nextK, parseJson(valueAsString));
                        break;
                    case ARRAY:
                        System.out.println("parseJson - ARRAY\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                        objectNode.set(nextK, arrayNode(valueAsString));
                        break;
                    case NUMBER:
                        System.out.println("parseJson - NUMBER\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                        objectNode.set(nextK, numberNode(value.numberValue()));
                        break;
                    case BOOLEAN:
                        System.out.println("parseJson - BOOLEAN\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                        objectNode.put(nextK, value.booleanValue());
                        break;
                    case NULL:
                        System.out.println("parseJson - NULL\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                        objectNode.set(nextK, NullNode.getInstance());
                        System.out.println("parseJson - NULL - objectNode\t" + objectNode);
                        break;
                    case STRING:
                        System.out.println("parseJson - STRING\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                        try {
                            System.out.println("parseJson - STRING - ARRAY\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                            if (value instanceof Map) {
                                valueAsString = objectMapper.writeValueAsString(value);
                            } else {
                                valueAsString = value.asText(value.toString());
                            }

                            if (EmptyUtil.isEmpty(valueAsString)) {
                                objectNode.set(nextK, NullNode.getInstance());
                            } else {
                                objectNode.set(nextK, arrayNode(valueAsString));
                            }
                        } catch (JsonProcessingException listException) {
                            System.out.println("parseJson - STRING - OTHER\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                            try {
                                System.out.println("parseJson - STRING - OTHER - MAP\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                                objectNode.set(nextK, mapNode(value.asText(value.toString())));
                            } catch (JsonProcessingException e) {
                                System.out.println("parseJson - STRING - OTHER - STRING\t" + nextK + "\t" + value.getNodeType() + "\t" + value);
                                objectNode.put(nextK, value.textValue());
                            }
                        }
                        break;
                }
                System.out.println("parseJson - objectNode\t" + objectNode);
            }
        } catch (JsonProcessingException e) {
            System.out.println("parseJson - catch JsonProcessingException\t" + e);
        }
        if (objectNode.size() == 0) {
            if (jsonString.equals("false") || jsonString.equals("true")) {
                objectNode.put(DEFAULT_KEY, Boolean.valueOf(jsonString));
            } else {
                objectNode.put(DEFAULT_KEY, EmptyUtil.isEmpty(jsonString) ? null : jsonString);
            }
        }
        System.out.println("objectNode\t" + objectNode);
        return objectNode;
    }

    /**
     * 当字符串中含有 Map 时，通过此方法转为 JsonNode
     *
     * @param valueString 输入的字符串
     * @return 返回 Map 型 JsonNode
     * @throws JsonProcessingException 需要将错误传递到下个程序。以便通过抓取错误后做其他处理
     */
    private static JsonNode mapNode(String valueString) throws JsonProcessingException {
        ObjectNode objectNode = objectMapper.createObjectNode();
        System.out.println("mapNode - valueString\t" + valueString);
        Map<?, ?> map = objectMapper.readValue(valueString, Map.class);
        map.forEach(
                (k, v) -> {
                    try {
                        System.out.println("mapNode - map\t" + k + "\t" + v);
//                        System.out.println("mapNode - v - class\t" + v.getClass());

                        String valueAsString;
                        if (v instanceof List) {
                            valueAsString = ((List<?>) v).stream().map(
                                    l -> {
                                        System.out.println("mapNode - List\t" + l);
                                        if (l instanceof Map) {
                                            try {
                                                System.out.println("mapNode - List - map - l\t" + l);
                                                String string = objectMapper.writeValueAsString(l);
                                                System.out.println("mapNode - List - map - string\t" + string);
                                                return string;
                                            } catch (JsonProcessingException e) {
                                                System.out.println("mapNode - List - map - JsonProcessingException\t" + l);
                                                return null;
                                            }
                                        } else {
                                            System.out.println("mapNode - List - else\t" + l);
                                            return l.toString();
                                        }
                                    }
                            ).collect(Collectors.toList()).toString();
                        } else if (v instanceof Map) {
                            valueAsString = objectMapper.writeValueAsString(v);
                        } else if (v == null) {
                            valueAsString = null;
                        } else {
                            valueAsString = v.toString();
                        }
                        System.out.println("mapNode - valueAsString\t" + k + "\t" + valueAsString);

                        JsonNode jsonNode;
                        if (v instanceof List) {
                            jsonNode = arrayNode(valueAsString);
                        } else {
                            jsonNode = parseJson(EmptyUtil.isEmpty(valueAsString) ? "" : valueAsString);
                        }
                        System.out.println("mapNode - map - jsonNode\t" + jsonNode);
                        JsonNode node = jsonNode.get(DEFAULT_KEY);
                        System.out.println("mapNode - map - node\t" + node + "\t" + (node != null));

                        if (node != null) {
                            objectNode.set(k.toString(), node);
                        } else if (jsonNode.size() != 0) {
                            objectNode.set(k.toString(), jsonNode);
                        } else {
                            objectNode.set(k.toString(), null);
                        }
                        System.out.println("mapNode - map\t" + objectNode);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
        );
        return objectNode;
    }

    /**
     * 当字符串中含有集合时，通过此方法转为 JsonNode
     *
     * @param valueString 输入的字符串
     * @return 返回 集合型 JsonNode
     * @throws JsonProcessingException 需要将错误传递到下个程序。以便通过抓取错误后做其他处理
     */
    private static JsonNode arrayNode(String valueString) throws JsonProcessingException {
        ArrayNode arrayNodeString = objectMapper.createArrayNode();
        System.out.println("arrayNode - valueString\t" + valueString);
        List<?> list = objectMapper.readValue(valueString, List.class);
        System.out.println("arrayNode - readValue\t" + list);
        list.forEach(
                l -> {
                    try {
                        System.out.println("arrayNode - list\t" + l);
                        String valueAsString;
                        if (l instanceof Map) {
                            valueAsString = objectMapper.writeValueAsString(l);
                        } else {
                            valueAsString = l.toString();
                        }
                        JsonNode jsonNode = parseJson(valueAsString);
                        JsonNode node = jsonNode.get(DEFAULT_KEY);
                        arrayNodeString.add(node == null ? jsonNode : node);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                }
        );
        return arrayNodeString;
    }

    /**
     * 转数字为 JsonNode
     *
     * @param number 输入的数值
     * @return 返回 数值型 JsonNode
     */
    private static JsonNode numberNode(Number number) {
        if (number instanceof Double) return new DoubleNode((Double) number);
        else if (number instanceof Long) return new LongNode((Long) number);
        else if (number instanceof Short) return new ShortNode((Short) number);
        else if (number instanceof Float) return new FloatNode((Float) number);
        else if (number instanceof Integer) return new IntNode((Integer) number);
        else if (number instanceof BigDecimal) return new DecimalNode((BigDecimal) number);
        else if (number instanceof BigInteger) return new BigIntegerNode((BigInteger) number);
        else return null;
    }
}
