package com.weshare;

import com.weshare.udf.*;
import com.weshare.utils.*;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class UDFTest {
    @Test
    public void encrypt() {
        System.out.println(new AesEncrypt().evaluate("18812345678"));
        System.out.println(new AesEncrypt().evaluate("18812345678", AesPlus.PASSWORD_WESHARE));
    }

    @Test
    public void decrypt() {
//        System.out.println(new AesDecrypt().evaluate("AdDesv4O8b9QR5jIZ6hwgw=="));
        System.out.println(new AesDecrypt().evaluate("cwZ9177KNMW8g3VW3N0JSweGMvP9MBu6UbNjMdUag7s=", AesPlus.PASSWORD_TENCENT));
    }

    @Test
    public void dateFormat() throws ParseException {
        System.out.println(new DateFormat().evaluate("20200429203312", "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss"));
        System.out.println(new DateFormat().evaluate(String.valueOf(20200429203312L), "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss"));
        System.out.println(new DateFormat().evaluate(null, "ms", "yyyy-MM-dd HH:mm:ss"));

        System.out.println(new DateFormat().evaluate("1588240046", "s", "yyyy-MM-dd HH:mm:ss.SSS"));
        System.out.println(new DateFormat().evaluate("1588240046812", "ms", "yyyy-MM-dd HH:mm:ss.SSS"));
        System.out.println(new DateFormat().evaluate("20200430181203", "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss.SSS"));
//    System.out.println(new DateFormat().evaluate(20200430181203L, "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss.SSS"));
    }

    @Test
    public void timeTest() {
        // 当前日期
        LocalDate ld = LocalDate.now();
        System.out.println(ld);
        // 当前时间
        LocalTime lt = LocalTime.now();
        System.out.println(lt);
        // 当前日期和时间
        LocalDateTime ldt = LocalDateTime.now();

        System.out.println(ldt);

        // 指定日期和时间
        LocalDate ld2 = LocalDate.of(2016, 11, 30);
        System.out.println(ld2);
        LocalTime lt2 = LocalTime.of(15, 16, 17);
        System.out.println(lt2);
        LocalDateTime ldt2 = LocalDateTime.of(2016, 11, 30, 15, 16, 17);
        System.out.println(ldt2);
        LocalDateTime ldt3 = LocalDateTime.of(ld2, lt2);
        System.out.println(ldt3);

        LocalDateTime parse = LocalDateTime.parse("2016-12-30 12", DateTimeFormatter.ofPattern("yyyy-MM-dd HH"));
        System.out.println(parse);

        String s = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(parse);
        System.out.println(s);
    }


    @Test
    public void sex() throws Exception {
        String[] strings = new String[]{
                "510522198209174418", // 男
                "440510198110230412", // 男
                "36250219840830501X", // 男
                "440521198011182514", // 男
                "510522198209174418", // 女
                "440510198110230412", // 女
                "429006198210081226", // 女
                "440307198308220069", // 女
                "310109196701301627", // 女
                "522528198407040826", // 女
        };
        for (String string : strings) {
            System.out.println(new GetSexOnIdNo().evaluate(string));
        }
    }


    @Test
    public void testAnalysisJsonArray() {
        String jsonArray = "[{\"name\":\"向小雄\",\"sequence\":1,\"mobile_phone\":\"13794483952\",\"relationship\":\"C\",\"relational_human_type\":\"RHT01\"},{\"name\":\"王龙\",\"sequence\":2,\"mobile_phone\":\"18566291465\",\"relationship\":\"O\",\"relational_human_type\":\"RHT01\"},{\"name\":\"邵华\",\"sequence\":3,\"mobile_phone\":\"13713857136\",\"relationship\":\"W\",\"relational_human_type\":\"RHT01\"}]";
        System.out.println(new AnalysisJsonArray().evaluate(jsonArray));
    }

    @Test
    public void test() {
        System.out.println("111111 MD5     :" + EncoderHandler.encodeByMD5("111111"));
        System.out.println("111111 SHA1    :" + EncoderHandler.encodeBySHA1("111111"));
        System.out.println("111111 SHA-256 :" + EncoderHandler.encodeBySHA256("111111"));
        System.out.println("111111 SHA-512 :" + EncoderHandler.encodeBySHA512("111111"));
    }

    @Test
    public void sha256HashSalt() {
        String string = "18812345678";
        String sha256 = EncoderHandler.encodeBySHA256(string);

        System.out.println(sha256);                                                                       // 93492748c362146f8e48dde778cdb703ead158e42de292307baf24a9b4d4e61b
        System.out.println(IdMappingGenerator.idGenerate(string, 1));
        System.out.println(IdMappingGenerator.idGenerate(sha256, 2));

        System.out.println(new Sha256Salt().evaluate(string, "", 1));                    // @_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087
        System.out.println(new Sha256Salt().evaluate(sha256, "", 2));                    // @_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "idNumber", 1));           // a_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "passport", 1));           // b_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "address", 1));            // c_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "userName", 1));           // d_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "phone", 1));              // e_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "bankCard", 1));           // f_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "imsi", 1));               // g_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "imei", 1));               // h_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "plateNumber", 1));       // i_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "houseNum", 1));          // j_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "frameNumber", 1));       // k_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "engineNumber", 1));      // l_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "businessNumber", 1));    // m_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "organizateCode", 1));    // n_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "taxpayerNumber", 1));    // o_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087

        System.out.println(new Sha256Salt().evaluate(string, "unifiedCreditCode", 1)); // p_e6688c6761527a37f597fafa5f0d3415d5ea9ade3f56ad29448c4d4d77aec087
    }

    @Test
    public void test2() {
        List<String> list = Arrays.asList("idNumber", "passport", "bankCard", "phone", "imsi", "imei", "plateNumber", "houseNum");

        System.out.println(list.indexOf("idNumber"));

        System.out.println(Arrays.toString("a".getBytes()));
        System.out.println(Arrays.toString("b".getBytes()));
        System.out.println((int) 'a');
        System.out.println((char) 97);
    }

    @Test
    public void test5() {
        System.out.println(0x01); // 1
    }

    @Test
    public void test7() {
        System.out.println(EmptyUtil.isEmpty("aa"));
        System.out.println(EmptyUtil.isEmpty("\t     \t   \t"));
    }

    @Test
    public void idNumberUtil() throws Exception {
        System.out.println(IdNumberUtil.len());
        System.out.println(IdNumberUtil.get18IdNo(""));
        System.out.println(IdNumberUtil.get18IdNo("123456789123456")); // 12345619789123456X
        System.out.println(IdNumberUtil.get18IdNo("110105491231002")); // 11010519491231002X
    }

    @Test
    public void ss() {
        Map<String, Map<String, String>> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            Map<String, String> hashMap = new HashMap<>();
            hashMap.put((char) (i + 97) + "", i + "");
            map.put((char) (i + 97) + "", hashMap);
        }
        map.entrySet().forEach(System.out::println);
        System.out.println(map.get("a").get("a"));
        Object o = aa().get("a");
    }

    private static Map aa() {
        Map<String, Map<String, String>> map = new HashMap<>();
        Map<String, String> hashMap = new HashMap<>();
        return map.size() == 0 ? map : hashMap;
    }

    @Test
    public void ageOnIdNo() throws Exception {
        System.out.println(new GetAgeOnIdNo().evaluate("11010519491231002X"));
        System.out.println(new GetAgeOnIdNo().evaluate("11010519491231002X", "2020-12-31"));
        System.out.println(new GetAgeOnIdNo().evaluate("522528198407040826"));
        System.out.println(new GetAgeOnIdNo().evaluate("522528198407040826", "2020-07-04"));
    }

    @Test
    public void getAge() {
        System.out.println(AgeUtil.getAge("2000-06-01", "2020-06-24"));
        System.out.println(AgeUtil.getAge("2000-06-01", "2020-05-24"));

        System.out.println(AgeUtil.getAge("2020-06-24", "2000-06-01"));
        System.out.println(AgeUtil.getAge("2020-05-24", "2000-06-01"));
    }

    @Test
    public void CompareUtilTest() {
        System.out.println(CompareUtil.getMaxDate(LocalDate.parse("2020-06-24"), LocalDate.parse("2000-06-01")));
        System.out.println(CompareUtil.getMinDate(LocalDate.parse("2020-06-24"), LocalDate.parse("2000-06-01")));
    }

    @Test
    public void GetDateTest() {
//    System.out.println(new GetDateMax().evaluate("2020-06-24", "2000-06-01"));
//    System.out.println(new GetDateMin().evaluate("2020-06-24", "2000-06-01"));
        System.out.println(new GetDateMin().evaluate("2020-06-24 12:14:00", "2000-06-01 12:14:00"));
        System.out.println(new GetDateMax().evaluate("2020-06-24 12:14:00", "2000-06-01 12:14:00"));
        System.out.println(new GetDateMax().evaluate("2020-06-24", null));
        System.out.println(new GetDateMin().evaluate("2000-06-01", null));
    }

    @Test
    public void test11() {
        System.out.println(LocalDateTime.parse("2020-06-24T12:00:10").toString().replace('T', ' '));
    }

    @Test
    public void getAgeOnBirthday() {
        System.out.println(new GetAgeOnBirthday().evaluate("2000-06-01"));
        System.out.println(new GetAgeOnBirthday().evaluate("2000-06-01", "2020-06-24"));
        System.out.println(new GetAgeOnBirthday().evaluate("2000-06-01", "2020-05-24"));
    }

    @Test
    public void testAnalysisStringToJson() {
        String str = "{\"k1\":1, \"k2\":\"value\", \"k3\":{\"k31\":1}}";
//        Map map = new AnalysisStringToJson().evaluate(str);
//        System.out.println(map);
//        System.out.println(map.get("k1"));

        String json = "{\"content\":\"{\\\"applyNo\\\":\\\"APP-20200613162750000001\\\",\\\"standardInfo\\\":\\\"1\\\",\\\"pubTraInfo\\\":\\\"1\\\",\\\"remark\\\":\\\"\\\"}\",\"data\":{\"applyNo\":\"APP-20200613162750000001\",\"pubTraInfo\":\"1\",\"remark\":\"\",\"standardInfo\":\"1\"},\"partner\":\"0005\",\"projectName\":\"民生租赁-瓜子-2020一期\",\"projectNo\":\"WS0005200001\",\"service\":\"loanApply\",\"serviceSn\":\"7dde85641066460f8ef819ecda21d4a4\",\"serviceVersion\":\"1\",\"sign\":\"G2VJV1KlnSM6JWiUVPweHcBJptRPSpOXtnogfYEZuwrLfH48hbzDLZH1jwkMCncWMRO9Cj8aTTzy\\n0oRGKpkcAvghsL95fUeTqUMVxJNQxG1VDHtMsIawJWKX+ITBKnkhJPVYpMLKxv9RGHMaubTTnBfU\\nOva8fv4QfpPSZqucSoo=\\n\",\"timeStamp\":\"1592389681316\"}";
//        System.out.println(json);
        Map map = new AnalysisStringToJson().evaluate(json);
//        System.out.println(map);
        map.forEach((key, value) -> System.out.println(key + "\t" + value));
    }

    @Test
    public void testA() {
        for (int i = 0; i <= 200; i++) {
            System.out.println(i + " : " + (i == 0 ? 0 : (i - 1) / 30 + 1));
        }
    }

    @Test
    public void testJsonMap() throws HiveException {
        AnalysisStringToJsonGenericUDF genericUDF = new AnalysisStringToJsonGenericUDF();
        ObjectInspector stringObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector initialize = genericUDF.initialize(new ObjectInspector[]{stringObjectInspector});
        String string = "{\"aa\":12,\"bb\":\"abc\",\"cc\":{\"caa\":111}}";
        Object evaluate = genericUDF.evaluate(new DeferredObject[]{new DeferredJavaObject(string)});
        System.out.println(evaluate);
    }

    @Test
    public void dataIsEmpty1() {
        System.out.println(new IsEmpty().evaluate("aa"));
        System.out.println(new IsEmpty().evaluate(null, "test"));
        System.out.println(new IsEmpty().evaluate("0", "test"));
        System.out.println(new IsEmpty().evaluate("N/A", "test"));
        System.out.println(new IsEmpty().evaluate("\n", "n test n"));
        System.out.println(new IsEmpty().evaluate(null, "", "N/A", "aa"));
        System.out.println(new IsEmpty().evaluate(null, 0));
        System.out.println(new IsEmpty().evaluate("中文"));
    }

    @Test
    public void testList() {
        List<Integer> intList = new ArrayList<>();
        intList.add(1);
        intList.add(3);
        System.out.println(intList);
    }

    @Test
    public void testIsEmtyGenericUDF() throws HiveException {
        IsEmptyGenericUDF emptyGenericUDF = new IsEmptyGenericUDF();

//        JavaBooleanObjectInspector objectInspector = PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
        JavaStringObjectInspector objectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector initialize = emptyGenericUDF.initialize(new ObjectInspector[]{objectInspector, objectInspector});
        System.out.println(initialize.getCategory());

        Object object = "aa";

        Object evaluate = emptyGenericUDF.evaluate(new DeferredObject[]{new DeferredJavaObject(""), new DeferredJavaObject("na")});

        System.out.println(evaluate);
        System.out.println(emptyGenericUDF.getDisplayString(new String[]{null, "aa"}));
    }

    @Test
    public void testIsEmpty2() {
        String s = "aa";
        System.out.println(((Object) s).getClass().getSimpleName());
        int i = 1;
        System.out.println(((Object) i).getClass().getSimpleName());
        long l = 1L;
        System.out.println(((Object) l).getClass().getSimpleName());
    }

    @Test
    public void testDateUtil() throws ParseException {
        System.out.println(DateUtil.getDate("2020/11/18", "", "yyyy-MM-dd"));
        System.out.println(DateUtil.getDate("2020-11-18", "", "yyyy-MM-dd"));
        System.out.println(DateUtil.getDate("2020.11.18", "", "yyyy-MM-dd"));
        System.out.println(DateUtil.getDate("1611303348", "", "yyyy-MM-dd"));
    }

    @Test
    public void testTrimPlus() {
        String str = "\tfbsaifh\\\\\\\\\\\\\\t";
        System.out.println(str);
        System.out.println(TrimPlusUtil.trim(str));

        System.out.println(new TrimPlus().evaluate(str));
    }

    @Test
    public void testRandom() {
        int i = 1;
        while (true) {
//            System.out.println("RandomUtil.getRandom(0, 1) " + RandomUtil.getRandom(0, 1));
//            System.out.println("new RandomPlus().evaluate() " + new RandomPlus().evaluate());
            System.out.println("new RandomPlus().evaluate(1, 10) " + new RandomPlus().evaluate(1, 10));
//            System.out.println("new RandomPlus().evaluate(0.0, 2) " + new RandomPlus().evaluate(0.0, 2));
            if (i == 50) break;
            i++;
        }
    }

    @Test
    public void testType() {
        Object object = "abc";
        System.out.println(object.getClass().getSimpleName());
    }
}
