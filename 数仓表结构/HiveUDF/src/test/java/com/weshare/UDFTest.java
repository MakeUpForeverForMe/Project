package com.weshare;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.weshare.udf.*;
import com.weshare.utils.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
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
    public void test() {
        System.out.println("111111 MD5     :" + EncoderHandler.md5("111111"));
        System.out.println("111111 SHA1    :" + EncoderHandler.sha1("111111"));
        System.out.println("111111 SHA-256 :" + EncoderHandler.sha256("111111"));
        System.out.println("111111 SHA-512 :" + EncoderHandler.sha512("111111"));
    }

    @Test
    public void sha256HashSalt() {
        String string = "18812345678";
        String sha256 = EncoderHandler.sha256(string);

        System.out.println(sha256);                                                                       // 93492748c362146f8e48dde778cdb703ead158e42de292307baf24a9b4d4e61b

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
    public void testA() {
        for (int i = 0; i <= 200; i++) {
            System.out.println(i + " : " + (i == 0 ? 0 : (i - 1) / 30 + 1));
        }
    }

//    @Test
//    public void testJsonMap() throws HiveException {
//        JsonString2StringGenericUDF genericUDF = new JsonString2StringGenericUDF();
//        ObjectInspector stringObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
//        ObjectInspector initialize = genericUDF.initialize(new ObjectInspector[]{stringObjectInspector});
//        String string = "{\"aa\":12,\"bb\":\"abc\",\"cc\":{\"caa\":111}}";
//        Object evaluate = genericUDF.evaluate(new DeferredObject[]{new DeferredJavaObject(string)});
//        System.out.println(evaluate);
//    }

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

    @Test
    public void testJsonUtil() throws JsonProcessingException {
//        String jsonString = "{\"org\": \"15601\", \"partner\": \"0006\", \"reqContent\": {\"jsonResp\": \"{\\\"sign\\\": null, \\\"partner\\\": \\\"0006\\\", \\\"rspData\\\": \\\"{\\\\\\\"consumptionLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"amtOfPerfermanceLoansLast3M\\\\\\\":99,\\\\\\\"hitDeadbeatList\\\\\\\":0,\\\\\\\"appsOfConFinOrgLastTwoYear\\\\\\\":99,\\\\\\\"overdueTimesOfCreditApplyLast4M\\\\\\\":99,\\\\\\\"overdueLoansOfMoreThan1DayLast6M\\\\\\\":99,\\\\\\\"catPool\\\\\\\":0,\\\\\\\"LastFinancialQuery\\\\\\\":99,\\\\\\\"scoreLevel\\\\\\\":\\\\\\\"B\\\\\\\",\\\\\\\"countOfInstallitionOfLoanAppLast2M\\\\\\\":99,\\\\\\\"countOfUninstallAppsOfLoanLast3M\\\\\\\":99,\\\\\\\"blkListLoc\\\\\\\":0,\\\\\\\"averageDailyOpenTimesOfFinAppsLastM\\\\\\\":99,\\\\\\\"timesOfUninstallFinAppsLast15D\\\\\\\":99,\\\\\\\"fraudIndustry\\\\\\\":0,\\\\\\\"sumCreditOfWebLoan\\\\\\\":99,\\\\\\\"suspiciousDevice\\\\\\\":0,\\\\\\\"daysOfLocationUploadLats9M\\\\\\\":99,\\\\\\\"accountHacked\\\\\\\":0,\\\\\\\"marriage\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"virtualMaliciousStatus\\\\\\\":0,\\\\\\\"accountForInTownInWorkDayLastM\\\\\\\":99,\\\\\\\"daysOfEarliestRegisterOfLoanAppLast9M\\\\\\\":99,\\\\\\\"applyByIDLastYear\\\\\\\":99,\\\\\\\"gamerArbitrageStatus\\\\\\\":0,\\\\\\\"abnormalPayment\\\\\\\":0,\\\\\\\"usageRateOfCompusLoanApp\\\\\\\":99,\\\\\\\"pass\\\\\\\":\\\\\\\"Yes\\\\\\\",\\\\\\\"cityLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"appsOfConFinProLastTwoYear\\\\\\\":99,\\\\\\\"countOfNoticesOfFinMessageLast9M\\\\\\\":99,\\\\\\\"financAbility\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"incomeLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"forgedIdStatus\\\\\\\":0,\\\\\\\"rejectTimesOfCreditApplyLast4M\\\\\\\":99,\\\\\\\"riskOfDevice\\\\\\\":99,\\\\\\\"idTheftStatus\\\\\\\":0,\\\\\\\"abnormalAccount\\\\\\\":0,\\\\\\\"edu\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"accountForInstallBussinessAppsLast4M\\\\\\\":99,\\\\\\\"blkList2\\\\\\\":0,\\\\\\\"blkList1\\\\\\\":0,\\\\\\\"accountForWifiUseTimeSpanLast5M\\\\\\\":99,\\\\\\\"loanAmtLast3M\\\\\\\":99,\\\\\\\"countOfRegisterAppsOfFinLastM\\\\\\\":99,\\\\\\\"counterfeitAgencyStatus\\\\\\\":0,\\\\\\\"status\\\\\\\":\\\\\\\"1\\\\\\\"}\\\", \\\"service\\\": \\\"WIND_CONTROL_CREDIT\\\", \\\"resultMsg\\\": \\\"请求成功\\\", \\\"resultCode\\\": \\\"0000\\\", \\\"serviceVersion\\\": \\\"1\\\"}\", \"reqMsgCreateDate\": \"2020-12-22 13:49:31\", \"jsonReq\": \"{\\\"sign\\\": \\\"rtAHHYtVX0he+npSXuZl0fMxGBVyoa1gm1vNux3OJYjhUYoNTpTqXkrBCbtLE6Pr5BRgKU58dcrz\\\\nGDtNH+k5szX/F2nmko25ouo9Go7IZXUfSosMKHLlatUDLoYRGM7MUZLxKs0GY/guwrZR1Hf1IfmM\\\\nwIhB/fAfED3ujRdAI0o=\\\\n\\\", \\\"content\\\": \\\"{\\\\\\\"reqData\\\\\\\":{\\\\\\\"ifCar\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"country\\\\\\\":\\\\\\\"CHN\\\\\\\",\\\\\\\"authAccountName\\\\\\\":\\\\\\\"王香皖\\\\\\\",\\\\\\\"dbBankCode\\\\\\\":\\\\\\\"313301008887\\\\\\\",\\\\\\\"lnRate\\\\\\\":2.100000,\\\\\\\"endDate\\\\\\\":\\\\\\\"2021-12-16\\\\\\\",\\\\\\\"workSts\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"workDuty\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"homeAddr\\\\\\\":\\\\\\\"安徽省阜阳市颍泉区清颍路131号10幢202户\\\\\\\",\\\\\\\"idNo\\\\\\\":\\\\\\\"341204200105190421\\\\\\\",\\\\\\\"sales\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"homeCode\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"ifCarCred\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"payType\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"isBelowRisk\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"children\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"vouType\\\\\\\":\\\\\\\"4\\\\\\\",\\\\\\\"creditLimit\\\\\\\":7165.00,\\\\\\\"cardAmt\\\\\\\":0.00,\\\\\\\"profession\\\\\\\":\\\\\\\"12\\\\\\\",\\\\\\\"idType\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"authNo\\\\\\\":\\\\\\\"1120122213485525204565\\\\\\\",\\\\\\\"mincome\\\\\\\":1000.00,\\\\\\\"postAddr\\\\\\\":\\\\\\\"安徽省阜阳市颍泉区清颍路131号10幢202户\\\\\\\",\\\\\\\"workTitle\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"ifCard\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"degree\\\\\\\":\\\\\\\"4\\\\\\\",\\\\\\\"ifAgent\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"birth\\\\\\\":\\\\\\\"20010519\\\\\\\",\\\\\\\"ifId\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"workName\\\\\\\":\\\\\\\"无\\\\\\\",\\\\\\\"prodType\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"telNo\\\\\\\":\\\\\\\"18856825195\\\\\\\",\\\\\\\"pactAmt\\\\\\\":14.40,\\\\\\\"ifPact\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"ifApp\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"edu\\\\\\\":\\\\\\\"20\\\\\\\",\\\\\\\"custType\\\\\\\":\\\\\\\"04\\\\\\\",\\\\\\\"loanDate\\\\\\\":\\\\\\\"2020-12-22\\\\\\\",\\\\\\\"postCode\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"hasOverdueLoan\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"income\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"appUse\\\\\\\":\\\\\\\"07\\\\\\\",\\\\\\\"riskLevel\\\\\\\":\\\\\\\"R3\\\\\\\",\\\\\\\"proCode\\\\\\\":\\\\\\\"002001\\\\\\\",\\\\\\\"zxhomeIncome\\\\\\\":12000.00,\\\\\\\"rpyMethod\\\\\\\":\\\\\\\"03\\\\\\\",\\\\\\\"launder\\\\\\\":\\\\\\\"03\\\\\\\",\\\\\\\"dbAccountName\\\\\\\":\\\\\\\"深圳市分期乐网络科技有限公司\\\\\\\",\\\\\\\"phoneNo\\\\\\\":\\\\\\\"18856825195\\\\\\\",\\\\\\\"ifMort\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"loanTime\\\\\\\":\\\\\\\"2020-12-22 13:49:16\\\\\\\",\\\\\\\"marriage\\\\\\\":\\\\\\\"10\\\\\\\",\\\\\\\"customerId\\\\\\\":\\\\\\\"148380565\\\\\\\",\\\\\\\"authBankName\\\\\\\":\\\\\\\"工商银行\\\\\\\",\\\\\\\"idEndDate\\\\\\\":\\\\\\\"2021-07-11\\\\\\\",\\\\\\\"idPreDate\\\\\\\":\\\\\\\"2016-07-11\\\\\\\",\\\\\\\"sex\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"ifRoom\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"appArea\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"ifLaunder\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"custName\\\\\\\":\\\\\\\"王香皖\\\\\\\",\\\\\\\"homeIncome\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"dbBankName\\\\\\\":\\\\\\\"南京银行\\\\\\\",\\\\\\\"authBankAccount\\\\\\\":\\\\\\\"6215581311005946674\\\\\\\",\\\\\\\"homeArea\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"homeSts\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"loanTerm\\\\\\\":12,\\\\\\\"trade\\\\\\\":\\\\\\\"26\\\\\\\",\\\\\\\"creditCoef\\\\\\\":2.100000,\\\\\\\"applyNo\\\\\\\":\\\\\\\"1120122213485525204565\\\\\\\",\\\\\\\"dbBankAccount\\\\\\\":\\\\\\\"2008270000000131\\\\\\\",\\\\\\\"workType\\\\\\\":\\\\\\\"Z\\\\\\\",\\\\\\\"workWay\\\\\\\":\\\\\\\"Z\\\\\\\",\\\\\\\"payDay\\\\\\\":16,\\\\\\\"dbOpenBankName\\\\\\\":\\\\\\\"南京银行股份有限公司南京麒麟支行\\\\\\\",\\\\\\\"age\\\\\\\":19}}\\\", \\\"partner\\\": \\\"0006\\\", \\\"service\\\": \\\"WIND_CONTROL_CREDIT\\\", \\\"projectNo\\\": \\\"WS0006200002\\\", \\\"serviceSn\\\": \\\"XFX9JSmRpxJtoxgjh3QX\\\", \\\"timeStamp\\\": \\\"1608616170150\\\", \\\"projectName\\\": null, \\\"serviceVersion\\\": \\\"1\\\"}\", \"partnerEnumStr\": \"GM_PARTNER\"}, \"txnId\": \"16086177005141Node1000028000000\"}";
//        String jsonString = "{\"content\":\"{\\\"preApplyNo\\\":\\\"PREAPP-20201010125512000001\\\",\\\"applyNo\\\":\\\"APP-20201010133308000001\\\",\\\"companyLoanBool\\\":false,\\\"product\\\":{\\\"productNo\\\":\\\"001702\\\",\\\"productName\\\":\\\"二手车融资租赁\\\",\\\"rentalDate\\\":\\\"2020-10-10\\\",\\\"currencyType\\\":\\\"RMB\\\",\\\"applyCity\\\":\\\"天津\\\",\\\"currencyAmt\\\":\\\"52034.00\\\",\\\"loanAmt\\\":\\\"52034.00\\\",\\\"loanTerms\\\":36,\\\"currencyTerms\\\":36,\\\"repayType\\\":\\\"RT01\\\",\\\"loanRateType\\\":\\\"LRT01\\\",\\\"agreementRateInd\\\":\\\"Y\\\",\\\"loanRate\\\":\\\"0.1584\\\",\\\"loanFeeRate\\\":\\\"0\\\",\\\"loanSvcFeeRate\\\":\\\"0\\\",\\\"loanPenaltyRate\\\":\\\"23.76\\\",\\\"guaranteeType\\\":\\\"D\\\",\\\"loanApplyUse\\\":\\\"LAU01\\\"},\\\"borrower\\\":{\\\"openId\\\":\\\"16730dba9a9b7fc6fe063182221bf4e9\\\",\\\"name\\\":\\\"席杰\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"320325198911135091\\\",\\\"idCountry\\\":\\\"中国\\\",\\\"idProvince\\\":\\\"120000\\\",\\\"idCity\\\":\\\"120100\\\",\\\"idAddress\\\":\\\"江苏省邳州市燕子埠镇棠棣埠村东棠棣埠153号\\\",\\\"mobilePhone\\\":\\\"13337938363\\\",\\\"tel\\\":\\\"13337938363\\\",\\\"sex\\\":\\\"\\\",\\\"homeProvince\\\":\\\"天津\\\",\\\"homeCity\\\":\\\"天津\\\",\\\"homeArea\\\":\\\"西青\\\",\\\"homeAddress\\\":\\\"天津天津西青张家窝镇，张家窝村\\\",\\\"homeTel\\\":\\\"13337938363\\\",\\\"maritalStatus\\\":\\\"O\\\",\\\"education\\\":\\\"Z\\\",\\\"industry\\\":\\\"Z\\\",\\\"occpType\\\":\\\"80\\\",\\\"companyName\\\":\\\"安通建筑有限公司\\\",\\\"companyProvince\\\":\\\"天津\\\",\\\"companyCity\\\":\\\"天津\\\",\\\"companyArea\\\":\\\"西青\\\",\\\"companyAddress\\\":\\\"张家窝镇亿洲办公楼\\\",\\\"annualIncomeMin\\\":\\\"\\\",\\\"annualIncomeMax\\\":\\\"\\\",\\\"haveHouse\\\":\\\"Y\\\",\\\"housingArea\\\":\\\"120\\\",\\\"housingValue\\\":\\\"\\\",\\\"drivrLicenNo\\\":\\\"\\\",\\\"drivingExpr\\\":2},\\\"relationalHumans\\\":[{\\\"name\\\":\\\"李志侠\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13357957363\\\",\\\"sex\\\":\\\"\\\",\\\"age\\\":30,\\\"relationship\\\":\\\"O\\\",\\\"relationalHumanType\\\":\\\"RHT01\\\",\\\"province\\\":\\\"天津\\\",\\\"city\\\":\\\"天津\\\",\\\"area\\\":\\\"西青\\\",\\\"address\\\":\\\"天津天津西青张家窝镇，张家窝村\\\"},{\\\"name\\\":\\\"梁杰\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13952275117\\\",\\\"sex\\\":\\\"\\\",\\\"age\\\":30,\\\"relationship\\\":\\\"O\\\",\\\"relationalHumanType\\\":\\\"RHT01\\\",\\\"province\\\":\\\"天津\\\",\\\"city\\\":\\\"天津\\\",\\\"area\\\":\\\"西青\\\",\\\"address\\\":\\\"天津天津西青张家窝镇，张家窝村\\\"}],\\\"guaranties\\\":[{\\\"guarantyType\\\":\\\"CAR\\\",\\\"guarantyNum\\\":\\\"APP-20201010133308000001\\\",\\\"car\\\":{\\\"busiType\\\":\\\"4\\\",\\\"usePurpose\\\":\\\"1\\\",\\\"carType\\\":\\\"2\\\",\\\"cxdm\\\":\\\"2\\\",\\\"carBrand\\\":\\\"MG\\\",\\\"carNo\\\":\\\"名爵ZS\\\",\\\"gpsNo\\\":\\\"\\\",\\\"carFrameNo\\\":\\\"LSJW74U37JZ140396\\\",\\\"engineNo\\\":\\\"SGGJB140003\\\",\\\"licenseNum\\\":null,\\\"carProvince\\\":\\\"\\\",\\\"carCity\\\":\\\"\\\",\\\"vehicleId\\\":\\\"\\\",\\\"vehicleDate\\\":\\\"2019-07-01\\\",\\\"mortgageRegisterDate\\\":\\\"1970-01-01\\\",\\\"color\\\":\\\"红色\\\",\\\"shift\\\":\\\"A\\\",\\\"isSunroof\\\":\\\"NA\\\",\\\"carDiaplace\\\":\\\"1.5L\\\",\\\"carSeats\\\":\\\"4\\\",\\\"sysEvlauateSource\\\":\\\"1\\\",\\\"sysEvlauatePrice\\\":\\\"52034.00\\\",\\\"manEvaluate\\\":\\\"52034.00\\\",\\\"evaluatePrice\\\":\\\"52034.00\\\",\\\"newCarPrice\\\":\\\"0\\\",\\\"carSalePrice\\\":\\\"58700.00\\\",\\\"firstPaymentPercent\\\":\\\"0.30\\\",\\\"firstPaymentAmount\\\":\\\"17700.00\\\",\\\"regDate\\\":\\\"2019-07\\\",\\\"carAdditional\\\":{\\\"buyDate\\\":\\\"2020-10-10\\\",\\\"buyPrice\\\":\\\"52034.00\\\",\\\"sellTimes\\\":1,\\\"mileage\\\":13000,\\\"claimTimes\\\":0,\\\"claimAmount\\\":\\\"0\\\",\\\"productionDate\\\":\\\"2019-07-01\\\"},\\\"extraFundingInfo\\\":[{\\\"extraFundingType\\\":\\\"01\\\",\\\"extraFundingAmount\\\":\\\"1761.00\\\"},{\\\"extraFundingType\\\":\\\"03\\\",\\\"extraFundingAmount\\\":\\\"2350.00\\\"},{\\\"extraFundingType\\\":\\\"05\\\",\\\"extraFundingAmount\\\":\\\"6923.00\\\"}],\\\"agentInfo\\\":{\\\"spName\\\":\\\"\\\",\\\"spProvince\\\":\\\"\\\",\\\"spCity\\\":\\\"\\\",\\\"spDetailAddr\\\":\\\"\\\"}}}],\\\"loanAccount\\\":{\\\"accountType\\\":\\\"BUSINESS\\\",\\\"accountNum\\\":\\\"631979743\\\",\\\"accountName\\\":\\\"瓜子融资租赁（中国）有限公司\\\",\\\"bankCode\\\":\\\"014\\\",\\\"bankName\\\":\\\"民生银行\\\",\\\"branchName\\\":\\\"中国民生银行广州越秀支行\\\",\\\"mobilePhone\\\":\\\"\\\"},\\\"payAccount\\\":{\\\"accountType\\\":\\\"PERSONAL\\\",\\\"accountNum\\\":\\\"6217994610009177039\\\",\\\"accountName\\\":\\\"席杰\\\",\\\"bankCode\\\":\\\"015\\\",\\\"bankName\\\":\\\"邮政储蓄\\\",\\\"branchName\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13337938363\\\"},\\\"auditInfo\\\":{\\\"isCalled\\\":\\\"2\\\"}}\",\"data\":{\"applyNo\":\"APP-20201010133308000001\",\"auditInfo\":{\"isCalled\":\"2\"},\"borrower\":{\"companyAddress\":\"张家窝镇亿洲办公楼\",\"companyArea\":\"西青\",\"companyCity\":\"天津\",\"companyName\":\"安通建筑有限公司\",\"companyProvince\":\"天津\",\"drivingExpr\":2,\"drivrLicenNo\":\"\",\"education\":\"Z\",\"haveHouse\":\"Y\",\"homeAddress\":\"天津天津西青张家窝镇，张家窝村\",\"homeArea\":\"西青\",\"homeCity\":\"天津\",\"homeProvince\":\"天津\",\"homeTel\":\"13337938363\",\"housingArea\":120,\"idAddress\":\"江苏省邳州市燕子埠镇棠棣埠村东棠棣埠153号\",\"idCity\":\"120100\",\"idCountry\":\"中国\",\"idNo\":\"320325198911135091\",\"idProvince\":\"120000\",\"idType\":\"I\",\"industry\":\"Z\",\"maritalStatus\":\"O\",\"mobilePhone\":\"13337938363\",\"name\":\"席杰\",\"occpType\":\"80\",\"openId\":\"16730dba9a9b7fc6fe063182221bf4e9\",\"sex\":\"\",\"tel\":\"13337938363\"},\"companyLoanBool\":false,\"guaranties\":[{\"guarantyNum\":\"APP-20201010133308000001\",\"guarantyType\":\"CAR\",\"car\":{\"carSeats\":\"4\",\"color\":\"红色\",\"agentInfo\":{\"spDetailAddr\":\"\",\"spProvince\":\"\",\"spName\":\"\",\"spCity\":\"\"},\"shift\":\"A\",\"manEvaluate\":\"52034.00\",\"regDate\":\"2019-07\",\"newCarPrice\":\"0\",\"firstPaymentAmount\":\"17700.00\",\"cxdm\":\"2\",\"carType\":\"2\",\"carNo\":\"名爵ZS\",\"carProvince\":\"\",\"sysEvlauatePrice\":\"52034.00\",\"busiType\":\"4\",\"vehicleId\":\"\",\"mortgageRegisterDate\":\"1970-01-01\",\"isSunroof\":\"NA\",\"carAdditional\":{\"buyPrice\":\"52034.00\",\"productionDate\":\"2019-07-01\",\"sellTimes\":1,\"buyDate\":\"2020-10-10\",\"claimAmount\":\"0\",\"claimTimes\":0,\"mileage\":13000},\"carDiaplace\":\"1.5L\",\"engineNo\":\"SGGJB140003\",\"carBrand\":\"MG\",\"sysEvlauateSource\":\"1\",\"carSalePrice\":\"58700.00\",\"evaluatePrice\":\"52034.00\",\"firstPaymentPercent\":\"0.30\",\"carCity\":\"\",\"vehicleDate\":\"2019-07-01\",\"gpsNo\":\"\",\"extraFundingInfo\":[{\"extraFundingAmount\":\"1761.00\",\"extraFundingType\":\"01\"},{\"extraFundingAmount\":\"2350.00\",\"extraFundingType\":\"03\"},{\"extraFundingAmount\":\"6923.00\",\"extraFundingType\":\"05\"}],\"usePurpose\":\"1\",\"carFrameNo\":\"LSJW74U37JZ140396\"}}],\"loanAccount\":{\"accountName\":\"瓜子融资租赁（中国）有限公司\",\"accountNum\":\"631979743\",\"accountType\":\"BUSINESS\",\"bankCode\":\"014\",\"bankName\":\"民生银行\",\"branchName\":\"中国民生银行广州越秀支行\",\"mobilePhone\":\"\"},\"payAccount\":{\"accountName\":\"席杰\",\"accountNum\":\"6217994610009177039\",\"accountType\":\"PERSONAL\",\"bankCode\":\"015\",\"bankName\":\"邮政储蓄\",\"branchName\":\"\",\"mobilePhone\":\"13337938363\"},\"preApplyNo\":\"PREAPP-20201010125512000001\",\"product\":{\"agreementRateInd\":\"Y\",\"applyCity\":\"天津\",\"currencyAmt\":52034.00,\"currencyTerms\":36,\"currencyType\":\"RMB\",\"guaranteeType\":\"D\",\"loanAmt\":52034.00,\"loanApplyUse\":\"LAU01\",\"loanFeeRate\":0,\"loanPenaltyRate\":23.76,\"loanRate\":0.1584,\"loanRateType\":\"LRT01\",\"loanSvcFeeRate\":0,\"loanTerms\":36,\"productName\":\"二手车融资租赁\",\"productNo\":\"001702\",\"rentalDate\":\"2020-10-10\",\"repayType\":\"RT01\"},\"relationalHumans\":[{\"address\":\"天津天津西青张家窝镇，张家窝村\",\"age\":\"30\",\"area\":\"西青\",\"city\":\"天津\",\"idNo\":\"\",\"idType\":\"I\",\"mobilePhone\":\"13357957363\",\"name\":\"李志侠\",\"province\":\"天津\",\"relationalHumanType\":\"RHT01\",\"relationship\":\"O\",\"sex\":\"\"},{\"address\":\"天津天津西青张家窝镇，张家窝村\",\"age\":\"30\",\"area\":\"西青\",\"city\":\"天津\",\"idNo\":\"\",\"idType\":\"I\",\"mobilePhone\":\"13952275117\",\"name\":\"梁杰\",\"province\":\"天津\",\"relationalHumanType\":\"RHT01\",\"relationship\":\"O\",\"sex\":\"\"}]},\"partner\":\"0005\",\"projectName\":\"民生租赁-瓜子-2020一期\",\"projectNo\":\"WS0005200001\",\"service\":\"incomeApply\",\"serviceSn\":\"40c4fbaab16a4329b44d91b35427fadb\",\"serviceVersion\":\"1\",\"sign\":\"c5jBx5kF70xjtqgXcviDsAxvbG6DoZPdvNTDjBzL7x1g9c778kMnKPTdP7VFAUE2Ywlb6VdsBjFY\\nkFwTs9fn6yb2B8HEUhL/75TzdlntfPbsMvAwmbXC4wrIeYmTn2kUo9Hzz3iwtWrtGaBlhsLhTs74\\n87h8+71aem6gv/PjKY4=\\n\",\"timeStamp\":\"1602307989607\"}";
//        String jsonString = "{\"userInfo\": {\"name\": \"温庆东\", \"ocrInfo\": {\"validDate\": \"2019.09.18-2039.09.18\"}, \"userRole\": \"P\"} }";
        String jsonString = "{\"applicationId\":\"DD00003036201911250933d6602e8c\",\"applySource\":\"2\",\"creditInfo\":{\"amount\":500000,\"endDate\":\"2020-11-24 00:00:00\",\"interestPenaltyRate\":999,\"interestRate\":666,\"startDate\":\"2019-11-25 00:00:00\"},\"creditResultStatus\":\"Yes\",\"didiRcFeature\":{\"deviceStability1Year\":\"-9999\",\"costLevel3M\":\"1\",\"registMonthLevel\":\"5\",\"costLevel1M\":\"1\",\"nightOrderNumLevel1M\":\"5\",\"isCommonlyUsedLoc\":\"-9999\",\"orderStartingStabilityLevel\":\"1\",\"phone2ImeiLevel3M\":\"3\",\"isImeiFreqChange3M\":\"0\",\"phoneNo2ImeiLevel3M\":\"-9999\",\"riskScore\":\"767\",\"consumeStabilityLevel\":\"1\",\"isCommonlyUsedDev\":\"0\"},\"userInfo\":{\"address\":\"-·昝张家口市宣化区西街横巷5号楼4单元304号\",\"bankCardNo\":\"\",\"cardNo\":\"130705197806102710\",\"idCardValidDate\":\"2039.09.18\",\"name\":\"温庆东\",\"ocrInfo\":{\"address\":\"-·昝张家口市宣化区西街横巷5号楼4单元304号\",\"authority\":\"张家口市公安局宣化分局\",\"birthday\":\"1978-06-10\",\"gender\":\"男\",\"idNo\":\"130705197806102710\",\"name\":\"温庆东\",\"race\":\"汉\",\"validDate\":\"2019.09.18-2039.09.18\"},\"phone\":\"13931314343\",\"telephone\":\"13931314343\",\"userRole\":\"P\"}}";
//        String jsonString = "{\"content\": \"{\\\"preApplyNo\\\":\\\"PREAPP-20201010125512000001\\\",\\\"applyNo\\\":\\\"APP-20201010133308000001\\\",\\\"companyLoanBool\\\":false,\\\"product\\\":{\\\"productNo\\\":\\\"001702\\\",\\\"productName\\\":\\\"二手车融资租赁\\\",\\\"rentalDate\\\":\\\"2020-10-10\\\",\\\"currencyType\\\":\\\"RMB\\\",\\\"applyCity\\\":\\\"天津\\\",\\\"currencyAmt\\\":\\\"52034.00\\\",\\\"loanAmt\\\":\\\"52034.00\\\",\\\"loanTerms\\\":36,\\\"currencyTerms\\\":36,\\\"repayType\\\":\\\"RT01\\\",\\\"loanRateType\\\":\\\"LRT01\\\",\\\"agreementRateInd\\\":\\\"Y\\\",\\\"loanRate\\\":\\\"0.1584\\\",\\\"loanFeeRate\\\":\\\"0\\\",\\\"loanSvcFeeRate\\\":\\\"0\\\",\\\"loanPenaltyRate\\\":\\\"23.76\\\",\\\"guaranteeType\\\":\\\"D\\\",\\\"loanApplyUse\\\":\\\"LAU01\\\"},\\\"borrower\\\":{\\\"openId\\\":\\\"16730dba9a9b7fc6fe063182221bf4e9\\\",\\\"name\\\":\\\"席杰\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"320325198911135091\\\",\\\"idCountry\\\":\\\"中国\\\",\\\"idProvince\\\":\\\"120000\\\",\\\"idCity\\\":\\\"120100\\\",\\\"idAddress\\\":\\\"江苏省邳州市燕子埠镇棠棣埠村东棠棣埠153号\\\",\\\"mobilePhone\\\":\\\"13337938363\\\",\\\"tel\\\":\\\"13337938363\\\",\\\"sex\\\":\\\"\\\",\\\"homeProvince\\\":\\\"天津\\\",\\\"homeCity\\\":\\\"天津\\\",\\\"homeArea\\\":\\\"西青\\\",\\\"homeAddress\\\":\\\"天津天津西青张家窝镇，张家窝村\\\",\\\"homeTel\\\":\\\"13337938363\\\",\\\"maritalStatus\\\":\\\"O\\\",\\\"education\\\":\\\"Z\\\",\\\"industry\\\":\\\"Z\\\",\\\"occpType\\\":\\\"80\\\",\\\"companyName\\\":\\\"安通建筑有限公司\\\",\\\"companyProvince\\\":\\\"天津\\\",\\\"companyCity\\\":\\\"天津\\\",\\\"companyArea\\\":\\\"西青\\\",\\\"companyAddress\\\":\\\"张家窝镇亿洲办公楼\\\",\\\"annualIncomeMin\\\":\\\"\\\",\\\"annualIncomeMax\\\":\\\"\\\",\\\"haveHouse\\\":\\\"Y\\\",\\\"housingArea\\\":\\\"120\\\",\\\"housingValue\\\":\\\"\\\",\\\"drivrLicenNo\\\":\\\"\\\",\\\"drivingExpr\\\":2},\\\"relationalHumans\\\":[{\\\"name\\\":\\\"李志侠\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13357957363\\\",\\\"sex\\\":\\\"\\\",\\\"age\\\":30,\\\"relationship\\\":\\\"O\\\",\\\"relationalHumanType\\\":\\\"RHT01\\\",\\\"province\\\":\\\"天津\\\",\\\"city\\\":\\\"天津\\\",\\\"area\\\":\\\"西青\\\",\\\"address\\\":\\\"天津天津西青张家窝镇，张家窝村\\\"},{\\\"name\\\":\\\"梁杰\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13952275117\\\",\\\"sex\\\":\\\"\\\",\\\"age\\\":30,\\\"relationship\\\":\\\"O\\\",\\\"relationalHumanType\\\":\\\"RHT01\\\",\\\"province\\\":\\\"天津\\\",\\\"city\\\":\\\"天津\\\",\\\"area\\\":\\\"西青\\\",\\\"address\\\":\\\"天津天津西青张家窝镇，张家窝村\\\"}],\\\"guaranties\\\":[{\\\"guarantyType\\\":\\\"CAR\\\",\\\"guarantyNum\\\":\\\"APP-20201010133308000001\\\",\\\"car\\\":{\\\"busiType\\\":\\\"4\\\",\\\"usePurpose\\\":\\\"1\\\",\\\"carType\\\":\\\"2\\\",\\\"cxdm\\\":\\\"2\\\",\\\"carBrand\\\":\\\"MG\\\",\\\"carNo\\\":\\\"名爵ZS\\\",\\\"gpsNo\\\":\\\"\\\",\\\"carFrameNo\\\":\\\"LSJW74U37JZ140396\\\",\\\"engineNo\\\":\\\"SGGJB140003\\\",\\\"licenseNum\\\":null,\\\"carProvince\\\":\\\"\\\",\\\"carCity\\\":\\\"\\\",\\\"vehicleId\\\":\\\"\\\",\\\"vehicleDate\\\":\\\"2019-07-01\\\",\\\"mortgageRegisterDate\\\":\\\"1970-01-01\\\",\\\"color\\\":\\\"红色\\\",\\\"shift\\\":\\\"A\\\",\\\"isSunroof\\\":\\\"NA\\\",\\\"carDiaplace\\\":\\\"1.5L\\\",\\\"carSeats\\\":\\\"4\\\",\\\"sysEvlauateSource\\\":\\\"1\\\",\\\"sysEvlauatePrice\\\":\\\"52034.00\\\",\\\"manEvaluate\\\":\\\"52034.00\\\",\\\"evaluatePrice\\\":\\\"52034.00\\\",\\\"newCarPrice\\\":\\\"0\\\",\\\"carSalePrice\\\":\\\"58700.00\\\",\\\"firstPaymentPercent\\\":\\\"0.30\\\",\\\"firstPaymentAmount\\\":\\\"17700.00\\\",\\\"regDate\\\":\\\"2019-07\\\",\\\"carAdditional\\\":{\\\"buyDate\\\":\\\"2020-10-10\\\",\\\"buyPrice\\\":\\\"52034.00\\\",\\\"sellTimes\\\":1,\\\"mileage\\\":13000,\\\"claimTimes\\\":0,\\\"claimAmount\\\":\\\"0\\\",\\\"productionDate\\\":\\\"2019-07-01\\\"},\\\"extraFundingInfo\\\":[{\\\"extraFundingType\\\":\\\"01\\\",\\\"extraFundingAmount\\\":\\\"1761.00\\\"},{\\\"extraFundingType\\\":\\\"03\\\",\\\"extraFundingAmount\\\":\\\"2350.00\\\"},{\\\"extraFundingType\\\":\\\"05\\\",\\\"extraFundingAmount\\\":\\\"6923.00\\\"}],\\\"agentInfo\\\":{\\\"spName\\\":\\\"\\\",\\\"spProvince\\\":\\\"\\\",\\\"spCity\\\":\\\"\\\",\\\"spDetailAddr\\\":\\\"\\\"}}}],\\\"loanAccount\\\":{\\\"accountType\\\":\\\"BUSINESS\\\",\\\"accountNum\\\":\\\"631979743\\\",\\\"accountName\\\":\\\"瓜子融资租赁（中国）有限公司\\\",\\\"bankCode\\\":\\\"014\\\",\\\"bankName\\\":\\\"民生银行\\\",\\\"branchName\\\":\\\"中国民生银行广州越秀支行\\\",\\\"mobilePhone\\\":\\\"\\\"},\\\"payAccount\\\":{\\\"accountType\\\":\\\"PERSONAL\\\",\\\"accountNum\\\":\\\"6217994610009177039\\\",\\\"accountName\\\":\\\"席杰\\\",\\\"bankCode\\\":\\\"015\\\",\\\"bankName\\\":\\\"邮政储蓄\\\",\\\"branchName\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13337938363\\\"},\\\"auditInfo\\\":{\\\"isCalled\\\":\\\"2\\\"}}\", \"sign\": \"c5jBx5kF70xjtqgXcviDsAxvbG6DoZPdvNTDjBzL7x1g9c778kMnKPTdP7VFAUE2Ywlb6VdsBjFY\\nkFwTs9fn6yb2B8HEUhL/75TzdlntfPbsMvAwmbXC4wrIeYmTn2kUo9Hzz3iwtWrtGaBlhsLhTs74\\n87h8+71aem6gv/PjKY4=\\n\"}";

//        System.out.println(new JsonString2StringUDF().evaluate(jsonString));
//        System.out.println(regexString2jsonJackson(JsonUtil.fromJson(jsonString, JsonNode.class)));
    }

    private JsonNode regexString2jsonJackson(JsonNode jsonNode) {
        System.out.println(jsonNode.getNodeType());
        Iterator<Map.Entry<String, JsonNode>> entryIterator = jsonNode.fields();
        System.out.println(entryIterator);
        while (entryIterator.hasNext()) {
            Map.Entry<String, JsonNode> entry = entryIterator.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            JsonNode node = regexString2jsonJackson(value);
            System.out.println("aa--> " + key + "\naa--> " + value + "\naa--> " + node);
            System.out.println();
            jsonNode = new ObjectMapper().createObjectNode().set(key, value);
        }
        return jsonNode;
    }

    @Test
    public void test12() throws JsonProcessingException {
//        String jsonString = "{\"org\": \"15601\", \"partner\": \"0006\", \"reqContent\": {\"jsonResp\": \"{\\\"sign\\\": null, \\\"partner\\\": \\\"0006\\\", \\\"rspData\\\": \\\"{\\\\\\\"consumptionLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"amtOfPerfermanceLoansLast3M\\\\\\\":99,\\\\\\\"hitDeadbeatList\\\\\\\":0,\\\\\\\"appsOfConFinOrgLastTwoYear\\\\\\\":99,\\\\\\\"overdueTimesOfCreditApplyLast4M\\\\\\\":99,\\\\\\\"overdueLoansOfMoreThan1DayLast6M\\\\\\\":99,\\\\\\\"catPool\\\\\\\":0,\\\\\\\"LastFinancialQuery\\\\\\\":99,\\\\\\\"scoreLevel\\\\\\\":\\\\\\\"B\\\\\\\",\\\\\\\"countOfInstallitionOfLoanAppLast2M\\\\\\\":99,\\\\\\\"countOfUninstallAppsOfLoanLast3M\\\\\\\":99,\\\\\\\"blkListLoc\\\\\\\":0,\\\\\\\"averageDailyOpenTimesOfFinAppsLastM\\\\\\\":99,\\\\\\\"timesOfUninstallFinAppsLast15D\\\\\\\":99,\\\\\\\"fraudIndustry\\\\\\\":0,\\\\\\\"sumCreditOfWebLoan\\\\\\\":99,\\\\\\\"suspiciousDevice\\\\\\\":0,\\\\\\\"daysOfLocationUploadLats9M\\\\\\\":99,\\\\\\\"accountHacked\\\\\\\":0,\\\\\\\"marriage\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"virtualMaliciousStatus\\\\\\\":0,\\\\\\\"accountForInTownInWorkDayLastM\\\\\\\":99,\\\\\\\"daysOfEarliestRegisterOfLoanAppLast9M\\\\\\\":99,\\\\\\\"applyByIDLastYear\\\\\\\":99,\\\\\\\"gamerArbitrageStatus\\\\\\\":0,\\\\\\\"abnormalPayment\\\\\\\":0,\\\\\\\"usageRateOfCompusLoanApp\\\\\\\":99,\\\\\\\"pass\\\\\\\":\\\\\\\"Yes\\\\\\\",\\\\\\\"cityLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"appsOfConFinProLastTwoYear\\\\\\\":99,\\\\\\\"countOfNoticesOfFinMessageLast9M\\\\\\\":99,\\\\\\\"financAbility\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"incomeLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"forgedIdStatus\\\\\\\":0,\\\\\\\"rejectTimesOfCreditApplyLast4M\\\\\\\":99,\\\\\\\"riskOfDevice\\\\\\\":99,\\\\\\\"idTheftStatus\\\\\\\":0,\\\\\\\"abnormalAccount\\\\\\\":0,\\\\\\\"edu\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"accountForInstallBussinessAppsLast4M\\\\\\\":99,\\\\\\\"blkList2\\\\\\\":0,\\\\\\\"blkList1\\\\\\\":0,\\\\\\\"accountForWifiUseTimeSpanLast5M\\\\\\\":99,\\\\\\\"loanAmtLast3M\\\\\\\":99,\\\\\\\"countOfRegisterAppsOfFinLastM\\\\\\\":99,\\\\\\\"counterfeitAgencyStatus\\\\\\\":0,\\\\\\\"status\\\\\\\":\\\\\\\"1\\\\\\\"}\\\", \\\"service\\\": \\\"WIND_CONTROL_CREDIT\\\", \\\"resultMsg\\\": \\\"请求成功\\\", \\\"resultCode\\\": \\\"0000\\\", \\\"serviceVersion\\\": \\\"1\\\"}\", \"reqMsgCreateDate\": \"2020-12-22 13:49:31\", \"jsonReq\": \"{\\\"sign\\\": \\\"rtAHHYtVX0he+npSXuZl0fMxGBVyoa1gm1vNux3OJYjhUYoNTpTqXkrBCbtLE6Pr5BRgKU58dcrz\\\\nGDtNH+k5szX/F2nmko25ouo9Go7IZXUfSosMKHLlatUDLoYRGM7MUZLxKs0GY/guwrZR1Hf1IfmM\\\\nwIhB/fAfED3ujRdAI0o=\\\\n\\\", \\\"content\\\": \\\"{\\\\\\\"reqData\\\\\\\":{\\\\\\\"ifCar\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"country\\\\\\\":\\\\\\\"CHN\\\\\\\",\\\\\\\"authAccountName\\\\\\\":\\\\\\\"王香皖\\\\\\\",\\\\\\\"dbBankCode\\\\\\\":\\\\\\\"313301008887\\\\\\\",\\\\\\\"lnRate\\\\\\\":2.100000,\\\\\\\"endDate\\\\\\\":\\\\\\\"2021-12-16\\\\\\\",\\\\\\\"workSts\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"workDuty\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"homeAddr\\\\\\\":\\\\\\\"安徽省阜阳市颍泉区清颍路131号10幢202户\\\\\\\",\\\\\\\"idNo\\\\\\\":\\\\\\\"341204200105190421\\\\\\\",\\\\\\\"sales\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"homeCode\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"ifCarCred\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"payType\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"isBelowRisk\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"children\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"vouType\\\\\\\":\\\\\\\"4\\\\\\\",\\\\\\\"creditLimit\\\\\\\":7165.00,\\\\\\\"cardAmt\\\\\\\":0.00,\\\\\\\"profession\\\\\\\":\\\\\\\"12\\\\\\\",\\\\\\\"idType\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"authNo\\\\\\\":\\\\\\\"1120122213485525204565\\\\\\\",\\\\\\\"mincome\\\\\\\":1000.00,\\\\\\\"postAddr\\\\\\\":\\\\\\\"安徽省阜阳市颍泉区清颍路131号10幢202户\\\\\\\",\\\\\\\"workTitle\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"ifCard\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"degree\\\\\\\":\\\\\\\"4\\\\\\\",\\\\\\\"ifAgent\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"birth\\\\\\\":\\\\\\\"20010519\\\\\\\",\\\\\\\"ifId\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"workName\\\\\\\":\\\\\\\"无\\\\\\\",\\\\\\\"prodType\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"telNo\\\\\\\":\\\\\\\"18856825195\\\\\\\",\\\\\\\"pactAmt\\\\\\\":14.40,\\\\\\\"ifPact\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"ifApp\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"edu\\\\\\\":\\\\\\\"20\\\\\\\",\\\\\\\"custType\\\\\\\":\\\\\\\"04\\\\\\\",\\\\\\\"loanDate\\\\\\\":\\\\\\\"2020-12-22\\\\\\\",\\\\\\\"postCode\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"hasOverdueLoan\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"income\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"appUse\\\\\\\":\\\\\\\"07\\\\\\\",\\\\\\\"riskLevel\\\\\\\":\\\\\\\"R3\\\\\\\",\\\\\\\"proCode\\\\\\\":\\\\\\\"002001\\\\\\\",\\\\\\\"zxhomeIncome\\\\\\\":12000.00,\\\\\\\"rpyMethod\\\\\\\":\\\\\\\"03\\\\\\\",\\\\\\\"launder\\\\\\\":\\\\\\\"03\\\\\\\",\\\\\\\"dbAccountName\\\\\\\":\\\\\\\"深圳市分期乐网络科技有限公司\\\\\\\",\\\\\\\"phoneNo\\\\\\\":\\\\\\\"18856825195\\\\\\\",\\\\\\\"ifMort\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"loanTime\\\\\\\":\\\\\\\"2020-12-22 13:49:16\\\\\\\",\\\\\\\"marriage\\\\\\\":\\\\\\\"10\\\\\\\",\\\\\\\"customerId\\\\\\\":\\\\\\\"148380565\\\\\\\",\\\\\\\"authBankName\\\\\\\":\\\\\\\"工商银行\\\\\\\",\\\\\\\"idEndDate\\\\\\\":\\\\\\\"2021-07-11\\\\\\\",\\\\\\\"idPreDate\\\\\\\":\\\\\\\"2016-07-11\\\\\\\",\\\\\\\"sex\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"ifRoom\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"appArea\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"ifLaunder\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"custName\\\\\\\":\\\\\\\"王香皖\\\\\\\",\\\\\\\"homeIncome\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"dbBankName\\\\\\\":\\\\\\\"南京银行\\\\\\\",\\\\\\\"authBankAccount\\\\\\\":\\\\\\\"6215581311005946674\\\\\\\",\\\\\\\"homeArea\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"homeSts\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"loanTerm\\\\\\\":12,\\\\\\\"trade\\\\\\\":\\\\\\\"26\\\\\\\",\\\\\\\"creditCoef\\\\\\\":2.100000,\\\\\\\"applyNo\\\\\\\":\\\\\\\"1120122213485525204565\\\\\\\",\\\\\\\"dbBankAccount\\\\\\\":\\\\\\\"2008270000000131\\\\\\\",\\\\\\\"workType\\\\\\\":\\\\\\\"Z\\\\\\\",\\\\\\\"workWay\\\\\\\":\\\\\\\"Z\\\\\\\",\\\\\\\"payDay\\\\\\\":16,\\\\\\\"dbOpenBankName\\\\\\\":\\\\\\\"南京银行股份有限公司南京麒麟支行\\\\\\\",\\\\\\\"age\\\\\\\":19}}\\\", \\\"partner\\\": \\\"0006\\\", \\\"service\\\": \\\"WIND_CONTROL_CREDIT\\\", \\\"projectNo\\\": \\\"WS0006200002\\\", \\\"serviceSn\\\": \\\"XFX9JSmRpxJtoxgjh3QX\\\", \\\"timeStamp\\\": \\\"1608616170150\\\", \\\"projectName\\\": null, \\\"serviceVersion\\\": \\\"1\\\"}\", \"partnerEnumStr\": \"GM_PARTNER\"}, \"txnId\": \"16086177005141Node1000028000000\"}";
//        String jsonString = "{\"content\":\"{\\\"preApplyNo\\\":\\\"PREAPP-20201010125512000001\\\",\\\"applyNo\\\":\\\"APP-20201010133308000001\\\",\\\"companyLoanBool\\\":false,\\\"product\\\":{\\\"productNo\\\":\\\"001702\\\",\\\"productName\\\":\\\"二手车融资租赁\\\",\\\"rentalDate\\\":\\\"2020-10-10\\\",\\\"currencyType\\\":\\\"RMB\\\",\\\"applyCity\\\":\\\"天津\\\",\\\"currencyAmt\\\":\\\"52034.00\\\",\\\"loanAmt\\\":\\\"52034.00\\\",\\\"loanTerms\\\":36,\\\"currencyTerms\\\":36,\\\"repayType\\\":\\\"RT01\\\",\\\"loanRateType\\\":\\\"LRT01\\\",\\\"agreementRateInd\\\":\\\"Y\\\",\\\"loanRate\\\":\\\"0.1584\\\",\\\"loanFeeRate\\\":\\\"0\\\",\\\"loanSvcFeeRate\\\":\\\"0\\\",\\\"loanPenaltyRate\\\":\\\"23.76\\\",\\\"guaranteeType\\\":\\\"D\\\",\\\"loanApplyUse\\\":\\\"LAU01\\\"},\\\"borrower\\\":{\\\"openId\\\":\\\"16730dba9a9b7fc6fe063182221bf4e9\\\",\\\"name\\\":\\\"席杰\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"320325198911135091\\\",\\\"idCountry\\\":\\\"中国\\\",\\\"idProvince\\\":\\\"120000\\\",\\\"idCity\\\":\\\"120100\\\",\\\"idAddress\\\":\\\"江苏省邳州市燕子埠镇棠棣埠村东棠棣埠153号\\\",\\\"mobilePhone\\\":\\\"13337938363\\\",\\\"tel\\\":\\\"13337938363\\\",\\\"sex\\\":\\\"\\\",\\\"homeProvince\\\":\\\"天津\\\",\\\"homeCity\\\":\\\"天津\\\",\\\"homeArea\\\":\\\"西青\\\",\\\"homeAddress\\\":\\\"天津天津西青张家窝镇，张家窝村\\\",\\\"homeTel\\\":\\\"13337938363\\\",\\\"maritalStatus\\\":\\\"O\\\",\\\"education\\\":\\\"Z\\\",\\\"industry\\\":\\\"Z\\\",\\\"occpType\\\":\\\"80\\\",\\\"companyName\\\":\\\"安通建筑有限公司\\\",\\\"companyProvince\\\":\\\"天津\\\",\\\"companyCity\\\":\\\"天津\\\",\\\"companyArea\\\":\\\"西青\\\",\\\"companyAddress\\\":\\\"张家窝镇亿洲办公楼\\\",\\\"annualIncomeMin\\\":\\\"\\\",\\\"annualIncomeMax\\\":\\\"\\\",\\\"haveHouse\\\":\\\"Y\\\",\\\"housingArea\\\":\\\"120\\\",\\\"housingValue\\\":\\\"\\\",\\\"drivrLicenNo\\\":\\\"\\\",\\\"drivingExpr\\\":2},\\\"relationalHumans\\\":[{\\\"name\\\":\\\"李志侠\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13357957363\\\",\\\"sex\\\":\\\"\\\",\\\"age\\\":30,\\\"relationship\\\":\\\"O\\\",\\\"relationalHumanType\\\":\\\"RHT01\\\",\\\"province\\\":\\\"天津\\\",\\\"city\\\":\\\"天津\\\",\\\"area\\\":\\\"西青\\\",\\\"address\\\":\\\"天津天津西青张家窝镇，张家窝村\\\"},{\\\"name\\\":\\\"梁杰\\\",\\\"idType\\\":\\\"I\\\",\\\"idNo\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13952275117\\\",\\\"sex\\\":\\\"\\\",\\\"age\\\":30,\\\"relationship\\\":\\\"O\\\",\\\"relationalHumanType\\\":\\\"RHT01\\\",\\\"province\\\":\\\"天津\\\",\\\"city\\\":\\\"天津\\\",\\\"area\\\":\\\"西青\\\",\\\"address\\\":\\\"天津天津西青张家窝镇，张家窝村\\\"}],\\\"guaranties\\\":[{\\\"guarantyType\\\":\\\"CAR\\\",\\\"guarantyNum\\\":\\\"APP-20201010133308000001\\\",\\\"car\\\":{\\\"busiType\\\":\\\"4\\\",\\\"usePurpose\\\":\\\"1\\\",\\\"carType\\\":\\\"2\\\",\\\"cxdm\\\":\\\"2\\\",\\\"carBrand\\\":\\\"MG\\\",\\\"carNo\\\":\\\"名爵ZS\\\",\\\"gpsNo\\\":\\\"\\\",\\\"carFrameNo\\\":\\\"LSJW74U37JZ140396\\\",\\\"engineNo\\\":\\\"SGGJB140003\\\",\\\"licenseNum\\\":null,\\\"carProvince\\\":\\\"\\\",\\\"carCity\\\":\\\"\\\",\\\"vehicleId\\\":\\\"\\\",\\\"vehicleDate\\\":\\\"2019-07-01\\\",\\\"mortgageRegisterDate\\\":\\\"1970-01-01\\\",\\\"color\\\":\\\"红色\\\",\\\"shift\\\":\\\"A\\\",\\\"isSunroof\\\":\\\"NA\\\",\\\"carDiaplace\\\":\\\"1.5L\\\",\\\"carSeats\\\":\\\"4\\\",\\\"sysEvlauateSource\\\":\\\"1\\\",\\\"sysEvlauatePrice\\\":\\\"52034.00\\\",\\\"manEvaluate\\\":\\\"52034.00\\\",\\\"evaluatePrice\\\":\\\"52034.00\\\",\\\"newCarPrice\\\":\\\"0\\\",\\\"carSalePrice\\\":\\\"58700.00\\\",\\\"firstPaymentPercent\\\":\\\"0.30\\\",\\\"firstPaymentAmount\\\":\\\"17700.00\\\",\\\"regDate\\\":\\\"2019-07\\\",\\\"carAdditional\\\":{\\\"buyDate\\\":\\\"2020-10-10\\\",\\\"buyPrice\\\":\\\"52034.00\\\",\\\"sellTimes\\\":1,\\\"mileage\\\":13000,\\\"claimTimes\\\":0,\\\"claimAmount\\\":\\\"0\\\",\\\"productionDate\\\":\\\"2019-07-01\\\"},\\\"extraFundingInfo\\\":[{\\\"extraFundingType\\\":\\\"01\\\",\\\"extraFundingAmount\\\":\\\"1761.00\\\"},{\\\"extraFundingType\\\":\\\"03\\\",\\\"extraFundingAmount\\\":\\\"2350.00\\\"},{\\\"extraFundingType\\\":\\\"05\\\",\\\"extraFundingAmount\\\":\\\"6923.00\\\"}],\\\"agentInfo\\\":{\\\"spName\\\":\\\"\\\",\\\"spProvince\\\":\\\"\\\",\\\"spCity\\\":\\\"\\\",\\\"spDetailAddr\\\":\\\"\\\"}}}],\\\"loanAccount\\\":{\\\"accountType\\\":\\\"BUSINESS\\\",\\\"accountNum\\\":\\\"631979743\\\",\\\"accountName\\\":\\\"瓜子融资租赁（中国）有限公司\\\",\\\"bankCode\\\":\\\"014\\\",\\\"bankName\\\":\\\"民生银行\\\",\\\"branchName\\\":\\\"中国民生银行广州越秀支行\\\",\\\"mobilePhone\\\":\\\"\\\"},\\\"payAccount\\\":{\\\"accountType\\\":\\\"PERSONAL\\\",\\\"accountNum\\\":\\\"6217994610009177039\\\",\\\"accountName\\\":\\\"席杰\\\",\\\"bankCode\\\":\\\"015\\\",\\\"bankName\\\":\\\"邮政储蓄\\\",\\\"branchName\\\":\\\"\\\",\\\"mobilePhone\\\":\\\"13337938363\\\"},\\\"auditInfo\\\":{\\\"isCalled\\\":\\\"2\\\"}}\",\"data\":{\"applyNo\":\"APP-20201010133308000001\",\"auditInfo\":{\"isCalled\":\"2\"},\"borrower\":{\"companyAddress\":\"张家窝镇亿洲办公楼\",\"companyArea\":\"西青\",\"companyCity\":\"天津\",\"companyName\":\"安通建筑有限公司\",\"companyProvince\":\"天津\",\"drivingExpr\":2,\"drivrLicenNo\":\"\",\"education\":\"Z\",\"haveHouse\":\"Y\",\"homeAddress\":\"天津天津西青张家窝镇，张家窝村\",\"homeArea\":\"西青\",\"homeCity\":\"天津\",\"homeProvince\":\"天津\",\"homeTel\":\"13337938363\",\"housingArea\":120,\"idAddress\":\"江苏省邳州市燕子埠镇棠棣埠村东棠棣埠153号\",\"idCity\":\"120100\",\"idCountry\":\"中国\",\"idNo\":\"320325198911135091\",\"idProvince\":\"120000\",\"idType\":\"I\",\"industry\":\"Z\",\"maritalStatus\":\"O\",\"mobilePhone\":\"13337938363\",\"name\":\"席杰\",\"occpType\":\"80\",\"openId\":\"16730dba9a9b7fc6fe063182221bf4e9\",\"sex\":\"\",\"tel\":\"13337938363\"},\"companyLoanBool\":false,\"guaranties\":[{\"guarantyNum\":\"APP-20201010133308000001\",\"guarantyType\":\"CAR\",\"car\":{\"carSeats\":\"4\",\"color\":\"红色\",\"agentInfo\":{\"spDetailAddr\":\"\",\"spProvince\":\"\",\"spName\":\"\",\"spCity\":\"\"},\"shift\":\"A\",\"manEvaluate\":\"52034.00\",\"regDate\":\"2019-07\",\"newCarPrice\":\"0\",\"firstPaymentAmount\":\"17700.00\",\"cxdm\":\"2\",\"carType\":\"2\",\"carNo\":\"名爵ZS\",\"carProvince\":\"\",\"sysEvlauatePrice\":\"52034.00\",\"busiType\":\"4\",\"vehicleId\":\"\",\"mortgageRegisterDate\":\"1970-01-01\",\"isSunroof\":\"NA\",\"carAdditional\":{\"buyPrice\":\"52034.00\",\"productionDate\":\"2019-07-01\",\"sellTimes\":1,\"buyDate\":\"2020-10-10\",\"claimAmount\":\"0\",\"claimTimes\":0,\"mileage\":13000},\"carDiaplace\":\"1.5L\",\"engineNo\":\"SGGJB140003\",\"carBrand\":\"MG\",\"sysEvlauateSource\":\"1\",\"carSalePrice\":\"58700.00\",\"evaluatePrice\":\"52034.00\",\"firstPaymentPercent\":\"0.30\",\"carCity\":\"\",\"vehicleDate\":\"2019-07-01\",\"gpsNo\":\"\",\"extraFundingInfo\":[{\"extraFundingAmount\":\"1761.00\",\"extraFundingType\":\"01\"},{\"extraFundingAmount\":\"2350.00\",\"extraFundingType\":\"03\"},{\"extraFundingAmount\":\"6923.00\",\"extraFundingType\":\"05\"}],\"usePurpose\":\"1\",\"carFrameNo\":\"LSJW74U37JZ140396\"}}],\"loanAccount\":{\"accountName\":\"瓜子融资租赁（中国）有限公司\",\"accountNum\":\"631979743\",\"accountType\":\"BUSINESS\",\"bankCode\":\"014\",\"bankName\":\"民生银行\",\"branchName\":\"中国民生银行广州越秀支行\",\"mobilePhone\":\"\"},\"payAccount\":{\"accountName\":\"席杰\",\"accountNum\":\"6217994610009177039\",\"accountType\":\"PERSONAL\",\"bankCode\":\"015\",\"bankName\":\"邮政储蓄\",\"branchName\":\"\",\"mobilePhone\":\"13337938363\"},\"preApplyNo\":\"PREAPP-20201010125512000001\",\"product\":{\"agreementRateInd\":\"Y\",\"applyCity\":\"天津\",\"currencyAmt\":52034.00,\"currencyTerms\":36,\"currencyType\":\"RMB\",\"guaranteeType\":\"D\",\"loanAmt\":52034.00,\"loanApplyUse\":\"LAU01\",\"loanFeeRate\":0,\"loanPenaltyRate\":23.76,\"loanRate\":0.1584,\"loanRateType\":\"LRT01\",\"loanSvcFeeRate\":0,\"loanTerms\":36,\"productName\":\"二手车融资租赁\",\"productNo\":\"001702\",\"rentalDate\":\"2020-10-10\",\"repayType\":\"RT01\"},\"relationalHumans\":[{\"address\":\"天津天津西青张家窝镇，张家窝村\",\"age\":\"30\",\"area\":\"西青\",\"city\":\"天津\",\"idNo\":\"\",\"idType\":\"I\",\"mobilePhone\":\"13357957363\",\"name\":\"李志侠\",\"province\":\"天津\",\"relationalHumanType\":\"RHT01\",\"relationship\":\"O\",\"sex\":\"\"},{\"address\":\"天津天津西青张家窝镇，张家窝村\",\"age\":\"30\",\"area\":\"西青\",\"city\":\"天津\",\"idNo\":\"\",\"idType\":\"I\",\"mobilePhone\":\"13952275117\",\"name\":\"梁杰\",\"province\":\"天津\",\"relationalHumanType\":\"RHT01\",\"relationship\":\"O\",\"sex\":\"\"}]},\"partner\":\"0005\",\"projectName\":\"民生租赁-瓜子-2020一期\",\"projectNo\":\"WS0005200001\",\"service\":\"incomeApply\",\"serviceSn\":\"40c4fbaab16a4329b44d91b35427fadb\",\"serviceVersion\":\"1\",\"sign\":\"c5jBx5kF70xjtqgXcviDsAxvbG6DoZPdvNTDjBzL7x1g9c778kMnKPTdP7VFAUE2Ywlb6VdsBjFY\\nkFwTs9fn6yb2B8HEUhL/75TzdlntfPbsMvAwmbXC4wrIeYmTn2kUo9Hzz3iwtWrtGaBlhsLhTs74\\n87h8+71aem6gv/PjKY4=\\n\",\"timeStamp\":\"1602307989607\"}";
        String jsonString = "{\"userInfo\": {\"ocr\": [\"a1\", \"a2\", \"a3\", {\"date\": \"2021-05-10\"} ], \"oo\": \"[{\\\"a\\\":\\\"aa\\\"}]\", \"ocrInfo\": {\"validDate\": \"2019.09.18-2039.09.18\"}, \"name\": \"温庆东\", \"price\": 123.5, \"userRole\": 5 } }";
//        String jsonString = "{\"applicationId\":\"DD00003036201911250933d6602e8c\",\"applySource\":\"2\",\"creditInfo\":{\"amount\":500000,\"endDate\":\"2020-11-24 00:00:00\",\"interestPenaltyRate\":999,\"interestRate\":666,\"startDate\":\"2019-11-25 00:00:00\"},\"creditResultStatus\":\"Yes\",\"didiRcFeature\":{\"deviceStability1Year\":\"-9999\",\"costLevel3M\":\"1\",\"registMonthLevel\":\"5\",\"costLevel1M\":\"1\",\"nightOrderNumLevel1M\":\"5\",\"isCommonlyUsedLoc\":\"-9999\",\"orderStartingStabilityLevel\":\"1\",\"phone2ImeiLevel3M\":\"3\",\"isImeiFreqChange3M\":\"0\",\"phoneNo2ImeiLevel3M\":\"-9999\",\"riskScore\":\"767\",\"consumeStabilityLevel\":\"1\",\"isCommonlyUsedDev\":\"0\"},\"userInfo\":{\"address\":\"-·昝张家口市宣化区西街横巷5号楼4单元304号\",\"bankCardNo\":\"\",\"cardNo\":\"130705197806102710\",\"idCardValidDate\":\"2039.09.18\",\"name\":\"温庆东\",\"ocrInfo\":{\"address\":\"-·昝张家口市宣化区西街横巷5号楼4单元304号\",\"authority\":\"张家口市公安局宣化分局\",\"birthday\":\"1978-06-10\",\"gender\":\"男\",\"idNo\":\"130705197806102710\",\"name\":\"温庆东\",\"race\":\"汉\",\"validDate\":\"2019.09.18-2039.09.18\"},\"phone\":\"13931314343\",\"telephone\":\"13931314343\",\"userRole\":\"P\"}}";
//        String jsonString = "{\"oo\":\"[{\\\"a\\\":\\\"aa\\\"}]\"}";

        JsonNode jsonNode = regexString2jsonJackson(jsonString);

        System.out.println("---->\t" + jsonNode);
    }

    private JsonNode regexString2jsonJackson(String jsonString) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = objectMapper.createObjectNode();
        ArrayNode arrayNode = objectMapper.createArrayNode();

        try {
            List<?> list = objectMapper.readValue(jsonString, List.class);
//                Class<?> lClass = list.stream().findFirst().map(Object::getClass).orElse(null);
//                if (JsonNode.class.equals(lClass)) list.forEach(l -> arrayNode.add((JsonNode) l));
//                objectNode.set(k, arrayNode);
            System.out.println("list -->\t" + list);
            list.forEach(
                    l -> {
                        System.out.println(l);
                        System.out.println(l.toString());
                        System.out.println(l.getClass());
                        Map<?, ?> convertMap = objectMapper.convertValue(l, Map.class);
                        convertMap.forEach(
                                (k, v) -> {
                                    objectNode.put(k.toString(), v.toString());
                                }
                        );
                        String string = null;
                        try {
                            string = objectMapper.writeValueAsString(objectNode);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        try {
                            regexString2jsonJackson(string);
                        } catch (JsonProcessingException e) {
                            System.out.println("list -->\t" + e);
                        }
                    }
            );
        } catch (MismatchedInputException listException) {
            Map<?, ?> json2Map = objectMapper.readValue(jsonString, Map.class);
            json2Map.forEach(
                    (k, v) -> {
                        try {
                            String json = objectMapper.writeValueAsString(objectMapper.createObjectNode().put(((String) k), v.toString()));
                            Iterator<Map.Entry<String, JsonNode>> fields = objectMapper.readTree(json).fields();
                            while (fields.hasNext()) {
                                Map.Entry<String, JsonNode> next = fields.next();
                                String key = next.getKey();
                                JsonNode value = next.getValue();
                                String text = value.asText(value.toString());
                                System.out.println(value.getNodeType() + "\t" + text);
                            }
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        System.out.println("map -->\t" + k + "\t" + v);
                        System.out.println("map -->\t" + v.getClass());
                        if (v instanceof Map) {
                            Map<?, ?> convertMap = objectMapper.convertValue(v, Map.class);
                            convertMap.forEach(
                                    (k_, v_) -> {
                                        System.out.println("k_ v_\t" + k_ + "\t" + v_);
                                        objectNode.put(k_.toString(), v_.toString());
                                    }
                            );
                        }
                        String string = null;
                        try {
                            string = objectMapper.writeValueAsString(objectNode);
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        try {
                            regexString2jsonJackson(string);
                        } catch (JsonProcessingException e) {
                            System.out.println("map -->\t" + e);
                        }
                    }
            );
        }
        return objectNode;
    }
}
