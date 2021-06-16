package cn.mine;

import cn.mine.netty2.SimpleClient;
import cn.mine.netty2.SimpleServer;
import com.alibaba.fastjson.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * @author 魏喜明
 */
public class SimpleTest {
    @Test
    public void simpleTestServer() {
        new SimpleServer(9999).run();
    }

    @Test
    public void simpleTestClient() throws Exception {
        new SimpleClient().connect("127.0.0.1", 9999);
    }

    @Test
    public void testJsonUtil() {
        String jsonString = "{\"org\": \"15601\", \"partner\": \"0006\", \"reqContent\": {\"jsonResp\": \"{\\\"sign\\\": null, \\\"partner\\\": \\\"0006\\\", \\\"rspData\\\": \\\"{\\\\\\\"consumptionLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"amtOfPerfermanceLoansLast3M\\\\\\\":99,\\\\\\\"hitDeadbeatList\\\\\\\":0,\\\\\\\"appsOfConFinOrgLastTwoYear\\\\\\\":99,\\\\\\\"overdueTimesOfCreditApplyLast4M\\\\\\\":99,\\\\\\\"overdueLoansOfMoreThan1DayLast6M\\\\\\\":99,\\\\\\\"catPool\\\\\\\":0,\\\\\\\"LastFinancialQuery\\\\\\\":99,\\\\\\\"scoreLevel\\\\\\\":\\\\\\\"B\\\\\\\",\\\\\\\"countOfInstallitionOfLoanAppLast2M\\\\\\\":99,\\\\\\\"countOfUninstallAppsOfLoanLast3M\\\\\\\":99,\\\\\\\"blkListLoc\\\\\\\":0,\\\\\\\"averageDailyOpenTimesOfFinAppsLastM\\\\\\\":99,\\\\\\\"timesOfUninstallFinAppsLast15D\\\\\\\":99,\\\\\\\"fraudIndustry\\\\\\\":0,\\\\\\\"sumCreditOfWebLoan\\\\\\\":99,\\\\\\\"suspiciousDevice\\\\\\\":0,\\\\\\\"daysOfLocationUploadLats9M\\\\\\\":99,\\\\\\\"accountHacked\\\\\\\":0,\\\\\\\"marriage\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"virtualMaliciousStatus\\\\\\\":0,\\\\\\\"accountForInTownInWorkDayLastM\\\\\\\":99,\\\\\\\"daysOfEarliestRegisterOfLoanAppLast9M\\\\\\\":99,\\\\\\\"applyByIDLastYear\\\\\\\":99,\\\\\\\"gamerArbitrageStatus\\\\\\\":0,\\\\\\\"abnormalPayment\\\\\\\":0,\\\\\\\"usageRateOfCompusLoanApp\\\\\\\":99,\\\\\\\"pass\\\\\\\":\\\\\\\"Yes\\\\\\\",\\\\\\\"cityLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"appsOfConFinProLastTwoYear\\\\\\\":99,\\\\\\\"countOfNoticesOfFinMessageLast9M\\\\\\\":99,\\\\\\\"financAbility\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"incomeLevel\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"forgedIdStatus\\\\\\\":0,\\\\\\\"rejectTimesOfCreditApplyLast4M\\\\\\\":99,\\\\\\\"riskOfDevice\\\\\\\":99,\\\\\\\"idTheftStatus\\\\\\\":0,\\\\\\\"abnormalAccount\\\\\\\":0,\\\\\\\"edu\\\\\\\":\\\\\\\"99\\\\\\\",\\\\\\\"accountForInstallBussinessAppsLast4M\\\\\\\":99,\\\\\\\"blkList2\\\\\\\":0,\\\\\\\"blkList1\\\\\\\":0,\\\\\\\"accountForWifiUseTimeSpanLast5M\\\\\\\":99,\\\\\\\"loanAmtLast3M\\\\\\\":99,\\\\\\\"countOfRegisterAppsOfFinLastM\\\\\\\":99,\\\\\\\"counterfeitAgencyStatus\\\\\\\":0,\\\\\\\"status\\\\\\\":\\\\\\\"1\\\\\\\"}\\\", \\\"service\\\": \\\"WIND_CONTROL_CREDIT\\\", \\\"resultMsg\\\": \\\"请求成功\\\", \\\"resultCode\\\": \\\"0000\\\", \\\"serviceVersion\\\": \\\"1\\\"}\", \"reqMsgCreateDate\": \"2020-12-22 13:49:31\", \"jsonReq\": \"{\\\"sign\\\": \\\"rtAHHYtVX0he+npSXuZl0fMxGBVyoa1gm1vNux3OJYjhUYoNTpTqXkrBCbtLE6Pr5BRgKU58dcrz\\\\nGDtNH+k5szX/F2nmko25ouo9Go7IZXUfSosMKHLlatUDLoYRGM7MUZLxKs0GY/guwrZR1Hf1IfmM\\\\nwIhB/fAfED3ujRdAI0o=\\\\n\\\", \\\"content\\\": \\\"{\\\\\\\"reqData\\\\\\\":{\\\\\\\"ifCar\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"country\\\\\\\":\\\\\\\"CHN\\\\\\\",\\\\\\\"authAccountName\\\\\\\":\\\\\\\"王香皖\\\\\\\",\\\\\\\"dbBankCode\\\\\\\":\\\\\\\"313301008887\\\\\\\",\\\\\\\"lnRate\\\\\\\":2.100000,\\\\\\\"endDate\\\\\\\":\\\\\\\"2021-12-16\\\\\\\",\\\\\\\"workSts\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"workDuty\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"homeAddr\\\\\\\":\\\\\\\"安徽省阜阳市颍泉区清颍路131号10幢202户\\\\\\\",\\\\\\\"idNo\\\\\\\":\\\\\\\"341204200105190421\\\\\\\",\\\\\\\"sales\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"homeCode\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"ifCarCred\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"payType\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"isBelowRisk\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"children\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"vouType\\\\\\\":\\\\\\\"4\\\\\\\",\\\\\\\"creditLimit\\\\\\\":7165.00,\\\\\\\"cardAmt\\\\\\\":0.00,\\\\\\\"profession\\\\\\\":\\\\\\\"12\\\\\\\",\\\\\\\"idType\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"authNo\\\\\\\":\\\\\\\"1120122213485525204565\\\\\\\",\\\\\\\"mincome\\\\\\\":1000.00,\\\\\\\"postAddr\\\\\\\":\\\\\\\"安徽省阜阳市颍泉区清颍路131号10幢202户\\\\\\\",\\\\\\\"workTitle\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"ifCard\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"degree\\\\\\\":\\\\\\\"4\\\\\\\",\\\\\\\"ifAgent\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"birth\\\\\\\":\\\\\\\"20010519\\\\\\\",\\\\\\\"ifId\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"workName\\\\\\\":\\\\\\\"无\\\\\\\",\\\\\\\"prodType\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"telNo\\\\\\\":\\\\\\\"18856825195\\\\\\\",\\\\\\\"pactAmt\\\\\\\":14.40,\\\\\\\"ifPact\\\\\\\":\\\\\\\"1\\\\\\\",\\\\\\\"ifApp\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"edu\\\\\\\":\\\\\\\"20\\\\\\\",\\\\\\\"custType\\\\\\\":\\\\\\\"04\\\\\\\",\\\\\\\"loanDate\\\\\\\":\\\\\\\"2020-12-22\\\\\\\",\\\\\\\"postCode\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"hasOverdueLoan\\\\\\\":\\\\\\\"0\\\\\\\",\\\\\\\"income\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"appUse\\\\\\\":\\\\\\\"07\\\\\\\",\\\\\\\"riskLevel\\\\\\\":\\\\\\\"R3\\\\\\\",\\\\\\\"proCode\\\\\\\":\\\\\\\"002001\\\\\\\",\\\\\\\"zxhomeIncome\\\\\\\":12000.00,\\\\\\\"rpyMethod\\\\\\\":\\\\\\\"03\\\\\\\",\\\\\\\"launder\\\\\\\":\\\\\\\"03\\\\\\\",\\\\\\\"dbAccountName\\\\\\\":\\\\\\\"深圳市分期乐网络科技有限公司\\\\\\\",\\\\\\\"phoneNo\\\\\\\":\\\\\\\"18856825195\\\\\\\",\\\\\\\"ifMort\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"loanTime\\\\\\\":\\\\\\\"2020-12-22 13:49:16\\\\\\\",\\\\\\\"marriage\\\\\\\":\\\\\\\"10\\\\\\\",\\\\\\\"customerId\\\\\\\":\\\\\\\"148380565\\\\\\\",\\\\\\\"authBankName\\\\\\\":\\\\\\\"工商银行\\\\\\\",\\\\\\\"idEndDate\\\\\\\":\\\\\\\"2021-07-11\\\\\\\",\\\\\\\"idPreDate\\\\\\\":\\\\\\\"2016-07-11\\\\\\\",\\\\\\\"sex\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"ifRoom\\\\\\\":\\\\\\\"2\\\\\\\",\\\\\\\"appArea\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"ifLaunder\\\\\\\":\\\\\\\"02\\\\\\\",\\\\\\\"custName\\\\\\\":\\\\\\\"王香皖\\\\\\\",\\\\\\\"homeIncome\\\\\\\":\\\\\\\"01\\\\\\\",\\\\\\\"dbBankName\\\\\\\":\\\\\\\"南京银行\\\\\\\",\\\\\\\"authBankAccount\\\\\\\":\\\\\\\"6215581311005946674\\\\\\\",\\\\\\\"homeArea\\\\\\\":\\\\\\\"000000\\\\\\\",\\\\\\\"homeSts\\\\\\\":\\\\\\\"9\\\\\\\",\\\\\\\"loanTerm\\\\\\\":12,\\\\\\\"trade\\\\\\\":\\\\\\\"26\\\\\\\",\\\\\\\"creditCoef\\\\\\\":2.100000,\\\\\\\"applyNo\\\\\\\":\\\\\\\"1120122213485525204565\\\\\\\",\\\\\\\"dbBankAccount\\\\\\\":\\\\\\\"2008270000000131\\\\\\\",\\\\\\\"workType\\\\\\\":\\\\\\\"Z\\\\\\\",\\\\\\\"workWay\\\\\\\":\\\\\\\"Z\\\\\\\",\\\\\\\"payDay\\\\\\\":16,\\\\\\\"dbOpenBankName\\\\\\\":\\\\\\\"南京银行股份有限公司南京麒麟支行\\\\\\\",\\\\\\\"age\\\\\\\":19}}\\\", \\\"partner\\\": \\\"0006\\\", \\\"service\\\": \\\"WIND_CONTROL_CREDIT\\\", \\\"projectNo\\\": \\\"WS0006200002\\\", \\\"serviceSn\\\": \\\"XFX9JSmRpxJtoxgjh3QX\\\", \\\"timeStamp\\\": \\\"1608616170150\\\", \\\"projectName\\\": null, \\\"serviceVersion\\\": \\\"1\\\"}\", \"partnerEnumStr\": \"GM_PARTNER\"}, \"txnId\": \"16086177005141Node1000028000000\"}";
//        String jsonString = "{\"org\": \"15601\", \"partner\": \"0006\", \"txnId\": \"16086177005141Node1000028000000\"}";

        System.out.println(JSONObject.toJSONString(regex_json(jsonString), true));
    }

    private JSONObject regex_json(String jsonString) {
        JSONObject json = new JSONObject();
        String defaultKey = "regex_json_default_key";
        try {
            JSONObject jsonObject = JSONObject.parseObject(jsonString);
            for (String jsonKey : jsonObject.keySet()) {
                JSONObject regexJson = regex_json(jsonObject.getString(jsonKey));
                Object regexKey = regexJson.get(defaultKey);
                json.put(jsonKey, regexKey == null ? regexJson : regexJson);
            }
            System.out.println(json);
        } catch (Exception e) {
            json.put(defaultKey, jsonString);
        }
        return json;
    }
}
