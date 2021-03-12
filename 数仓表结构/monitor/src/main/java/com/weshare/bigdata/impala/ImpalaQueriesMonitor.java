package com.weshare.bigdata.impala;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weshare.bigdata.impala.bean.QueryDetail;
import com.weshare.bigdata.util.HTTPUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by mouzwang on 2021-01-06 17:55
 */
public class ImpalaQueriesMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImpalaQueriesMonitor.class);
    private static final Properties prop = new Properties();

    public static void main(String[] args) throws InterruptedException {
        InputStream is = ImpalaQueriesMonitor.class.getClassLoader().getResourceAsStream("test_http.properties");
        try {
            prop.load(is);
        } catch (IOException e) {
            LOGGER.error("load http properties failed : ", e);
        }
        while (true) {
            ArrayList<NameValuePair> nameValuePairs = new ArrayList<>();
            // get cookie from login page
            nameValuePairs.add(new BasicNameValuePair("j_username", "admin"));
            nameValuePairs.add(new BasicNameValuePair("j_password", "admin"));
            CloseableHttpResponse loginResponse =
                    HTTPUtils.postFormAndGetHTTPResp(prop.getProperty("login_uri"), null, null,
                            nameValuePairs);
            HashMap<String, String> cookieMap = HTTPUtils.getCookie(loginResponse);
            String cookie = "";
            Set<Map.Entry<String, String>> entries = cookieMap.entrySet();
            for (Map.Entry<String, String> entry : entries) {
                if ("CLOUDERA_MANAGER_SESSIONID".equals(entry.getKey())) {
                    cookie += entry.getKey() + "=" + entry.getValue() + "; ";
                }
            }
            // request params
            HashMap<String, String> params = new HashMap<>();
            params.put("startTime", String.valueOf(System.currentTimeMillis() - 30 * 60 * 1000));
            params.put("endTime", String.valueOf(System.currentTimeMillis()));
            params.put("filters", null);
            params.put("offset", "0");
            params.put("limit", "100");
            params.put("serviceName", "impala");
            params.put("histogramAttributes", prop.getProperty("histogramAttributes"));
            params.put("_", String.valueOf(System.currentTimeMillis()));
            //get impala executing queries
            String executingQueriesEntity = HTTPUtils.getReqAndGetRespEntity(prop.getProperty("impala_query_uri"), params, cookie);
            if (executingQueriesEntity == null) {
                LOGGER.warn("find no queries this time !");
                return;
            }
            // find out timeout queries and close
            LinkedList<QueryDetail> queryDetails = getQueryDetails(executingQueriesEntity);
            String cancelCookie = "";
            for (Map.Entry<String, String> entry : entries) {
                if ("CLOUDERA_MANAGER_SESSIONID".equals(entry.getKey())) {
                    cancelCookie += entry.getKey() + "=" + entry.getValue() + "; ";
                }
            }
            if (queryDetails.size() > 0) {
                for (QueryDetail queryDetail : queryDetails) {
                    closeTimeoutQueries(queryDetail, cancelCookie);
                }
            }
            LOGGER.info("impala queries check completed,all timeout queries has been closed !");
            TimeUnit.MINUTES.sleep(30);
        }
    }

    /**
     * find out timeout queries
     *
     * @param executingQueriesEntity
     * @return
     */
    private static LinkedList<QueryDetail> getQueryDetails(String executingQueriesEntity) {
        JsonObject jsonObject = new JsonParser().parse(executingQueriesEntity).getAsJsonObject();
        JsonArray items = jsonObject.getAsJsonArray("items");
        LinkedList<QueryDetail> queryDetails = new LinkedList<>();
        LOGGER.info("executing impala queries nums : " + items.size());
        for (int i = 0; i < items.size(); i++) {
            JsonObject itemObject = items.get(i).getAsJsonObject();
            if (itemObject != null
                    && itemObject.get("duration") != null
                    && itemObject.get("duration").getAsJsonObject().get("standardSeconds") != null
                    && itemObject.get("duration").getAsJsonObject().get("standardSeconds").getAsLong() / 60 > 10
                    && itemObject.get("queryType") != null
                    && "DDL".equals(itemObject.get("queryType").getAsString())
            ) {
                QueryDetail queryDetail = new QueryDetail();
                queryDetail.setFrontendHostName(itemObject.get("frontendHostName").getAsString());
                queryDetail.setQueryState(itemObject.get("queryState").getAsString());
                queryDetail.setQueryType(itemObject.get("queryType").getAsString());
                queryDetail.setQueryId(itemObject.get("queryId").getAsString());
                queryDetail.setStatement(itemObject.get("statement").getAsString());
                queryDetail.setDurationTime(itemObject.get("duration").getAsJsonObject().get("standardSeconds").getAsLong());
                queryDetail.setServiceName(itemObject.get("serviceName").getAsString());
                queryDetails.add(queryDetail);
            }
        }
        return queryDetails;
    }

    /**
     * close timeout impala queries by query id and service name
     *
     * @param queryDetail
     */
    private static void closeTimeoutQueries(QueryDetail queryDetail, String cookie) {
        ArrayList<NameValuePair> nvps = new ArrayList<>();
        if ("impala".equals(queryDetail.getServiceName())) {
            nvps.add(new BasicNameValuePair("queryId", queryDetail.getQueryId()));
            LOGGER.info("timeout impala queryId : " + queryDetail.getQueryId());
            nvps.add(new BasicNameValuePair("serviceName", "impala"));
            String referer = prop.getProperty("close_query_referer");
            CloseableHttpResponse closeQueryResponse =
                    HTTPUtils.postFormAndGetHTTPResp(prop.getProperty("close_query_uri"), cookie, referer, nvps);
            try {
                if (closeQueryResponse != null) closeQueryResponse.close();
            } catch (IOException e) {
                LOGGER.error("cancel impala response close failed : ", e);
            } finally {
                try {
                    if (closeQueryResponse != null) closeQueryResponse.close();
                } catch (IOException e) {
                    LOGGER.error("cancel impala response close failed : ", e);
                }
            }
        }
    }
}
