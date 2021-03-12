package com.weshare.bigdata.util;


import org.apache.commons.io.Charsets;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by mouzwang on 2020-12-11 10:20
 */
public class HTTPUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HTTPUtils.class);
    private static CloseableHttpClient httpClient;

    static {
        httpClient = HttpClients.createDefault();
        LOGGER.info("===== HTTP Client 初始化成功 =====");
    }

    // 编码格式。发送编码格式统一用UTF-8
    private static final String ENCODING = "UTF-8";
    // 设置连接超时时间，单位毫秒。
    private static final int CONNECT_TIMEOUT = 30000;
    // 请求获取数据的超时时间(即响应时间)，单位毫秒。
    private static final int SOCKET_TIMEOUT = 30000;

    public static CloseableHttpResponse postReqAndGetHTTPResp(String url, String mediaType,
                                                              String cookie, String referer, HttpEntity entity) {
        LOGGER.info("[postRequest] resourceUrl : {}", url);
        HttpPost httpPost = new HttpPost(url);
        RequestConfig config = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        httpPost.setConfig(config);
        httpPost.addHeader("Content-type", mediaType);
        if (cookie != null) httpPost.addHeader("Cookie", cookie);
//        httpPost.addHeader("Accept", "application/json");
        if (referer != null) httpPost.addHeader("Referer",referer);
        httpPost.addHeader("Accept", "*/*");
        httpPost.setEntity(entity);
        try {
            CloseableHttpResponse response = httpClient.execute(httpPost);
            int code = response.getStatusLine().getStatusCode();
            LOGGER.info("response code : " + code);
            if (code >= 400) {
                throw new Exception(EntityUtils.toString(response.getEntity()));
            }
            return response;
        } catch (Exception e) {
            LOGGER.error("PostRequest error,response message : ", e);
        } finally {
            httpPost.releaseConnection();
        }
        return null;
    }

    public static String getReqAndGetRespEntity(String url, Map<String, String> params,
                                                String cookie) {
        URIBuilder uriBuilder = null;
        try {
            uriBuilder = new URIBuilder(url);
        } catch (URISyntaxException e) {
            LOGGER.error("GetRequest URI Builder error :  ", e);
        }
        if (params != null) {
            Set<Map.Entry<String, String>> entrySet = params.entrySet();
            for (Map.Entry<String, String> entry : entrySet) {
                uriBuilder.addParameter(entry.getKey(), entry.getValue());
            }
        }
        HttpGet httpGet = null;
        try {
            httpGet = new HttpGet(uriBuilder.build());
        } catch (URISyntaxException e) {
            LOGGER.error("Create HTTPMethod Failed : ", e);
        }
        RequestConfig config = RequestConfig.custom().setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT).build();
        httpGet.setConfig(config);
        httpGet.addHeader("Accept", "application/json");
        httpGet.addHeader("Cookie", cookie);
        httpGet.addHeader("Content-type", "application/json");
        try {
            CloseableHttpResponse response = httpClient.execute(httpGet);
            int code = response.getStatusLine().getStatusCode();
            if (code >= 400) {
                throw new Exception(EntityUtils.toString(response.getEntity()));
            }
            return EntityUtils.toString(response.getEntity());
        } catch (Exception e) {
            LOGGER.error("GetRequest error,response message : ", e);
        } finally {
            httpGet.releaseConnection();
        }
        return null;
    }

    /**
     * 向表单发请求并获得响应对象
     *
     * @param url
     * @param cookie
     * @param parameterBoby
     * @return
     */
    public static CloseableHttpResponse postFormAndGetHTTPResp(String url, String cookie, String referer,
                                                               List<NameValuePair> parameterBoby) {
        HttpEntity entity = new UrlEncodedFormEntity(parameterBoby, Charsets.UTF_8);
        CloseableHttpResponse response = postReqAndGetHTTPResp(url, "application/x-www-form" +
                "-urlencoded; charset=UTF-8", cookie, referer, entity);
        return response;
    }

    /**
     * 获取响应信息Entity
     *
     * @param httpResponse
     * @return
     */
    public static String getRespEntity(CloseableHttpResponse httpResponse) {
        try {
            String respEntity = EntityUtils.toString(httpResponse.getEntity());
            httpResponse.close();
            return respEntity;
        } catch (IOException e) {
            LOGGER.error("Get Response Entity error : ", e);
        }
        return null;
    }

    /**
     * 获取Cookie
     *
     * @param httpResponse
     * @return
     */
    public static HashMap<String, String> getCookie(CloseableHttpResponse httpResponse) {
        HashMap<String, String> cookieMap = new HashMap<>();
        if (httpResponse == null) return null;
        Header[] headers = httpResponse.getHeaders("set-Cookie");
        try {
            httpResponse.close();
        } catch (IOException e) {
            LOGGER.error("Http Response close failed : ", e);
        } finally {
            try {
                httpResponse.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (headers == null || headers.length == 0) {
            LOGGER.info("there are no cookies");
            return null;
        }
        String cookie = "";
        for (int i = 0; i < headers.length; i++) {
            cookie += headers[i].getValue();
            if (i != headers.length - 1) cookie += ";";
        }
        String cookies[] = cookie.split(";");
        for (String c : cookies) {
            c = c.trim();
            cookieMap.put(c.split("=")[0], c.split("=").length == 1 ? "" : (c.split("=").length == 2 ? c.split("=")[1] : c.split("=", 2)[1]));
        }
        return cookieMap;
    }

    /**
     * 发送HTTP 请求
     * @param url
     * @param str
     * @return
     */
    public static String sendPost(String url,String str){
        String userAgent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36";
        CloseableHttpClient httpClient = HttpClients.createDefault();
        StringEntity entity = new StringEntity(str, Consts.UTF_8);
        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("User-Agent", userAgent);
        httpPost.setHeader("Content-Type", "application/json");
        httpPost.setEntity(entity);
        CloseableHttpResponse response  = null;
        String result=null;
       // 执行post请求
        try {
            response = httpClient.execute(httpPost);
            HttpEntity entity1 = response.getEntity();
            // 得到字符串
            result = EntityUtils.toString(entity1);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                response.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        // 得到entity
       return  result;
    }
}
