package com.weshare.util;


import org.apache.commons.io.Charsets;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by mouzwang on 2020-12-11 10:20
 */
public class HTTPUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(HTTPUtils.class);
    public static String postRequest(String url, String mediaType, HttpEntity entity) {
        LOGGER.info("[postRequest] resourceUrl : {}", url);
        HttpPost httpPost = new HttpPost(url);
        httpPost.addHeader("Content-type", mediaType);
        httpPost.addHeader("Accept", "application/json");
        httpPost.setEntity(entity);
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            CloseableHttpResponse response = httpClient.execute(httpPost);
            int code = response.getStatusLine().getStatusCode();
            if (code >= 400) {
                throw new Exception(EntityUtils.toString(response.getEntity()));
            }
            return EntityUtils.toString(response.getEntity());
        } catch (Exception e) {
            LOGGER.error("PostRequest error,response message : ",e);
        } finally {
            httpPost.releaseConnection();
        }
        return null;
    }

    public static String postForm(String url, List<NameValuePair> parameterBoby) {
        HttpEntity entity = new UrlEncodedFormEntity(parameterBoby, Charsets.UTF_8);
        return postRequest(url, "application/x-www-form-urlencoded", entity);
    }
}
