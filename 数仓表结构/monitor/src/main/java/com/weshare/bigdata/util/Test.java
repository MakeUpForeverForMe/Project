package com.weshare.bigdata.util;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * created by chao.guo on 2021/3/2
 **/
public class Test {
    public static void main(String[] args) {
        String  user_phones ="13722983963,13751852885,18037896762,15201517809,18331126210,17645774457";
        StringBuffer phoneBuffer = new StringBuffer();
        String warning="@all";
        Arrays.stream(user_phones.split(",")).forEach(it->{
            phoneBuffer.append("\"").append(it).append("\"").append(",");

        });
        if(StringUtils.endsWith(phoneBuffer.toString(),",")){
            warning=phoneBuffer.toString().substring(0,phoneBuffer.toString().lastIndexOf(","));
        }
        System.out.println(warning);

    }
}
