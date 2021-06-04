package com.springboot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.client.RestTemplate;

import java.io.Serializable;

@SpringBootTest
class JavaWebSpringBoot02ApplicationTests {

    @Test
    void contextLoads() {
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Address implements Serializable {
        private String cip;
        private String cid;
        private String cname;
    }

    @Test
    void test1() throws JsonProcessingException {
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://pv.sohu.com/cityjson?ie=utf-8"; // 直接获取本机 ip （ 新浪 格式 js  ）
        String template = restTemplate.getForObject(url, String.class);
        System.out.println(template);
        assert template != null;
        template = template.split("=")[1].split(";")[0];
        ObjectMapper objectMapper = new ObjectMapper();
        Address fromJson = objectMapper.readValue(template, Address.class);
        System.out.println(fromJson);
        System.out.println(fromJson.getCip());
    }
}
