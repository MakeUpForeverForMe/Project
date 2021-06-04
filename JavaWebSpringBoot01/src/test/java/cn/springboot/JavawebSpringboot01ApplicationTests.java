package cn.springboot;

import cn.springboot.entities.Address2;
import cn.springboot.entities.User;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.client.RestTemplate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@SpringBootTest
class JavawebSpringboot01ApplicationTests {

    @Autowired
    DataSource dataSource;

    User user = new User(1, "AA", "AA@qq.com");

    @Test
    void test() throws SQLException {
        System.out.println(dataSource.getClass());
        Connection connection = dataSource.getConnection();
        System.out.println(connection);
        connection.close();
    }

    @Test
    void test1() throws JsonProcessingException {
        System.out.println(user);
        RestTemplate restTemplate = new RestTemplate();
        String url = "http://pv.sohu.com/cityjson?ie=utf-8"; // 直接获取本机 ip （ 新浪 格式 js  ）
        String template = restTemplate.getForObject(url, String.class);
        System.out.println(template);
        ObjectMapper objectMapper = new ObjectMapper();
        Address2 fromJson = objectMapper.readValue(template, Address2.class);
        System.out.println(fromJson);
    }
}
