package cn.mine;

import cn.mine.dao.DAO;
import cn.mine.util.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author ximing.wei 2021-06-07 21:26:06
 */
@Slf4j
public class ProjectUtilsTest extends DAO<Customer> {

    @Test
    public void test1() {
        log.info("首个测试类的方法 aaa-------");
    }

    @Test
    public void test2() {
        log.info("测试加解密");
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
        Arrays.stream(strings).forEach(idCard -> {
            String encryptKey = AesUtils.PASSWORD_TENCENT;
            String encrypt = AesUtils.encrypt(idCard, encryptKey);
            log.info("原值 '{}'，加密值为 '{}'", idCard, encrypt);
            String decrypt = AesUtils.decrypt(encrypt, encryptKey);
            log.info("原值 '{}'，解密值为 '{}'", encrypt, decrypt);
        });
    }

    @Test
    public void test3() {
        log.info("测试日期格式化");
        String dateTime;
        log.info("'{}'--'{}'", (dateTime = "2021年06月07日"), DateUtils.DATE_CHINESE.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "2021-06-07"), DateUtils.DATE_LINE.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "2021/06/07"), DateUtils.DATE_SLASH.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "2021/6/7"), DateUtils.DATE_SLASH_TWO.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "2021\\06\\07"), DateUtils.DATE_DOUBLE_SLASH.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "20210607"), DateUtils.DATE_NONE.toDefaultDateTime(dateTime));

        log.info("'{}'--'{}'", (dateTime = "2021-06-07 21:44:30"), DateUtils.DATE_TIME_LINE.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "2021/06/07 21:44:30"), DateUtils.DATE_TIME_SLASH.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "2021\\06\\07 21:44:30"), DateUtils.DATE_TIME_DOUBLE_SLASH.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "20210607 21:44:30"), DateUtils.DATE_TIME_NONE.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "20210607214430"), DateUtils.DATE_TIME_STAMP.toDefaultDateTime(dateTime));

        log.info("'{}'--'{}'", (dateTime = "2021-06-07 21:44:30.123"), DateUtils.DATE_TIME_MILES_LINE.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "2021/06/07 21:44:30.123"), DateUtils.DATE_TIME_MILES_SLASH.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "2021\\06\\07 21:44:30.123"), DateUtils.DATE_TIME_MILES_DOUBLE_SLASH.toDefaultDateTime(dateTime));
        log.info("'{}'--'{}'", (dateTime = "20210607 21:44:30.123"), DateUtils.DATE_TIME_MILES_NONE.toDefaultDateTime(dateTime));
    }

    @Test
    public void test4() {
        ConfigUtil mysqlDriver = ConfigUtil.MYSQL_DRIVER;
        ConfigUtil aaa = mysqlDriver.set("aaa");
        log.info(mysqlDriver.key);
        log.info(mysqlDriver.key);
        log.info(aaa.value);
    }

    @Test
    public void test5() {
        log.info("MYSQL_DRIVER             {}", ConfigUtil.MYSQL_DRIVER.value);
        log.info("MYSQL_HOST               {}", ConfigUtil.MYSQL_HOST.value);
        log.info("MYSQL_USER               {}", ConfigUtil.MYSQL_USER.value);
        log.info("MYSQL_PASSWORD           {}", ConfigUtil.MYSQL_PASSWORD.value);
        log.info("MYSQL_DATABASE           {}", ConfigUtil.MYSQL_DATABASE.value);
        log.info("MYSQL_URL                {}", ConfigUtil.MYSQL_URL.value);

        log.info("KAFKA_SERVERS            {}", ConfigUtil.KAFKA_SERVERS.value);
        log.info("KAFKA_TOPIC              {}", ConfigUtil.KAFKA_TOPIC.value);
        log.info("KAFKA_GROUP              {}", ConfigUtil.KAFKA_GROUP.value);
        log.info("KAFKA_KEY_SERIAL         {}", ConfigUtil.KAFKA_KEY_SERIAL.value);
        log.info("KAFKA_KEY_DESERIALIZER   {}", ConfigUtil.KAFKA_KEY_DESERIALIZER.value);
        log.info("KAFKA_VALUE_SERIAL       {}", ConfigUtil.KAFKA_VALUE_SERIAL.value);
        log.info("KAFKA_VALUE_DESERIALIZER {}", ConfigUtil.KAFKA_VALUE_DESERIALIZER.value);
    }

    @Test
    public void test5p1() {
        if (update("alter table client_info change column `_id` `id` varchar(255);")) {
            log.info("修改列名成功");
        } else {
            log.info("修改列名失败");
        }
        if (update("alter table client_info change column `id` `_id` varchar(255);")) {
            log.info("修改列名成功");
        } else {
            log.info("修改列名失败");
        }
    }

    @Test
    public void test5p2() {
        log.info(getValue("select count(1) from client_info"));
    }

    @Test
    public void test5p3() {
        Customer customer = get("select `_id` as id,cTime,uTime,birthDate,sex,city,expectation,province,sourceChannel from client_info where `_id` = '5ced41ffef6f9c4aaf5a9d3f14087026'");
        log.info(customer.getCTime());
        log.info(customer.getUTime());
    }

    @Test
    public void test5p4() {
        List<Customer> customerList = getList("select `_id` as id,cTime,uTime,birthDate,sex,city,expectation,province,sourceChannel from client_info");
        customerList.forEach(customer -> {
            log.info(customer.getCTime());
            log.info(customer.getUTime());
        });
    }

    private final KafkaUtil$ module = KafkaUtil$.MODULE$;

    @Test
    public void test6() {
        log.info("getKafkaServersProps  {}", module.getKafkaServersProps());
        log.info("getKafkaProducerProps {}", module.getKafkaProducerProps());
        log.info("getKafkaConsumerProps {}", module.getKafkaConsumerProps());
        log.info("ConsumerGroups        {}", module.getConsumerGroupList());
        log.info("Topics                {}", module.getTopicsList());
        // module.consumerSubscribeMsg();
    }

    @Test
    public void test6p5() {
        module.producerSendMsg("哈哈哈哈！");
    }
}
