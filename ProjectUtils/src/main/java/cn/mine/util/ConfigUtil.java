package cn.mine.util;

import java.io.IOException;
import java.util.Properties;

/**
 * @author ximing.wei 2021-06-15 16:50:00
 */
public enum ConfigUtil {
    // MYSQL 配置
    MYSQL_DRIVER("mysql.driver"),
    MYSQL_HOST("mysql.host"),
    MYSQL_USER("mysql.user"),
    MYSQL_PASSWORD("mysql.password"),
    MYSQL_DATABASE("mysql.database"),
    MYSQL_URL("jdbc:mysql://" + MYSQL_HOST.value + ":3306/" + MYSQL_DATABASE.value + "?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai"),

    // Kafka 配置
    KAFKA_SERVERS("kafka.bootstrap.servers"),
    KAFKA_TOPIC("kafka.topic"),
    KAFKA_GROUP("kafka.group.id"),
    KAFKA_KEY_SERIAL("kafka.key.serializer"),
    KAFKA_KEY_DESERIALIZER("kafka.key.deserializer"),
    KAFKA_VALUE_SERIAL("kafka.value.serializer"),
    KAFKA_VALUE_DESERIALIZER("kafka.value.deserializer"),

    // Hadoop 配置
    // Hive 配置
    // Flink 配置
    // HBase 配置
    // Spark 配置

    ;

    private final Properties properties = new Properties();

    public transient String key;
    public transient String value;

    ConfigUtil(String propKey) {
        try {
            this.properties.load(this.getClass().getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.key = propKey;
        this.value = this.getProp(propKey);
    }

    private String getProp(String key) {
        return this.properties.getProperty(key, null);
    }

    public Properties properties() {
        return this.properties;
    }

    public ConfigUtil set(String setVal) {
        if (setVal == null) return null;
        this.properties.setProperty(this.key, setVal);
        this.value = this.getProp(this.key);
        return this;
    }
}
