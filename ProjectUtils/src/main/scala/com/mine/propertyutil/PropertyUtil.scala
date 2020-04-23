package com.mine.propertyutil

import java.net.URLEncoder
import java.util.Properties

object PropertyUtil {
    def apply(): PropertyUtil = new PropertyUtil()

    def apply(fileName: String): PropertyUtil = new PropertyUtil(fileName)
}

class PropertyUtil {

    private val properties = new Properties()

    def this(fileName: String) {
        this
        properties.load(getClass.getClassLoader.getResourceAsStream(fileName))
    }

    def getProps(key: String): String = properties.getProperty(key)

    // MYSQL 配置
    final lazy val MYSQL_DRIVER: String         =   getProps("mysql.driver")
    final lazy val MYSQL_HOST: String           =   getProps("mysql.host")
    final lazy val MYSQL_DATABASE: String       =   getProps("mysql.database")
    final lazy val MYSQL_USER: String           =   getProps("mysql.user")
    final lazy val MYSQL_PASSWORD: String       =   getProps("mysql.password")
    final lazy val MYSQL_URL: String            =   s"jdbc:mysql://$MYSQL_HOST:3306/$MYSQL_DATABASE?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai"

    // Hadoop 配置
    // Hive 配置
    // Flink 配置
    // HBase 配置
    // Spark 配置
    // Kafka 配置
    final lazy val KAFKA_SERVERS: String        =   getProps("kafka.bootstrap.servers")
    final lazy val KAFKA_TOPIC: String          =   getProps("kafka.topic")
    final lazy val KAFKA_GROUP: String          =   getProps("kafka.group.id")
    final lazy val KAFKA_KEY_SERIAL: String     =   getProps("kafka.key.serializer")
    final lazy val KAFKA_VALUE_SERIAL: String   =   getProps("kafka.value.serializer")
    final lazy val KAFKA_KEY_DESERIAL: String   =   getProps("kafka.key.deserializer")
    final lazy val KAFKA_VALUE_DESERIAL: String =   getProps("kafka.value.deserializer")
}
