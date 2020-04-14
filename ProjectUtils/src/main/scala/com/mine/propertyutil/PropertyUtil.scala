package com.mine.propertyutil

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

    def getPropertyValueByKey(key: String): String = properties.getProperty(key)

    // Kafka 配置
    final lazy val KAFKA_SERVERS: String        =   getPropertyValueByKey("kafka.bootstrap.servers")
    final lazy val KAFKA_TOPIC: String          =   getPropertyValueByKey("kafka.topic")
    final lazy val KAFKA_GROUP: String          =   getPropertyValueByKey("kafka.group.id")
    final lazy val KAFKA_KEY_SERIAL: String     =   getPropertyValueByKey("kafka.key.serializer")
    final lazy val KAFKA_VALUE_SERIAL: String   =   getPropertyValueByKey("kafka.value.serializer")
    final lazy val KAFKA_KEY_DESERIAL: String   =   getPropertyValueByKey("kafka.key.deserializer")
    final lazy val KAFKA_VALUE_DESERIAL: String =   getPropertyValueByKey("kafka.value.deserializer")

    // MYSQL 配置
    final lazy val MYSQL_DRIVER: String         =   getPropertyValueByKey("mysql.driver")
    final lazy val MYSQL_HOST: String           =   getPropertyValueByKey("mysql.host")
    final lazy val MYSQL_DATABASE: String       =   getPropertyValueByKey("mysql.database")
    final lazy val MYSQL_USER: String           =   getPropertyValueByKey("mysql.user")
    final lazy val MYSQL_PASSWORD: String       =   getPropertyValueByKey("mysql.password")
    final lazy val MYSQL_URL: String            =   s"jdbc:mysql://$MYSQL_HOST:3306/$MYSQL_DATABASE?user=$MYSQL_USER&password=$MYSQL_PASSWORD&useSSL=false&useUnicode=true&characterEncoding=utf8" // serverTimezone=UTC&
}
