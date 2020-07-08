package com.mine.propertyutil

import java.util.Properties

object ConfigUtil {
  def apply(): ConfigUtil = new ConfigUtil()

  def apply(fileName: String): ConfigUtil = new ConfigUtil(fileName)
}

class ConfigUtil {

  private val properties = new Properties()

  def this(fileName: String) {
    this
    properties.load(getClass.getClassLoader.getResourceAsStream(fileName))
  }

  def getProps(key: String): String = properties.getProperty(key)

  def setProps(key: String, value: String): AnyRef = properties.setProperty(key, value)

  // MYSQL 配置
  final lazy val MYSQL_DRIVER: String = getProps("mysql.driver")
  final lazy val MYSQL_HOST: String = getProps("mysql.host")
  final lazy val MYSQL_DATABASE: String = getProps("mysql.database")
  final lazy val MYSQL_USER: String = getProps("mysql.user")
  final lazy val MYSQL_PASSWORD: String = getProps("mysql.password")
  final lazy val MYSQL_URL: String = s"jdbc:mysql://$MYSQL_HOST:3306/$MYSQL_DATABASE?useSSL=false&useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai"

  // Kafka 配置
  final lazy val KAFKA_SERVERS: String = getProps("kafka.bootstrap.servers")
  final lazy val KAFKA_TOPIC: String = getProps("kafka.topic")
  final lazy val KAFKA_GROUP: String = getProps("kafka.group.id")
  final lazy val KAFKA_KEY_SERIAL: String = getProps("kafka.key.serializer")
  final lazy val KAFKA_VALUE_SERIAL: String = getProps("kafka.value.serializer")
  final lazy val KAFKA_KEY_DESERIALIZER: String = getProps("kafka.key.deserializer")
  final lazy val KAFKA_VALUE_DESERIALIZER: String = getProps("kafka.value.deserializer")

  // Hadoop 配置
  // Hive 配置
  // Flink 配置
  // HBase 配置
  // Spark 配置
}
