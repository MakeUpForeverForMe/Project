package com.mine

import java.util.Properties

import com.mine.KafkaConsole.{getGroupsList, getKafkaProps, getTopicsList}
import com.mine.projectUtils.PropertyUtil
import org.junit.Test

import scala.collection.JavaConverters._

class KafkaConsoleTest {

    def testGetKafkaProps: Properties = getKafkaProps

    @Test
    def testPrintGetKafkaConsumerProps(): Unit = getKafkaProps.asScala.foreach(println)

    // 获取 Kafka 的消费者组列表
    @Test
    def testGetGroupsList(): Unit = getGroupsList(testGetKafkaProps).foreach(println)

    // 获取 Kafka 的 Topic 列表
    @Test
    def testGetTopicsList(): Unit = getTopicsList(testGetKafkaProps).foreach(println)

    @Test
    def testGetProps(): Unit = {
        PropertyUtil.initProperty("config.properties")
        val str = PropertyUtil.getPropertyValueByKey("kafka.bootstrap.servers")
        println(str)
    }
}
