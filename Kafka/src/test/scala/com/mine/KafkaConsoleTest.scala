package com.mine

import java.util.Properties

import com.mine.console.KafkaConsole._
import com.mine.propertyprepare.PropertyPrepare
import org.junit.Test

import scala.collection.JavaConverters._

class KafkaConsoleTest {

    private val kafkaServersProps: Properties = getKafkaServersProps
    private val kafkaProducerProps: Properties = getKafkaProducerProps
    private val kafkaConsumerProps: Properties = getKafkaConsumerProps

    @Test
    def testPrintGetKafkaConsumerProps(): Unit = getKafkaServersProps.asScala.foreach(println)

    // 获取 Kafka 的消费者组列表
    @Test
    def testGetGroupsList(): Unit = getGroupsList(kafkaServersProps).foreach(println)

    // 获取 Kafka 的 Topic 列表
    @Test
    def testGetTopicsList(): Unit = getTopicsList(kafkaServersProps).foreach(println)

    @Test
    def testGetServersProps(): Unit = println(kafkaServersProps)

    @Test
    def testGetProducerProps(): Unit = println(kafkaProducerProps)

    @Test
    def testGetConsumerProps(): Unit = println(kafkaConsumerProps)

    @Test
    def testProducerSendMsg(): Unit = producerSendMsg(PropertyPrepare.KAFKA_TOPIC)

    @Test
    def testConsumerSubscribeMsg(): Unit = consumerSubscribeMsg(List(PropertyPrepare.KAFKA_TOPIC))
}
