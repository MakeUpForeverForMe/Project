package com.mine

import com.mine.console.KafkaConsole
import org.junit.Test

import scala.collection.JavaConverters._

class KafkaConsoleTest {

    private val kafkaConsole = new KafkaConsole("conf.properties")

    import kafkaConsole._

    @Test
    def testPrintGetKafkaConsumerProps(): Unit = getKafkaServersProps.asScala.foreach(println)

    // 获取 Kafka 的消费者组列表
    @Test
    def testGetGroupsList(): Unit = getGroupsList(getKafkaServersProps).foreach(println)

    // 获取 Kafka 的 Topic 列表
    @Test
    def testGetTopicsList(): Unit = getTopicsList(getKafkaServersProps).foreach(println)

    @Test
    def testGetServersProps(): Unit = println(getKafkaServersProps)

    @Test
    def testGetProducerProps(): Unit = println(getKafkaProducerProps)

    @Test
    def testGetConsumerProps(): Unit = println(getKafkaConsumerProps)

    @Test
    def testProducerSendMsg(): Unit = producerSendMsg("hello")

    @Test
    def testConsumerSubscribeMsg(): Unit = consumerSubscribeMsg()
}
