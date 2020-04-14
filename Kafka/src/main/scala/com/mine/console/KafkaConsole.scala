package com.mine.console

import java.time.Duration
import java.util.Properties
import java.util.stream.Collectors

import com.mine.propertyutil.GetValueByKeyFromProperty
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer._

import scala.collection.JavaConverters._

object KafkaConsole {
    def apply: KafkaConsole = new KafkaConsole()

    def apply(fileName: String): KafkaConsole = new KafkaConsole(fileName)
}

class KafkaConsole {
    private var fileName: String = _

    def this(fileName: String) = {
        this
        this.fileName = fileName
    }

    private lazy val props = GetValueByKeyFromProperty(fileName)

    /**
      * 消费者从 Kafka 订阅消息
      */
    def consumerSubscribeMsg(): Unit = {
        // 消费者
        val consumer = new KafkaConsumer[String, String](getKafkaConsumerProps)
        consumer.subscribe(props.KAFKA_TOPIC.split(",").toList.asJava)
        while (true) consumer.poll(Duration.ofMillis(100)).asScala.foreach(record => println(record.value()))
    }

    /**
      * 生产者向 Kafka 发送消息
      *
      * @param msg 要发送的 Message
      */
    def producerSendMsg(msg: String): Unit = {
        // 生产者
        val producer = new KafkaProducer[String, String](getKafkaProducerProps)
        for (i <- 1 to 10) producer.send(new ProducerRecord(props.KAFKA_TOPIC, msg + i))
        producer.close()
    }

    /**
      * 获取 Kafka 消费者运行环境
      *
      * @return 返回 Kafka 消费者运行环境
      */
    def getKafkaConsumerProps: Properties = {
        val kafkaConsumerProps = new Properties()
        kafkaConsumerProps.putAll(getKafkaServersProps)
        kafkaConsumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, props.KAFKA_GROUP) // 指定消费者所在的消费者组
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, props.KAFKA_KEY_DESERIAL)
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, props.KAFKA_VALUE_DESERIAL)
        kafkaConsumerProps
    }

    /**
      * 获取 Kafka 生产者运行环境
      *
      * @return 返回 Kafka 生产者运行环境
      */
    def getKafkaProducerProps: Properties = {
        val kafkaProducerProps = new Properties()
        kafkaProducerProps.putAll(getKafkaServersProps)
        kafkaProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.KAFKA_KEY_SERIAL)
        kafkaProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.KAFKA_VALUE_SERIAL)
        kafkaProducerProps
    }

    /**
      * 获取 Kafka 基础运行环境
      *
      * @return 返回 Kafka 基础运行环境
      */
    def getKafkaServersProps: Properties = {
        val kafkaServersProps = new Properties()
        kafkaServersProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, props.KAFKA_SERVERS)
        kafkaServersProps
    }

    /**
      * 获取 Kafka 的 Topic 列表
      *
      * @param kafkaServersProps 传入 Kafka 的配置信息
      * @return 返回 Kafka 的 Topic 列表
      */
    def getTopicsList(kafkaServersProps: Properties): List[String] = getAdminClient(kafkaServersProps).listTopics.names.get.asScala.toList

    /**
      * 获取 Kafka 的消费者组列表
      *
      * @param kafkaServersProps 传入 Kafka 的配置信息
      * @return 返回 Kafka 的消费者组列表
      */
    def getGroupsList(kafkaServersProps: Properties): List[String] = getAdminClient(kafkaServersProps).listConsumerGroups.valid.get.stream.collect(Collectors.toList()).asScala.toList.map(list => list.groupId)

    /**
      * 获取 Kafka 的 AdminClient 客户端
      *
      * @param kafkaServersProps 传入 Kafka的配置信息
      * @return 返回 Kafka 的 AdminClient 客户端
      */
    def getAdminClient(kafkaServersProps: Properties): AdminClient = AdminClient.create(kafkaServersProps)
}
