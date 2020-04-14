package com.mine.kafkautil

import java.time.Duration
import java.util.Properties
import java.util.stream.Collectors

import com.mine.propertyutil.PropertyUtil
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.JavaConverters._

object KafkaUtil {
    def apply: KafkaUtil = new KafkaUtil()

    def apply(fileName: String): KafkaUtil = new KafkaUtil(fileName)

    def apply(props: PropertyUtil): KafkaUtil = new KafkaUtil(props)
}

class KafkaUtil {

    private var props: PropertyUtil = _

    def this(fileName: String) = {
        this
        this.props = PropertyUtil(fileName)
    }

    def this(props: PropertyUtil) = {
        this
        this.props = props
    }

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
      * @tparam A 要发送的 Message 的 Key 类型
      * @tparam B 要发送的 Message 的 Value 类型
      */
    def producerSendMsg[A, B](msg: B): Unit = {
        // 生产者
        val producer = new KafkaProducer[A, B](getKafkaProducerProps)
        producer.send(new ProducerRecord(props.KAFKA_TOPIC, msg))
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