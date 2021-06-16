package cn.mine.util

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._

/**
 * @author ximing.wei 2021-06-15 16:25:00
 */
object KafkaUtil {
    private var adminClient: AdminClient = _

    /**
     * 消费者从 Kafka 订阅消息
     */
    def consumerSubscribeMsg(): Unit = {
        val consumer = new KafkaConsumer[String, String](getKafkaConsumerProps)
        consumer.subscribe(ConfigUtil.KAFKA_TOPIC.value.split(",").toList.asJava)
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
        val producer = new KafkaProducer[A, B](getKafkaProducerProps)
        producer.send(new ProducerRecord(ConfigUtil.KAFKA_TOPIC.value, msg))
        producer.flush()
        producer.close()
    }

    /**
     * 获取 Kafka 的 Topic 列表
     *
     * @return 返回 Kafka 的 Topic 列表
     */
    def getTopicsList: List[String] = getAdminClient.listTopics.names.get.asScala.toList

    /**
     * 获取 Kafka 的消费者组列表
     *
     * @return 返回 Kafka 的消费者组列表
     */
    def getConsumerGroupList: List[String] = getAdminClient.listConsumerGroups.valid.get.asScala.toList.map(line => line.groupId())

    def getAdminClient: AdminClient = {
        if (adminClient == null) adminClient = AdminClient.create(getKafkaServersProps)
        adminClient
    }

    /**
     * 获取 Kafka 消费者运行环境
     *
     * @return 返回 Kafka 消费者运行环境
     */
    def getKafkaConsumerProps: Properties = {
        val kafkaConsumerProps = new Properties()
        kafkaConsumerProps.putAll(getKafkaServersProps)
        kafkaConsumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, ConfigUtil.KAFKA_GROUP.value) // 指定消费者所在的消费者组
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConfigUtil.KAFKA_KEY_DESERIALIZER.value)
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConfigUtil.KAFKA_VALUE_DESERIALIZER.value)
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
        kafkaProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ConfigUtil.KAFKA_KEY_SERIAL.value)
        kafkaProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfigUtil.KAFKA_VALUE_SERIAL.value)
        kafkaProducerProps
    }

    /**
     * 获取 Kafka 基础运行环境
     *
     * @return 返回 Kafka 基础运行环境
     */
    def getKafkaServersProps: Properties = {
        val kafkaServersProps = new Properties()
        kafkaServersProps.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigUtil.KAFKA_SERVERS.value)
        kafkaServersProps
    }
}
