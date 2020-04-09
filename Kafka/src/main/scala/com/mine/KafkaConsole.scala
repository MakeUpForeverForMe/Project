package com.mine

import java.util.Properties
import java.util.stream.Collectors

import com.mine.projectUtils.PropertyUtil
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConverters._

object KafkaConsole {
    def main(args: Array[String]): Unit = {
        val kafkaServersProps: Properties = getKafkaServersProps

        val kafkaProducerProps: Properties = getKafkaProducerProps(kafkaServersProps)

        //        val producer = new KafkaProducer[_, _](kafkaProps)
        // 发送
        //        var i = 0
        //        while (i < 100) {
        //            println(i)
        //            //            producer.send(new ProducerRecord[String, String]("mytopic", "hello" + i))
        //            //            i += 1
        //            //            i - 1
        //            i += 1
        //        }

        val kafkaConsumerProps: Properties = getKafkaConsumerProps(kafkaServersProps)

        // 获取消费者
        val consumer = new KafkaConsumer[Byte, Byte](kafkaConsumerProps)

        //consumer.metrics().asScala.foreach(println)
        //        consumer.listTopics().asScala.foreach(println)

        //        consumer.close()
    }

    /**
      * 获取 Kafka 消费者运行环境
      *
      * @param kafkaServersProps 传入 Kafka 的配置信息
      * @return 返回 Kafka 消费者运行环境
      */
    def getKafkaConsumerProps(kafkaServersProps: Properties): Properties = {
        val kafkaConsumerProps = new Properties(kafkaServersProps)
        kafkaConsumerProps.setProperty("group.id", "(0_0)") // 指定消费者所在的消费者组
        kafkaConsumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        kafkaConsumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        kafkaConsumerProps
    }

    /**
      * 获取 Kafka 生产者运行环境
      *
      * @param kafkaServersProps 传入 Kafka 的配置信息
      * @return 返回 Kafka 生产者运行环境
      */
    def getKafkaProducerProps(kafkaServersProps: Properties): Properties = {
        val kafkaProducerProps = new Properties(kafkaServersProps)
        kafkaProducerProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        kafkaProducerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        kafkaProducerProps
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
      * 获取 Kafka 基础运行环境
      *
      * @return 返回 Kafka 基础运行环境
      */
    def getKafkaServersProps: Properties = {
        PropertyUtil.initProperty("config.properties")
        val str = PropertyUtil.getPropertyValueByKey("kafka.bootstrap.servers")
        println(str)
        val kafkaServersProps = new Properties()
        // kafkaPropsServers.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "") // 与 bootstrap.servers 配置一样
        kafkaServersProps.setProperty("bootstrap.servers", "bssit-cdh-1:9092,bssit-cdh-2:9092,bssit-cdh-3:9092")
        kafkaServersProps
    }

    /**
      * 获取 Kafka 的 AdminClient 客户端
      *
      * @param kafkaServersProps 传入 Kafka的配置信息
      * @return 返回 Kafka 的 AdminClient 客户端
      */
    def getAdminClient(kafkaServersProps: Properties): AdminClient = AdminClient.create(kafkaServersProps)
}
