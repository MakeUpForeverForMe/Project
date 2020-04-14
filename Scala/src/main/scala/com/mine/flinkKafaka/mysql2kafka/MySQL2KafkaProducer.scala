package com.mine.flinkKafaka.mysql2kafka

import java.util.Properties

import com.mine.flinkKafaka.mysql2kafka.bean.Customer
import com.mine.jdbcutil.dao.BaseDAO
import com.mine.jdbcutil.utils.JDBCUtils
import com.mine.propertyutil.GetValueByKeyFromProperty
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object MySQL2KafkaProducer extends BaseDAO[Customer] {


    def main(args: Array[String]): Unit = {
        // 获取 MySQL 数据
        val connection = JDBCUtils.getConnection(props)
        val customer = daoGetDataOne(connection, "select keywords from drip_loan_binlog_filter")
        // 发送到 Kafka
        val producer = new KafkaProducer[String, String](getKafkaProducerProps)
        producer.send(new ProducerRecord(props.KAFKA_TOPIC, customer.keywords))
        producer.close()
    }

    private lazy val props = GetValueByKeyFromProperty("conf.properties")

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
}
