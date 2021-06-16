package cn.mine.flinkKafaka

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.collection.JavaConverters.seqAsJavaListConverter

object FlinkKafkaTest {

    def main(args: Array[String]): Unit = {
        // 获取 Kafka 运行环境
        val kafkaProps = new Properties()
        kafkaProps.setProperty("bootstrap.servers", "bssit-cdh-1:9092,bssit-cdh-2:9092,bssit-cdh-3:9092")
        kafkaProps.setProperty("group.id", "(0_0)")
        val topicList = List("maxwells", "binlog_properties")

        // 获取 Flink 运行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.of(10, TimeUnit.SECONDS)))

        // 初始化 FlinkKafka 消费者
         val dataStream = new FlinkKafkaConsumer[String](topicList.asJava, new SimpleStringSchema(), kafkaProps)
//        val dataStream = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), kafkaProps)

        // 初始化消费者流
        val transaction = env.addSource(dataStream)
        transaction.print()
        env.execute()
    }
}
