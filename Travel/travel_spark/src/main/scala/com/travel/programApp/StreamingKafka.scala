package com.travel.programApp

import com.travel.common.{ConfigUtil, Constants, HBaseUtil, JedisUtil}
import com.travel.utils.HbaseTools
import org.apache.hadoop.hbase.client.Connection
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object StreamingKafka {
  def main(args: Array[String]): Unit = {
    val brokers = ConfigUtil.getConfig(Constants.KAFKA_BOOTSTRAP_SERVERS)
    val topics = Array(ConfigUtil.getConfig(Constants.CHENG_DU_GPS_TOPIC),ConfigUtil.getConfig(Constants.HAI_KOU_GPS_TOPIC))
  //  val conf = new SparkConf().setMaster("local[1]").setAppName("sparkKafka")
    val group:String = "gps_consum_group"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",// earliest,latest,和none
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    //通过sparkStreaming消费kafka的数据将offset维护到hbase里面去
    //如果使用receiver方式来做消费  lcoal[1]
    val conf: SparkConf = new SparkConf().setMaster("local[1]").setAppName("sparKafka")
    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")
    //获取程序入口类
    val streamingContext:StreamingContext = new StreamingContext(context,Seconds(5))
    /**
      * ssc: StreamingContext,
      * locationStrategy: LocationStrategy,
      * consumerStrategy: ConsumerStrategy[K, V]
      */
    //查询hbase的offset值

   /* val partitionToLong = new mutable.HashMap[TopicPartition,Long]()

    //获取habse的表连接
    val conn: Connection = HbaseTools.getHbaseConn
    val admin: Admin = conn.getAdmin
    if(!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))){
      val hbaseoffsetstoretable = new HTableDescriptor(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
      hbaseoffsetstoretable.addFamily(new HColumnDescriptor(Constants.HBASE_OFFSET_FAMILY_NAME))
      admin.createTable(hbaseoffsetstoretable)
      admin.close()

    }
    val table: Table = conn.getTable(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
    //查询offset
    for(eachTopic <- topics){
      val rowkey =group + ":" + eachTopic
      val get = new Get(rowkey.getBytes)
      val result: Result = table.get(get)
      val cells: Array[Cell] = result.rawCells()  //获取每一个column
      for(eachCell <- cells){
        //获取到了offset的值
        val offsetValue: String = Bytes.toString(eachCell.getValue)
        val columnName: String = Bytes.toString(eachCell.getQualifier)
        val strings: Array[String] = columnName.split(":") //将列名切割之后，获取到了 topic以及partition
        val topicPartition = new TopicPartition(strings(1),strings(2).toInt)
        //将每一个分区里面的offset值都获取到了，然后保存到了map里面去了
        partitionToLong.+=(topicPartition -> offsetValue.toLong)
      }
    }
    /*
    带了offset值  从hbase里面查到了offset就带上
    pattern: ju.regex.Pattern,
      kafkaParams: collection.Map[String, Object],
      offsets: collection.Map[TopicPartition, Long]  offset值
//不带offset的值  查不到offse就不带offset
        pattern: ju.regex.Pattern,
      kafkaParams: collection.Map[String, Object]
     */
    val consumerStrategy: ConsumerStrategy[String, String] = if (partitionToLong.size > 0) {
      ConsumerStrategies.SubscribePattern[String,String](Pattern.compile("(.*)gps_topic"), kafkaParams, partitionToLong)
    } else {
      ConsumerStrategies.SubscribePattern[String,String](Pattern.compile("(.*)gps_topic"), kafkaParams)
    }
    consumerStrategy
    KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,consumerStrategy)
*/
    //每次从kafka里面获取到的数据，都在这里面了
    val result: InputDStream[ConsumerRecord[String, String]] = HbaseTools.getStreamingContextFromHBase(streamingContext,kafkaParams,topics,group,"(.*)gps_topic")
    //将数据保存到hbase以及redis里面去
    result.foreachRDD(eachRdd =>{
      //获取到了每一个分区里面的数据
      eachRdd.foreachPartition(eachPartition =>{
        val connection: Connection = HBaseUtil.getConnection
        val jedis: Jedis = JedisUtil.getJedis
        //获取到了每一条数据
        eachPartition.foreach(record =>{
          //保存数据到hbase以及redis里面去
          HbaseTools.saveToHBaseAndRedis(connection,jedis,record)
        })
        JedisUtil.returnJedis(jedis)
        connection.close()
      })
      //更新hbase的offset的值
      val ranges: Array[OffsetRange] = eachRdd.asInstanceOf[HasOffsetRanges].offsetRanges
     // result.asInstanceOf[CanCommitOffsets].commitAsync(ranges)  //将offset提交到了kafka里面去了
      for(eachRange <- ranges){
        val startOffset: Long = eachRange.fromOffset  //其实offset
        val endOffset: Long = eachRange.untilOffset  ///消费结束offset
        val topic: String = eachRange.topic   //topic
        val partition: Int = eachRange.partition  //partition
        //group: String, topic: String, partition: String, offset: Long
        HbaseTools.saveBatchOffset(group,topic,partition+"",endOffset.toLong)
      }
    })
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
