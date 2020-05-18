package com.travel.utils

import java.util.Date
import java.util.regex.Pattern

import com.travel.bean._
import com.travel.common._
import com.travel.loggings.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit
import org.apache.hadoop.hbase.util.{Base64, Bytes, MD5Hash}
import org.apache.hadoop.mapreduce.Job
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
  * Created by laowang
  */
object HbaseTools extends Logging with Serializable {



  def getHbaseConfiguration(): Configuration = {
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node01,node02,node03")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.setInt("hbase.client.operation.timeout", 3000)
    hconf
  }


  // 对指定的列构造rowKey,采用Hash前缀拼接业务主键的方法
  def rowKeyWithHashPrefix(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val hash_prefix = getHashCode(rkString)
    val rowKey = Bytes.add(Bytes.toBytes(hash_prefix), Bytes.toBytes(rkString))
    rowKey
  }

  // 对指定的列构造rowKey, 采用Md5 前缀拼接业务主键方法，主要目的是建表时采用MD5 前缀进行预分区
  def rowKeyWithMD5Prefix(separator:String,length: Int,column: String*): Array[Byte] = {
    val columns = column.mkString(separator)

    var md5_prefix = MD5Hash.getMD5AsHex(Bytes.toBytes(columns))
    if (length < 8){
      md5_prefix = md5_prefix.substring(0, 8)
    }else if (length >= 8 || length <= 32){
      md5_prefix = md5_prefix.substring(0, length)
    }
    val row = Array(md5_prefix,columns)
    val rowKey = Bytes.toBytes(row.mkString(separator))
    rowKey
  }

  // 对指定的列构造RowKey,采用MD5方法
 /* def rowKeyByMD5(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val md5 = MD5Hash.getMD5AsHex(Bytes.toBytes(rkString))
    val rowKey = Bytes.toBytes(md5.substring(0 , 16))
    rowKey
  }*/
  // 直接拼接业务主键构造rowKey
  def rowKey(column:String*):Array[Byte] = Bytes.toBytes(column.mkString(""))

  // Hash 前缀的方法：指定列拼接之后与最大的Short值做 & 运算
  // 目的是预分区，尽量保证数据均匀分布
  private def getHashCode(field: String): Short ={
    (field.hashCode() & 0x7FFF).toShort
  }



  /**
    * @param tablename 表名
    * @param regionNum 预分区数量
    * @param columns 列簇数组
    */
  def createHTableByHexStringSplit(connection: Connection, tablename: String,regionNum: Int, columns: Array[String]): Unit = {
    this.synchronized{
      val hexsplit: HexStringSplit = new HexStringSplit()
      // 预先构建分区，指定分区的start key
      val splitkeys: Array[Array[Byte]] = hexsplit.split(regionNum)
      val admin = connection.getAdmin
      val tableName = TableName.valueOf(tablename)
      if (!admin.tableExists(tableName)) {
        val tableDescriptor = new HTableDescriptor(tableName)
        if (columns != null) {
          columns.foreach(c => {
            val hcd = new HColumnDescriptor(c.getBytes()) //设置列簇
            hcd.setMaxVersions(1)
            hcd.setBloomFilterType(BloomType.ROW)
            hcd.setCompressionType(Algorithm.SNAPPY) //设定数据存储的压缩类型.默认无压缩(NONE)
            tableDescriptor.addFamily(hcd)
          })
        }
        admin.createTable(tableDescriptor,splitkeys)
      }
    }

  }




  /**
    * @param mapData 要插入的数据[列名 ， 值]
    * */

  def putMapData(conn:Connection, tableName: String, rowKey:String, mapData:Map[String , Any]) = {
    val admin: Admin = conn.getAdmin
    if(!admin.tableExists(TableName.valueOf(tableName))){
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

      val familyDescriptor = new HColumnDescriptor(Constants.DEFAULT_DB_FAMILY)
      tableDescriptor.addFamily(familyDescriptor)

      admin.createTable(tableDescriptor)
    }

    val table: Table = conn.getTable(TableName.valueOf(tableName))
    try{
      val rowkey = Bytes.toBytes(RowkeyUtil.getRowKey(rowKey,Constants.DEFAULT_REGION_NUM))
      val put: Put = new Put(rowkey)
      if(mapData.size > 0){
        for((k , v) <- mapData){
          put.addColumn(Bytes.toBytes(columnFamily) ,Bytes.toBytes(k+"") , Bytes.toBytes(v+""))
        }
      }
      table.put(put)

    }catch{
      case e:Exception => e.printStackTrace()
        info("###############################################################")
    }finally {
      table.close()
    }

  }

  /**
    * 删除hdfs下的文件
    * @param url 需要删除的路径
    */
  def delete_hdfspath(url: String) {
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path: Path = new Path(url)
    if (hdfs.exists(path)) {
      val filePermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ)
      hdfs.delete(path, true)
    }
  }
  def convertScanToString(scan: Scan):String={
    val proto = ProtobufUtil.toScan(scan)
    return Base64.encodeBytes(proto.toByteArray)
  }









//  val configuration = HBaseConfiguration.create()
//  configuration.set("hbase.zookeeper.quorum",Constants.ZK_IP)
//  val connection = ConnectionFactory.createConnection(configuration)

  import scala.collection.JavaConverters._

  def saveBatchData(chengduListBuffer: ListBuffer[Put], CHENG_DU_GPS_TOPIC: String) = {
    HBaseUtil.savePuts(chengduListBuffer.asJava,CHENG_DU_GPS_TOPIC)
  }


  def saveToHBaseAndRedis(connection:Connection,jedis:Jedis, eachLine: ConsumerRecord[String, String]): ConsumerRecord[String,String] = {

    var rowkey = ""
    //司机ID
    var driverId = ""
    //订单ID
    var orderId = ""
    //经度
    var lng = ""
    //维度
    var lat = ""
    //时间戳
    var timestamp = ""

    val topic: String = eachLine.topic()
    val line: String = eachLine.value()
    //成都数据
    if(line.split(",").size > 4){
      if(!line.contains("end") ){
        //非结束数据，保存到hbase里面去
        //成都数据
        val strings: Array[String] = line.split(",")
        val split: Array[String] = line.split(",")
        driverId = split(0)
        orderId = split(1)
        timestamp = split(2)
        lng = split(3)
        lat = split(4)
        rowkey = orderId + "_" + timestamp
        val put = new Put(rowkey.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"CITYCODE".getBytes(),Constants.CITY_CODE_CHENG_DU.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DRIVERID".getBytes(),driverId.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"ORDERID".getBytes(),orderId.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"TIMESTAMP".getBytes(),(timestamp+"").getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"TIME".getBytes(),DateUtils.formateDate(new Date((timestamp + "000").toLong),"yyyy-MM-dd HH:mm:ss").getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"LNG".getBytes(),lng.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"LAT".getBytes(),lat.getBytes())
        val table: Table = connection.getTable(TableName.valueOf(Constants.HTAB_GPS))
        table.put(put)
        table.close()

      }
      //数据保存到redis里面去
      if(line.split(",").size == 5 || line.contains("end")){
        JedisUtil.saveChengDuJedis(line)
      }
      //无论如何，成都数据都需要往下传递
    }else{
      //海口数据
      /**
        * 17595848583981 3 83 0898 460108 1 0 5 0 0 1642 0000-00-00 00:00:00 2017-09-20 03:20:00 14 NULL 2932979a59c14a3200007183013897db 3 110.4613 19.9425 110.462 19.9398 2017 09 20
        */
      var rowkey: String = ""
      val fields: Array[String] = line.split("\t")
      //println(fields.length)
      if(fields.length == 24 && !line.contains("dwv_order_make_haikou")){
        //订单ID+出发时间作为hbase表的rowkey
        rowkey = fields(0) + "_" + fields(13).replaceAll("-", "") + fields(14).replaceAll(":", "")
        val put = new Put(rowkey.getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"ORDER_ID".getBytes(),fields(0).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"PRODUCT_ID".getBytes(),fields(1).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"CITY_ID".getBytes(),fields(2).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DISTRICT".getBytes(),fields(3).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"COUNTY".getBytes(),fields(4).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"TYPE".getBytes(),fields(5).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"COMBO_TYPE".getBytes(),fields(6).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"TRAFFIC_TYPE".getBytes(),fields(7).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"PASSENGER_COUNT".getBytes(),fields(8).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DRIVER_PRODUCT_ID".getBytes(),fields(9).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"START_DEST_DISTANCE".getBytes(),fields(10).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"ARRIVE_TIME".getBytes(),fields(11).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DEPARTURE_TIME".getBytes(),fields(12) .getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"PRE_TOTAL_FEE".getBytes(),fields(13).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"NORMAL_TIME".getBytes(),fields(14).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"BUBBLE_TRACE_ID".getBytes(),fields(15).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"PRODUCT_1LEVEL".getBytes(),fields(16).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DEST_LNG".getBytes(),fields(17).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DEST_LAT".getBytes(),fields(18).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"STARTING_LNG".getBytes(),fields(19).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"STARTING_LAT".getBytes(),fields(20).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"YEAR".getBytes(),fields(21).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"MONTH".getBytes(),fields(22).getBytes())
        put.addColumn(Constants.DEFAULT_FAMILY.getBytes(),"DAY".getBytes(),fields(23).getBytes())
        val table: Table = connection.getTable(TableName.valueOf(Constants.HTAB_HAIKOU_ORDER))
        table.put(put)
        table.close()
      }
    }
    eachLine

  }


  def getStreamingContextFromHBase(streamingContext: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String], group: String,matchPattern:String): InputDStream[ConsumerRecord[String, String]] = {
    val connection: Connection = getHbaseConn
    val admin: Admin = connection.getAdmin
    var getOffset:collection.Map[TopicPartition, Long]  = HbaseTools.getOffsetFromHBase(connection,admin,topics,group)
    val result = if(getOffset.size > 0){
      val consumerStrategy: ConsumerStrategy[String, String] =  ConsumerStrategies.SubscribePattern[String,String](Pattern.compile(matchPattern),kafkaParams,getOffset)
      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,consumerStrategy)
      value
    }else{
      val consumerStrategy: ConsumerStrategy[String, String] =  ConsumerStrategies.SubscribePattern[String,String](Pattern.compile(matchPattern),kafkaParams)
      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,consumerStrategy)
      value
    }
    admin.close()
    connection.close()
    result
  }


  def getOffsetFromHBase(connection: Connection,admin: Admin,topics: Array[String], group: String): collection.Map[TopicPartition, Long] = {
    if(!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))){
      val chengdu_gps_offset = new HTableDescriptor(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
      chengdu_gps_offset.addFamily(new HColumnDescriptor(Constants.HBASE_OFFSET_FAMILY_NAME))
      admin.createTable(chengdu_gps_offset)
      admin.close();
    }
    val table = connection.getTable(TableName.valueOf(ConfigUtil.getConfig(Constants.HBASE_OFFSET_STORE_TABLE)))
    var myReturnValue:collection.Map[TopicPartition, Long] = new mutable.HashMap[TopicPartition,Long]()
    for(eachTopic <- topics){
      val get = new Get((group+":"+eachTopic).getBytes())
      val result: Result = table.get(get)
      val cells: Array[Cell] = result.rawCells()
      for(result <- cells){
        //列名  group:topic:partition
        val topicPartition: String = Bytes.toString( CellUtil.cloneQualifier(result))
        //列值 offset
        val offsetValue: String = Bytes.toString(CellUtil.cloneValue(result))
        //切割列名，获取 消费组，消费topic，消费partition
        val strings: Array[String] = topicPartition.split(":")
        val myStr = strings(2)
        //println(myStr)
        val partition =  new TopicPartition(strings(1),strings(2).toInt)
        myReturnValue += (partition -> offsetValue.toLong)
      }
    }
    table.close()
    myReturnValue
  }

  def loadHBaseData(sparkSession:SparkSession,conf:Configuration):DataFrame = {
    val context: SparkContext = sparkSession.sparkContext
    conf.set(TableInputFormat.INPUT_TABLE, Constants.HTAB_HAIKOU_ORDER)
    val scan = new Scan
    //ORDER_ID String,CITY_ID String,STARTING_LNG String,STARTING_LAT String
    scan.addFamily(Bytes.toBytes("f1"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("ORDER_ID"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("CITY_ID"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LNG"))
    scan.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("STARTING_LAT"))
    //添加scan  使用hbase的conf来设置Scan对象 conf里面只能带上字符串
    conf.set(TableInputFormat.SCAN, SparkUtils.convertScanToString(scan))
    val rddResult: RDD[(ImmutableBytesWritable, Result)] = context.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    //

    import sparkSession.implicits._

    val haiKouOrderRdd: RDD[HaiKouOrder] = rddResult.mapPartitions(eachPartition => {
      val orders: Iterator[HaiKouOrder] = eachPartition.map(eachResult => {
        val value: Result = eachResult._2
        val order_id: String = Bytes.toString(value.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("ORDER_ID")))
        val city_id: String = Bytes.toString(value.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("CITY_ID")))
        val starting_lng: String = Bytes.toString(value.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LNG")))
        val starting_lat: String = Bytes.toString(value.getValue(Bytes.toBytes(Constants.DEFAULT_FAMILY), Bytes.toBytes("STARTING_LAT")))
        HaiKouOrder(order_id, city_id, starting_lng, starting_lat)
      })
      orders
    })
    haiKouOrderRdd.toDF
  }

  /**
    * 保存hbase数据
    */
  def saveOrWriteData(configuration: Configuration, result: RDD[Row], tableName: String): Unit = {
    configuration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(configuration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val finalSavePaiRdd: RDD[(ImmutableBytesWritable, Put)] = result.mapPartitions(eachPartition => {
      val tuples: Iterator[(ImmutableBytesWritable, Put)] = eachPartition.map(eachRow => {
        //order_id:String,city_id:String, starting_lng:String,starting_lat:String
        val order_id: String = eachRow.getString(0)
        val city_id: String = eachRow.getString(1)
        val lng: String = eachRow.getString(2)
        val lat: String = eachRow.getString(3)
        val station_count: Int = eachRow.getInt(4)
        val district_name: String = eachRow.getString(5)
        val put = new Put(Bytes.toBytes(order_id))
        val immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(order_id))
        put.addColumn(Bytes.toBytes("f1"),
          Bytes.toBytes("ORDER_ID"),
          Bytes.toBytes(order_id))
        put.addColumn(Bytes.toBytes("f1"),
          Bytes.toBytes("CITY_ID"),
          Bytes.toBytes(city_id))
        put.addColumn(Bytes.toBytes("f1"),
          Bytes.toBytes("STARTING_LNG"),
          Bytes.toBytes(lng))
        put.addColumn(Bytes.toBytes("f1"),
          Bytes.toBytes("STARTING_LAT"),
          Bytes.toBytes(lat))
        put.addColumn(Bytes.toBytes("f1"),
          Bytes.toBytes("DISTRICT_NAME"),
          Bytes.toBytes(district_name))
        put.addColumn(Bytes.toBytes("f1"),
          Bytes.toBytes("ORDER_ID"),
          Bytes.toBytes(order_id))
        put.addColumn(Bytes.toBytes("f1"),
          Bytes.toBytes("STATION_COUNT"),
          Bytes.toBytes(station_count))
        (immutableBytesWritable, put)
      })
      tuples
    })
    //创建HBase表
    HBaseUtil.createTable(HBaseUtil.getConnection, tableName, Constants.DEFAULT_FAMILY)
    finalSavePaiRdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  def getHbaseConn: Connection = {
    try{
      val config:Configuration = HBaseConfiguration.create()
      config.set("hbase.zookeeper.quorum" , GlobalConfigUtils.getProp("hbase.zookeeper.quorum"))
   //   config.set("hbase.master" , GlobalConfigUtils.getProp("hbase.master"))
      config.set("hbase.zookeeper.property.clientPort" , GlobalConfigUtils.getProp("hbase.zookeeper.property.clientPort"))
      //      config.set("hbase.rpc.timeout" , GlobalConfigUtils.rpcTimeout)
      //      config.set("hbase.client.operator.timeout" , GlobalConfigUtils.operatorTimeout)
      //      config.set("hbase.client.scanner.timeout.period" , GlobalConfigUtils.scannTimeout)
      //      config.set("hbase.client.ipc.pool.size","200");
      val connection = ConnectionFactory.createConnection(config)
      connection

    }catch{
      case exception: Exception =>
        error(exception.getMessage)
        error("HBase获取连接失败")
        null
    }
  }

  def closeConn(conn : Connection) = {
    try {
      if (!conn.isClosed){
        conn.close()
      }
    }catch {
      case exception: Exception =>
        error("HBase连接关闭失败")
        exception.printStackTrace()
    }
  }


  def saveBatchOffset(group: String, topic: String, partition: String, offset: Long): Unit = {
    val conn: Connection = HbaseTools.getHbaseConn
    val table: Table = conn.getTable(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
    val rowkey = group + ":" + topic
    val columName = group + ":" + topic + ":" + partition
    val put = new Put(rowkey.getBytes())
    put.addColumn(Constants.HBASE_OFFSET_FAMILY_NAME.getBytes(),columName.getBytes(),offset.toString.getBytes())
    table.put(put)

    table.close()
    conn.close()

  }


  val columnFamily = GlobalConfigUtils.heartColumnFamily


  /**
    *
    * syn.table.order_info="order_info"
    * syn.table.renter_info="renter_info"
    * syn.table.driver_info="driver_info"
    * syn.table.opt_alliance_business="opt_alliance_business"
    *
    *
    */

  val order_info_table = GlobalConfigUtils.getProp("syn.table.order_info")
  val renter_info_table = GlobalConfigUtils.getProp("syn.table.renter_info")
  val driver_info_table = GlobalConfigUtils.getProp("syn.table.driver_info")
  val opt_alliance_business = GlobalConfigUtils.getProp("syn.table.opt_alliance_business")


  def saveBusinessDatas(interpreter:String , parse: (String, Any) , conn:Connection): Unit ={
    interpreter match {
      //订单表
      case tableName if(tableName.startsWith(order_info_table)) =>
        val data = parse._2.asInstanceOf[OrderInfo]
        //落地Hbase表明 = database_table
        val tableName = order_info_table
        val type_ = data.operatorType
        if(data.id !=null){
          //数据落地操作
          val mapData: Map[String, String] = ReflectBean.reflect(data, tableName)
          //订单时间
          val create_time = data.create_time
          val tmpTime = TimeUtils.formatYYYYmmdd(create_time).get
          // 确保时间对上
          val rowkey = data.id+ "_" +tmpTime
          // 暂时只处理新增数据
          type_ match {
            case "insert" =>
              HbaseTools.putMapData(conn ,tableName,  rowkey, mapData)
            case "update" =>
              HbaseTools.putMapData(conn ,tableName, rowkey, mapData)
            case _=> Nil
          }
        }

      //用户表
      case tableName if(tableName.equals(renter_info_table) ||  tableName == renter_info_table) =>
        //注册表(用户表)
        val data = parse._2.asInstanceOf[RegisterUsers]
        //落地Hbase表明 = database_table
        val tableName = renter_info_table
        val type_ = data.operatorType
        //数据落地操作
        val mapData: Map[String, String] = ReflectBean.reflect(data, tableName)
        //订单时间
        val create_time = Some(data.create_time).getOrElse(data.last_logon_time)
        val tmpTime = TimeUtils.formatYYYYmmdd(create_time).get
        //          val times = tmpTime.substring(0, tmpTime.length - 2)
        // 确保时间对上
        val rowkey = data.id + "_" +tmpTime
        type_ match {
          case "insert" =>
            HbaseTools.putMapData(conn ,tableName ,rowkey, mapData)
          case "update" =>
            HbaseTools.putMapData(conn ,tableName, rowkey, mapData)
          case _ => Nil
        }
      //司机表
      case tableName if(tableName.equals(driver_info_table) ||  tableName == driver_info_table) =>
        //注册表（司机）
        val data = parse._2.asInstanceOf[DriverInfo]
        val tableName = driver_info_table
        val type_ = data.operatorType
        val mapData: Map[String, String] = ReflectBean.reflect(data, tableName)
        //rowkey : 司机Id_年月日
        val create_time = Some(data.create_time).getOrElse(data.id)
        val tmpTime = TimeUtils.formatYYYYmmdd(create_time).get
        //        val rowkey = data.id + "_" + times
        val rowkey = data.id  + "_" +tmpTime
        type_ match {
          case "insert" =>
            HbaseTools.putMapData(conn ,tableName, rowkey, mapData)
          case "update" =>
            HbaseTools.putMapData(conn ,tableName, rowkey, mapData)
          case _ => Nil
        }
      //加盟表
      case tableName if(tableName.equals(opt_alliance_business) || tableName == opt_alliance_business) =>

        val data = parse._2.asInstanceOf[Opt_alliance_business]
        val tableName = opt_alliance_business
        val type_ = data.operatorType
        val mapData: Map[String, String] = ReflectBean.reflect(data, tableName)
        val create_time = Some(data.create_time).getOrElse(data.update_time)
        val tmpTime = TimeUtils.formatYYYYmmdd(create_time).get
        val rowkey = data.id_ + "_" + tmpTime
        type_ match {
          case "insert" =>
            HbaseTools.putMapData(conn ,tableName,  rowkey, mapData)
          case "update" =>
            HbaseTools.putMapData(conn ,tableName,  rowkey, mapData)
          case _ => Nil
        }
      case _=>
        info("######其他表##### : {}"  , interpreter)
    }

  }




}
