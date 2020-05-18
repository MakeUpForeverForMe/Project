package com.travel.programApp

import java.util

import com.travel.common.{Constants, District}
import com.travel.utils.{HbaseTools, SparkUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.jts.geom.{GeometryFactory, Point, Polygon}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

object SparkSQLVirtualStation {
  def main(args: Array[String]): Unit = {
    //第一件事：读取hbase的数据

    //第二件事儿：计算虚拟车站

    //第三件事儿：求取出每一个区域边界

    //第四件事儿：判断虚拟车站属于哪一个区域

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkSqlVirtutalStation")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val context: SparkContext = sparkSession.sparkContext
    context.setLogLevel("WARN")


    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node01,node02,node03")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")

    //使用sparkContext.newAPIHadoopRDD这个方法实现了数据的获取



    //读取hbase的数据

    // context.newAPIHadoopRDD()  这种方式不太好，会对hbase的表进行扫描，给hbase带来一些压力  提供其他的解决方案



    val hbaseFrame: DataFrame = HbaseTools.loadHBaseData(sparkSession,hconf)
    //将我们获取的数据注册成为一张表
    hbaseFrame.createOrReplaceTempView("order_df")


    //将这个代码，封装到一个方法里面去  就不会报错
   // val h3: H3Core = H3Core.newInstance()  //在main方法里面初始化对象，存在序列化的问题

    /*//现在所有的数据都拿到了，通过这些数据来计算虚拟车站
    //通过经纬度可以计算出编码值  通过上车点来计算虚拟车站
    //通过h3来实现计算
    val h3: H3Core = H3Core.newInstance()  //初始化一个对象

    //通过经纬度来得到h3的编码值  自定义sparkSQL的udf函数，然后通过自定义udf函数，将经纬度转换成为h3的编码值
    sparkSession.udf.register("locationToH3",new UDF3[String,String,Int,Long] {
      override def call(lat: String, lng: String, result: Int): Long = {
        h3.geoToH3(lat.toDouble,lng.toDouble,result)

      }
    },DataTypes.LongType)

    //order_id:String,city_id:String, starting_lng:String,starting_lat:String  将df注册成为一张表，可以通过sql语句的方式去求取虚拟车站


    //定义sql语句，通过自定义函数，实现数据虚拟车站的计算
    val order_sql =
      """
        |select  order_id,city_id,starting_lng,starting_lat,locationToH3(starting_lat,starting_lng,12) as h3code from order_df
      """.stripMargin

    //这里面就得到了所有的额虚拟车站，如何在每一个地方取一个车站
    val gridDf: DataFrame = sparkSession.sql(order_sql)

    val sql: String =
      s"""
         | select
         |order_id,
         |city_id,
         |starting_lng,
         |starting_lat,
         |row_number() over(partition by order_grid.h3code order by starting_lng,starting_lat asc) rn
         | from order_grid  join (
         | select h3code,count(1) as totalResult from order_grid  group by h3code having totalResult >=1
         | ) groupcount on order_grid.h3code = groupcount.h3code
         |having(rn=1)
      """.stripMargin


    //最终得到每一个地区的虚拟车站
    val frame: DataFrame = sparkSession.sql(sql)
*/
    //可以运行没问题的，上面的代码会报错
    val virtual_rdd: RDD[Row] = SparkUtils.getVirtualFrame(sparkSession)


    //每一个虚拟车站，属于哪一个区？？？  需要我们将虚拟车站，与每一个区进行比较
    //获取每一个区的边界线   其实就是很多经纬度  ==》 有很多办法，例如高德地图可以调接口，百度地图

    //获取边界数据与虚拟车站的数据进行比较  边界的数据比较少  虚拟车站的数据比较多
    //使用spark当中的广播变量，将边界的数据广播出去  边界数据获取到了，并且广播出去了
    val districtBrodaCastVar: Broadcast[util.ArrayList[District]] = SparkUtils.broadCastDistrictValue(sparkSession)


    //比较虚拟车站与边界的数据的比较 ，判断虚拟车站，落在哪一个边界里面了

    //获取到了每一个区有多少个虚拟车站
    val finalSaveRow: RDD[mutable.Buffer[Row]] = virtual_rdd.mapPartitions(eachPartition => {

      //做我们图形化的操作，将边界的经纬度值传入进去，帮我们连接起来  ===》变成一个图像
      val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory(null)
      val reader = new WKTReader(geometryFactory)

      //获取所有边界的数据
      val wktPolygons: mutable.Buffer[(District, Polygon)] = SparkUtils.changeDistictToPolygon(districtBrodaCastVar, reader)


      //获取到了每一条虚拟车站的数据
      eachPartition.map(row => {
        //获取每一个虚拟车站的经纬度，判断虚拟车站经纬度究竟在哪一个区里面
        val lng: String = row.getAs[String]("starting_lng")
        val lat: String = row.getAs[String]("starting_lat")

        //将经纬度信息转换成为一个点，与面进行判断
        val wktPoint = "POINT(" + lng + " " + lat + ")"
        val point: Point = reader.read(wktPoint).asInstanceOf[Point]

        //获取到了每一个区域的边界，进行判断
        val rows: mutable.Buffer[Row] = wktPolygons.map(polygon => {
          //包含这个点
          if (polygon._2.contains(point)) {
            val fields: Array[Any] = row.toSeq.toArray ++ Seq(polygon._1.getName)
            Row.fromSeq(fields)
          } else {
            null
          }
        }).filter(null != _)
        rows //过滤为空的数据

      })

    })
    //将我们最终的结果数据给保存起来
    val rowRdd: RDD[Row] = finalSaveRow.flatMap(x => x )

    //将最终结果，保存到hbase里面去，供我们的前台页面进行查询
    HbaseTools.saveOrWriteData(hconf,rowRdd,Constants.VIRTUAL_STATION)

  }


}
