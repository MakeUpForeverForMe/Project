package com.travel.transaction

import java.util
import com.travel.utils.GetCenterPointFromListOfCoordinates
import com.uber.h3core.H3Core
import com.uber.h3core.util.GeoCoord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import redis.clients.jedis.GeoCoordinate

import scala.collection.JavaConverters

/**
  * Created by laowang
  */
object HotOrderTransation {
  val h3 = H3Core.newInstance()
  def init(sparkSession: SparkSession): Unit ={
    sparkSession.udf.register("locationToH3" , locationToH3 _)

    lazy val orderHotTmp =(time:Int) =>
      s"""
         |select
         |open_lng ,
         |open_lat ,
         |create_time ,
         |begin_address_code ,
         |locationToH3(open_lng , open_lat , 7) as h3Code
         |from
         |order
         |where ${time} <= cast(date_format(create_time , 'yyyyMMdd') as int)
    """.stripMargin

    //经纬度  转成h3
    sparkSession.sql(orderHotTmp(20190715)).createOrReplaceTempView("orderHotTmp")


    lazy  val getHotArea =
      """
        |select tb2.h3Code ,
        |tb2.num as count ,
        |tb2.create_time ,
        |tb2.begin_address_code
        |from
        |(select * ,
        |row_number()  over(partition by h3Code order by rank desc) num
        |from
        |(select * ,
        |row_number() over(partition by h3Code order by create_time) rank
        |from
        |orderHotTmp) tb) tb2
        |where  tb2.num = 1
      """.stripMargin


    val rdd:RDD[Row] = sparkSession.sql(getHotArea).rdd
    val reultHot: RDD[(String, String, String, Int)] = rdd.map { line =>
      val h3Code = line.getAs[Long]("h3Code")
      val count = line.getAs[Int]("count")
      val create_time = line.getAs[String]("create_time")
      val begin_address_code = line.getAs[String]("begin_address_code")

      val geoCood: List[GeoCoord] = h3To6(h3Code)

      val list = new util.ArrayList[GeoCoordinate]()
      for (in <- geoCood) {
        list.add(new GeoCoordinate(in.lng, in.lat))
      }

      val toList = JavaConverters.asScalaIteratorConverter(list.iterator()).asScala.toList
      val centerPoint: GeoCoordinate = GetCenterPointFromListOfCoordinates.getCenterPoint(toList)

      val rk = h3Code.toString
      (rk, begin_address_code, centerPoint.getLongitude + "," + centerPoint.getLatitude, count)


    }
    import sparkSession.sqlContext.implicits._
    val hotOrder = reultHot.toDF("rk" , "begin_address_code" , "centerPoint" , "count")

    //SparkSQLHBaseSink.saveToHBase(hotOrder,"hotOrder","rk","rk,begin_address_code,centerPoint,count")


  }



  //UDF   经纬度  --->h3编码
  private def locationToH3(lat:Double , lon:Double , res:Int):Long = {
    h3.geoToH3(lat , lon , res)
  }

  //h3 -->热区的那个点--->六边形
  private def h3To6(geoCode:Long): List[GeoCoord] ={
    val boundary: util.List[GeoCoord] = h3.h3ToGeoBoundary(geoCode)
    JavaConverters.asScalaIteratorConverter(boundary.iterator()).asScala.toList
  }

}
