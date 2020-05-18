package com.travel.utils

import java.util

import com.alibaba.fastjson.JSONArray
import com.travel.common.{District, MapUtil}
import com.uber.h3core.H3Core
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.locationtech.jts.geom.Polygon
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable

object SparkUtils {

  //1.创建h3实例
  val h3 = H3Core.newInstance

   def getVirtualFrame(sparkSession: SparkSession): RDD[Row] = {
    sparkSession.udf.register("locationToH3", new UDF3[String, String, Int, Long]() {
      override def call(lat: String, lng: String, result: Int): Long = {
        h3.geoToH3(lat.toDouble, lng.toDouble, result)
      }
    }, DataTypes.LongType)

    val order_sql =
      s"""
        | select
        |         order_id,
        |         city_id,
        |         starting_lng,
        |         starting_lat,
        |         locationToH3(starting_lat,starting_lng,12) as  h3code
        |          from order_df
      """.stripMargin
    //在sql语句中使用h3接口进行六边形栅格化
    val gridDf = sparkSession.sql(order_sql)

    gridDf.createOrReplaceTempView("order_grid")

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
    //上面的sql语句，将每个经纬度转换成为了一个HashCode码值，然后对hashCode码值分组，获取每个组里面经纬度最小的那一个，得到这个经纬度，然后再计算，这个经纬度坐落在哪一个区里面
    val virtual_frame: DataFrame = sparkSession.sql(sql)
    //判断每个虚拟车站一个经纬度，这个经纬度坐落在哪一个区里面，就能知道每个区里面有多少个虚拟车站了
    val virtual_rdd: RDD[Row] = virtual_frame.rdd
    virtual_rdd
  }


  def broadCastDistrictValue(sparkSession: SparkSession): Broadcast[util.ArrayList[District]] = {
    //获取每个区域的边界，每个区域画一个圈，判断经纬度是否在这个圈内
    val districtList = new java.util.ArrayList[District]();
    //获取海口市下面每个行政区的边界
    val districts: JSONArray = MapUtil.getDistricts("海口市", null);
    MapUtil.parseDistrictInfo(districts, null, districtList);

    //行政区域广播变量（spark开发优化的一个点）
    val districtsBroadcastVar = sparkSession.sparkContext.broadcast(districtList)
    districtsBroadcastVar
  }



  def changeDistictToPolygon(districtsBroadcastVar: Broadcast[util.ArrayList[District]],reader:WKTReader):mutable.Buffer[(District,Polygon)] = {
    //获取广播变量
    val districtList = districtsBroadcastVar.value
    //将java集合转换成为scala集合，或者将scala集合转换成为java集合
    import scala.collection.JavaConversions._

    val districtAndPology: mutable.Buffer[(District, Polygon)] = districtList.map(district => {
      val polygonStr = district.getPolygon
      var wktPolygon = ""
      if (!StringUtils.isEmpty(polygonStr)) {
        wktPolygon = "POLYGON((" + polygonStr.replaceAll(",", " ").replaceAll(";", ",") + "))"
        val polygon: Polygon = reader.read(wktPolygon).asInstanceOf[Polygon]
        (district, polygon)
      } else {
        null
      }
    })
    districtAndPology
  }




  def convertScanToString(scan: Scan):String = {
    val proto = ProtobufUtil.toScan(scan)  //Protobuf 是一个专门做序列化的框架
    Base64.encodeBytes(proto.toByteArray)
  }


}
