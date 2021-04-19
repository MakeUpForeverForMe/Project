package com.weshare.common

import java.io.{BufferedReader, InputStreamReader}
import java.net.URL

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}


/**
 * created by chao.guo on 2021/2/18
 * // 补全身份证信息码表的数据
 **/
object ObtainIdCard {
  case class IdCardInfo(idno_addr:String,idno_area:String,idno_area_cn:String,idno_province:String,idno_province_cn:String,idno_city:String,idno_city_cn:String,idno_county:String,idno_county_cn:String)
  private val log: Logger = LoggerFactory.getLogger("IdCard")

  def main(args: Array[String]): Unit = {
    val idno="340829199602114415"
    log.info("<--调用接口请求返回身份证信息-->")
    val url:URL=new URL(s"http://api.k780.com:88/?app=idcard.get&idcard=${idno}&appkey=10003&sign=b59bc3ef6191eb9f747dd4e83c99f2a4&format=json")
    val stream = url.openStream()
    val reader = new BufferedReader(new InputStreamReader(stream,"UTF-8"))
    val line = reader.readLine()
    log.info(line)
    val jsonObject = JSON.parseObject(line)

    //insert into table dim_new.dim_idno values('231029','2','东北地区','23','黑龙江省','2310','牡丹江市','231029','牡丹江市');
    val info = dealIdNoOtherMessage(jsonObject,idno)
    val sql = s"insert into table dim_new.dim_idno values('${info.idno_addr}','${info.idno_area}','${info.idno_area_cn}'," +
    s"'${info.idno_province}','${info.idno_province_cn}','${info.idno_city}','${info.idno_city_cn}','${info.idno_county}','${info.idno_county_cn.replaceAll(" ","")}');"
    println(sql)
    stream.close()
    reader.close()
  }


  /**
   * 处理身份证信息 返回元组 拼装sql 
   * @param jsonObject
   * @param idno
   * @return
   */
  def dealIdNoOtherMessage(jsonObject:JSONObject,idno:String)={
    val resultJson = jsonObject.getJSONObject("result")
    val idno_addr = resultJson.getString("par")
    val address_ = resultJson.getString("style_citynm").split(",")
    var adress_info = resultJson.getString("att")
    var array: Array[String] = Array[String]();
    for (elem <- 1 until  address_.length) { // 中华人民共和国,河南省,驻马店市
      val str = address_(elem)
      adress_info=adress_info.replace(str,"")
      array= array.:+(str)
    }
    /*println(adress_info)
    println(array.toList)*/
    val provice = idno.substring(0,2)
    val city = idno.substring(0,4)
    var provice_cn=""
    var city_cn=""
    array.toList.length match {
      case 1 => {
        provice_cn=array.toList.head
        city_cn=array.toList.head
      }
      case 2=>{
        provice_cn=array.toList.head
        city_cn=array.toList.last
      }
    }
    if(StringUtils.isBlank(adress_info)){
      if(StringUtils.isEmpty(city_cn)){
        adress_info=provice_cn
      }else{
        adress_info=city_cn
      }
    }
    val area_tump = dealIdno_area(idno)
    IdCardInfo(idno_addr,
      area_tump._1,
      area_tump._2,
      provice, provice_cn, city, city_cn, idno_addr, adress_info)
}





  /**
   * 获取身份证归属地所属地区
   * @param idNo
   * @return
   */
  def dealIdno_area(idNo:String)={
    val idno_area_cn= idNo.head.toString match {
      case "1" =>"华北地区"
      case "2" =>"东北地区"
      case "5" =>"西南地区"
      case "4" =>"中南地区"
      case "3" => "华东地区"
      case "6" => "西北地区"
      case _=>"未知"
    }
    (idNo.head.toString,idno_area_cn)
  }

}
