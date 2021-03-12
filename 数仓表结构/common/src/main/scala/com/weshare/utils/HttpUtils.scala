package com.weshare.utils

/**
 * created by chao.guo on 2021/2/22
 **/
import java.io.IOException
import java.util
import java.util.UUID

import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.http.{Consts, NameValuePair}
import org.apache.spark.sql.SparkSession

object HttpUtils {
  val userAgent="Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
  val httpclient:CloseableHttpClient=HttpClients.createDefault();

  def sendPost(url: String, map: Map[String,String]): String = { // 设置参数
    val formparams = new util.ArrayList[NameValuePair]
    for (entry <- map) {
      formparams.add(new BasicNameValuePair(entry._1, entry._2))
    }
    // 编码
    val formEntity = new UrlEncodedFormEntity(formparams, Consts.UTF_8)
    // 取得HttpPost对象
    val httpPost = new HttpPost(url)
    // 防止被当成攻击添加的
    httpPost.setHeader("User-Agent", userAgent)
    httpPost.setHeader("Content-Type", "application/json")
    // 参数放入Entity
    httpPost.setEntity(formEntity)
    var response : CloseableHttpResponse= null
    var result :String= null
    try { // 执行post请求
      response = httpclient.execute(httpPost)
      // 得到entity
      val entity = response.getEntity
      // 得到字符串
      result = EntityUtils.toString(entity)
      println(result)
    } catch {
      case e: IOException =>
    } finally if (response != null) try response.close
    catch {
      case e: IOException =>
    }
    result
  }
  def sendPost(url: String, str:String): String = { // 设置参数
    // 编码
    val entity: StringEntity = new StringEntity(str, Consts.UTF_8)
    // 取得HttpPost对象
    val httpPost = new HttpPost(url)
    // 防止被当成攻击添加的
    httpPost.setHeader("User-Agent", userAgent)
    httpPost.setHeader("Content-Type", "application/json")
    // 参数放入Entity
    httpPost.setEntity(entity)
    var response : CloseableHttpResponse= null
    var result :String= null
    try { // 执行post请求
      response = httpclient.execute(httpPost)
      // 得到entity
      val entity = response.getEntity
      // 得到字符串
      result = EntityUtils.toString(entity)
      println(result)
    } catch {
      case e: IOException =>
    } finally if (response != null) try response.close
    catch {
      case e: IOException =>
    }
    result
  }
}
