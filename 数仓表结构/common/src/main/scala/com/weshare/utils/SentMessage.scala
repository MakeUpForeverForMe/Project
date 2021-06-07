package com.weshare.utils

import java.util.{ Properties}

import org.apache.commons.lang3.StringUtils

/**
 * created by chao.guo on 2021/4/19
 **/
case class  RobootRerson(id:Int,hookurl:String,isEnable:Int,user_phones:String)
object SentMessage {

  /**
   * 获取机器信息
   * @param pro
   * @throws
   * @return
   */

    @throws[Exception]
    def getAllRobootRerson(pro: Properties,sql:String) = {
      var robootRersons = List[RobootRerson]()
      var hdfs_config = pro.getProperty("cm_config")
      var hdfs_master = pro.getProperty("hdfs_master")
      val isHa = pro.getProperty("isHa").toInt
      if (null != System.getenv && null != System.getenv.get("OS") && System.getenv.get("OS").startsWith("Windows")) {
        hdfs_master = "hdfs://node5:8020"
        hdfs_config = "/user/admin/data_watch/test_properties.properties" //

      }
      DruidDataSourceUtils.initDataSource(hdfs_config, hdfs_master, "cm_mysql", isHa)
      val cm_mysql = DruidDataSourceUtils.getConection("cm_mysql")
      val resultSet = cm_mysql.createStatement.executeQuery(sql)
      while (resultSet.next) {
        robootRersons = RobootRerson(resultSet.getInt("id"), resultSet.getString("hookurl"), resultSet.getInt("isEnable"), resultSet.getString("user_phones")) :: robootRersons
      }
      robootRersons
    }

  /**
   * 初始化消息内容
   *
   * @param context
   * @param type
   * @return
   */
  def initMsgInfo(context: String, `type`: String, user_phones: String): String = {
    val buffer = new StringBuffer
    val phoneBuffer = new StringBuffer
    var warning = "@all"
    user_phones.split(",").foreach(it=>{
      phoneBuffer.append("\"").append(it).append("\"").append(",")
    })


    if (StringUtils.endsWith(phoneBuffer.toString, ","))   warning = phoneBuffer.toString.substring(0, phoneBuffer.toString.lastIndexOf(","))
    buffer.append("{").append("\n")
    `type` match {
      case "markdown" =>
        buffer.append("\"msgtype\":\"text\"").append(",").append("\n")
        buffer.append("\"text\":{").append("\n")
        buffer.append("\"content\":").append("\"").append(context).append("\"").append("\n")
      case "text" =>
        buffer.append("\"msgtype\":\"text\"").append(",").append("\n")
        buffer.append("\"text\":{").append("\n")
        buffer.append("\"content\":").append("\"").append(context).append("\"").append(",").append("\n")
        buffer.append("\"mentioned_list\":[" + warning + "]").append(",").append("\n")
        buffer.append("\"mentioned_mobile_list\":[" + warning + "]").append("\n")
    }
    buffer.append("}").append("\n").append("}")
    buffer.toString
  }

  @throws[Exception]
  def sendMessage(context: String, `type`: String, pro: Properties,sql:String): Unit = {
    val allRobootRerson = getAllRobootRerson(pro,sql)
    allRobootRerson.foreach((it: RobootRerson) => {
      def foo(it: RobootRerson) = {
        val msg = initMsgInfo(context, `type`, it.user_phones)
        System.out.println(msg)
        HttpUtils.sendPost(it.hookurl, msg)
      }
      foo(it)
    })
  }
}



