package com.weshare.data_check.Handler

import java.sql.Connection

import com.weshare.data_check.mode.Mode.RebootPerson
import com.weshare.utils.{HttpUtils, JDBCUtils}

/**
 * created by chao.guo on 2021/2/22
 *
 * 处理每日播报的内容
 *
 **/
object MesHandler {


  /**
   * 初始化消息体
   * @param batch_Date
   * @param result_data
   * @param th_arrays
   * @return
   */
  private def  initMsgTmp(batch_Date:String,result_data:List[Map[String, String]],th_arrays:Array[String]): String ={
  val tr_str = new  StringBuffer
  tr_str.append(s"${batch_Date},新增数据汇总反馈,请相关同事注意。\n")

  result_data.foreach(map=>{
    val head = th_arrays.head
    val project_name = map.getOrElse(head,"")
    tr_str.append(s"${project_name}新增数据如下:\n")
    val strings: Array[String] = th_arrays.drop(0)
    strings.foreach(
      field_name=>{
        tr_str.append(
          s"""
             |>${field_name}:  ${map.getOrElse(field_name,"0")} \n
             |""".stripMargin)
      }
    )
  })

  s"""
    |{
    |    "msgtype": "markdown",
    |    "markdown": {
    |        "content": "${tr_str}",
    |        "mentioned_list":["@all"],
    |        "mentioned_mobile_list":["@all"]
    |    }
    |}
    |
    |
    |""".stripMargin
}

  /**
   * 获取所有的 机器人信息
   * @param connection
   * @param
   */
  private def getAllRobotList( connection:Connection,sql:String) ={
    val resultMap: List[Map[String, String]] = JDBCUtils.executeSQL(sql,connection)
    val persons = resultMap.map(it => {
      RebootPerson(it.getOrElse("id", "").toInt, it.getOrElse("hookurl", ""), it.getOrElse("isEnable", "").toInt)
    })
    persons
}

  /**
   * 发送消息给机器人
   */
  def sendMessageToRebootPerson(batch_Date:String,result_data:List[Map[String, String]],th_arrays:Array[String],connection:Connection,sql:String): Unit ={
    //获取所有的机器人
    val persons = getAllRobotList(connection,sql)
    // 消息模版
    val message = initMsgTmp(batch_Date,result_data,th_arrays)
    persons.foreach(it=>{
      HttpUtils.sendPost(it.hookurl,message)
    })
  }
}
