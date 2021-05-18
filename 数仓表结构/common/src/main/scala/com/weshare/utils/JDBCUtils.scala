package com.weshare.utils

import java.sql.Connection

import org.apache.commons.lang3.StringUtils

/**
 * created by chao.guo on 2020/10/20
 **/
object JDBCUtils {

  /**
   * List[Map[String,String]] 返回sql执行完的结果 一行数据一个map  map key 为字段名  value为值
   * @param sql sql语句
   * @param conn
   * @return
   */
  def executeSQL(sql:String,conn:Connection):List[Map[String,String]]= {
    executeSQL(sql,conn,"")
  }

  def executeSQL(sql:String,conn:Connection,db_name:String):List[Map[String,String]]= {
    var resultDataList = List[Map[String,String]]()
    val statement = conn.createStatement()
    if(!StringUtils.isEmpty(db_name)){
      statement.execute(s"use ${db_name}")
    }
    val resultSet = statement.executeQuery(sql)
    while (resultSet.next()) {
      val data = resultSet.getMetaData // 获取sql 执行完的结果元数据信息
      val columnCount = data.getColumnCount
      var data_map = Map[String, String]()
      for (index <- 1 to columnCount) {
        val field_name = data.getColumnName(index)
        val field_type_name = data.getColumnTypeName(index)
        val field_value = field_type_name.toLowerCase match {
          case "varchar" | "string" => {
            resultSet.getString(field_name)
          }
          case "decimal" | "decimal*" => {
            resultSet.getBigDecimal(field_name).toString
          }
          case "int" => {
            resultSet.getInt(field_name).toString
          }
          case "long" | "bigint" => {
            resultSet.getLong(field_name).toString
          }
          case _ =>
            s"${field_type_name}not match"
        }
        data_map += (field_name -> field_value)
      }
      resultDataList=data_map::resultDataList
    }
    statement.close()
    resultDataList



  }

}
