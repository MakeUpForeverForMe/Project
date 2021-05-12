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
    resultDataList



  }

  /**
   * 执行sql 返回对应的执行结果  公共字段----> sql执行完的结果
   *  sql执行完的结果 buffer.append(s"${filed_name}@${value}|")
   * 公共字段 comment_fields_value.append(s"${filed_name}@${value}|")
   * @param sql
   * @param dimFilelds
   * @param conn
   * @return
   */
  def executeSQL(sql:String,dimFilelds:String,conn:Connection):List[(String,Map[String,String])]={
    var resultDataList = List[(String, Map[String,String])]()
    val statement = conn.createStatement()
//    println(
//      s"""正在执行的SQL:\n
//         |${sql}""".stripMargin)

    val resultSet = statement.executeQuery(sql)
    var array=Array[String]()
    if(StringUtils.isNotEmpty(dimFilelds)){ // 公共字段名
      array= dimFilelds.split(",")
    }
    while (resultSet.next()){
      val data = resultSet.getMetaData
      val columnCount = data.getColumnCount
      val comment_fields_value = new StringBuffer()
      var data_map = Map[String, String]()
      for (index <- 1 to columnCount){
        val field_name = data.getColumnName(index)
        val field_type_name = data.getColumnTypeName(index)
       val field_value= field_type_name.toLowerCase match {
          case  "varchar" | "string"=>{
           if(null==resultSet.getString(field_name)){
             "无"
           } else{
             resultSet.getString(field_name)
           }
          }
          case "decimal" | "decimal*"=>{
            if(resultSet.getBigDecimal(field_name)!=null){
              resultSet.getBigDecimal(field_name).toString
            }else{
              "无"
            }

          }
          case "int"=>{
              resultSet.getInt(field_name).toString
          }
          case "long" | "bigint"=>{
              resultSet.getLong(field_name).toString
          }
          case _=>
           s"${field_type_name}not match"
        }
        data_map +=(field_name.toLowerCase->field_value)
        //data_buffer.append(s"${field_name.toLowerCase}@${field_value}|")
        if(array.length>0 && array.contains(field_name.toLowerCase)){
            comment_fields_value.append(s"${field_name.toLowerCase}@${field_value}|")
        }
    }
      resultDataList = (comment_fields_value.toString,data_map)::resultDataList
    }
    resultSet.close()
    statement.close()
    resultDataList
  }
}
