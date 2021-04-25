package com.weshare.pmml.dao

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.weshare.pmml.domain.PmmlParam
import com.weshare.utils.DruidDataSourceUtils


/**
  * Created by mouzwang on 2021-03-03 11:33
  */
object PmmlDaoImpl {

  def initializeDataSource(pro:Properties): Unit = {
    val pro_hdfs_config=pro.getProperty("cm_config")
    //val pro_hdfs_config = "/user/admin/data_watch/cm_properties.properties"
    var hdfs_master =pro.getProperty("hdfs_master")
    //var hdfs_master = "hdfs://node233:8020"
    if ((null != System.getenv && null != System.getenv.get("OS") && System.getenv.get("OS").startsWith
    ("Windows")) || (System.getProperty("os.name") == "Mac OS X")) {
      hdfs_master = "hdfs://node5:8020"
    }
    val isHa=pro.getProperty("isHa").toInt
    DruidDataSourceUtils.initDataSource(pro_hdfs_config, hdfs_master, "cm_mysql", isHa)
  }

  def getPoolTotalPrincipal(project_id:String) : BigDecimal = {
    val connection = DruidDataSourceUtils.getConection("cm_mysql")
    val statement = connection.createStatement()
    val sql =
      s"""
        | select available_amount from data_check.data_asset_pool where project_id='${project_id}'
      """.stripMargin
    val resultSet = statement.executeQuery(sql)
    resultSet.next()
    val available_amount = resultSet.getBigDecimal("available_amount")
    connection.close()
    available_amount
  }

  def updatePoolTotalPrincipal(available_amount : BigDecimal,project_id:String) = {
    val connection = DruidDataSourceUtils.getConection("cm_mysql")
    val statement = connection.createStatement()
    val currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    val sql =
      s"""
        | update data_check.data_asset_pool
        | set available_amount = "${available_amount}",update_time = "${currentTime}" where project_id='${project_id}';
      """.stripMargin
    val result = statement.executeUpdate(sql)
    println("update result : " + result)
    connection.close()
  }

  /**
   * 获取参数
   * @param project_ids
   */
  def getPmmlParamMode(project_ids:String) ={
    val connection = DruidDataSourceUtils.getConection("cm_mysql")
    val statement = connection.createStatement()
    val sql=
      s"""
        |select
        |project_name,
        |project_id,
        |project_start_date,
        |project_end_date,
        |init_total_amount,
        |cycle_start_date,
        |cycle_end_date,
        |amortization_period_start_date,
        |amortization_period_end_date,
        |loan_terms,
        |available_amount,
        |pmml_url,
        |mode_rate
        |from
        |data_check.data_asset_pool
        |where project_id in ('${project_ids}')
        |""".stripMargin
    statement.executeQuery(sql)
    val resultSet = statement.getResultSet
    var param: PmmlParam = null;
    if(resultSet.next()){
       param = PmmlParam(
        "",
        "",
        "0",
        resultSet.getString("project_id"),
        1,
        "",
        resultSet.getString("project_start_date"),
        resultSet.getString("project_end_date"),
        resultSet.getBigDecimal("init_total_amount"),
        resultSet.getString("cycle_start_date"),
        resultSet.getString("cycle_end_date"),
        resultSet.getString("amortization_period_start_date"),
        resultSet.getString("amortization_period_end_date"),
        resultSet.getString("loan_terms"),
        resultSet.getBigDecimal("available_amount"),
        resultSet.getString("pmml_url"),
         deal_mode_type(resultSet.getString("mode_rate"))
      )


    }
    param
  }

  val deal_mode_type=(mode_rate:String)=>{
    mode_rate.split(",").map(it=>(it.split("=")(0),it.split("=")(1)))
      .toMap

  }

}
