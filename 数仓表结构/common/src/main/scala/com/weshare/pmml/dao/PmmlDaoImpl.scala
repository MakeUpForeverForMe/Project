package com.weshare.pmml.dao

import java.text.SimpleDateFormat
import java.util.Date

import com.weshare.utils.DruidDataSourceUtils


/**
  * Created by mouzwang on 2021-03-03 11:33
  */
object PmmlDaoImpl {

  def initializeDataSource(): Unit = {
    val pro_hdfs_config = "/user/admin/data_watch/cm_properties.properties"
    var hdfs_master = "hdfs://node233:8020"
    if ((null != System.getenv && null != System.getenv.get("OS") && System.getenv.get("OS").startsWith
    ("Windows")) || (System.getProperty("os.name") == "Mac OS X")) {
      hdfs_master = "hdfs://node5:8020"
    }
    DruidDataSourceUtils.initDataSource(pro_hdfs_config, hdfs_master, "cm_mysql", 0)
  }

  def getPoolTotalPrincipal() : BigDecimal = {
    val connection = DruidDataSourceUtils.getConection("cm_mysql")
    val statement = connection.createStatement()
    val sql =
      """
        | select pool_total_principal from data_check.data_asset_pool
      """.stripMargin
    val resultSet = statement.executeQuery(sql)
    resultSet.next()
    val poolTotalPrincipal = resultSet.getBigDecimal("pool_total_principal")
    connection.close()
    poolTotalPrincipal
  }

  def updatePoolTotalPrincipal(poolTotalPrincipal : BigDecimal) = {
    val connection = DruidDataSourceUtils.getConection("cm_mysql")
    val statement = connection.createStatement()
    val currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    val sql =
      s"""
        | update data_check.data_asset_pool
        | set pool_total_principal = "${poolTotalPrincipal}",update_time = "${currentTime}";
      """.stripMargin
    val result = statement.executeUpdate(sql)
    println("update result : " + result)
    connection.close()
  }
}
