package com.weshare.utils

/**
 * created by chao.guo on 2020/8/28
 **/

import java.util.Properties

import com.alibaba.druid.pool.{DruidDataSource, DruidDataSourceFactory, DruidPooledConnection}
import org.apache.hadoop.fs.{FileSystem, Path}

object DruidDataSourceUtils {
  var test_dataSource:DruidDataSource = null // cm的 数据库 存储的是 跑批任务信息
  var hive_dataSource:DruidDataSource = null
  var pro_dataSource :DruidDataSource= null
  var old_imapla_dataSource :DruidDataSource= null
  var emr_imapla_dataSource :DruidDataSource= null

  @throws[Exception]
  def initDataSource(hdfs_filePath: String, hdfs_maser: String, `type`: String, isHa: Int): Unit = {
    val conf = HdfsUtils.initConfiguration(hdfs_maser, isHa)
    val in = FileSystem.get(conf).open(new Path(hdfs_filePath))
    val properties = new Properties
    properties.load(in)
    `type` match {
      case "cm_mysql" =>
        if (null == test_dataSource) test_dataSource = DruidDataSourceFactory.createDataSource(properties).asInstanceOf[DruidDataSource]
      case "pro_mysql" =>
        if (null == pro_dataSource) pro_dataSource = DruidDataSourceFactory.createDataSource(properties).asInstanceOf[DruidDataSource]
      case "impala" | "old_impala" =>
        if (null == old_imapla_dataSource) old_imapla_dataSource = DruidDataSourceFactory.createDataSource(properties).asInstanceOf[DruidDataSource]
      case "EMR_impala" =>
        if (null == emr_imapla_dataSource) emr_imapla_dataSource = DruidDataSourceFactory.createDataSource(properties).asInstanceOf[DruidDataSource]

      case "hive"=>
        if (null == hive_dataSource) hive_dataSource = DruidDataSourceFactory.createDataSource(properties).asInstanceOf[DruidDataSource]
    }
    in.close()
  }

  def getConection(`type`: String) = {
    var connection:DruidPooledConnection= null
    `type` match {
      case "cm_mysql" =>
        connection = test_dataSource.getConnection
      case "pro_mysql" =>
        connection = pro_dataSource.getConnection
      case "impala" | "old_impala" =>
        connection = old_imapla_dataSource.getConnection
      case "EMR_impala" =>
        connection = emr_imapla_dataSource.getConnection
      case "hive"=>
        connection =hive_dataSource.getConnection
    }
    connection
  }

  def close(): Unit = {
    if (pro_dataSource != null && pro_dataSource.isKeepAlive) pro_dataSource.close()
    if (test_dataSource != null && test_dataSource.isKeepAlive) test_dataSource.close()
    if (old_imapla_dataSource != null && old_imapla_dataSource.isKeepAlive) old_imapla_dataSource.close()
    if (emr_imapla_dataSource != null && emr_imapla_dataSource.isKeepAlive) emr_imapla_dataSource.close()
    if (hive_dataSource != null && hive_dataSource.isKeepAlive) hive_dataSource.close()
  }

}
