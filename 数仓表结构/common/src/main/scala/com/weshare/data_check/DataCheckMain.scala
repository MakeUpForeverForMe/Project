package com.weshare.data_check

import java.util.Properties

import com.weshare.data_check.DataCkeckStage.initCompent
import com.weshare.data_check.mode.Mode.DataWatchRule
import com.weshare.data_check.serviceImpl.DataCheckMysqlService
import com.weshare.utils.{DateUtils, DruidDataSourceUtils, JDBCUtils, PropertiesUtils, SendEmailUtil}
import org.apache.commons.lang3.StringUtils

/**
 * created by chao.guo on 2021/5/10
 **/
object DataCheckMain {

  def main(args: Array[String]): Unit = {

    val mode_type=args(0)
    val mode_name=args(1)
    val properties = PropertiesUtils.propertiesLoad(mode_type)
    println(mode_type)
    println(properties)
    initCompent(properties)
    val batch_date=args(2)
    val last_batch_date=DateUtils.clcalLastBatchDate(batch_date)
    //val ql_list: List[DataWatchRule] = get_Check_SQl_list(batch_date,last_batch_date)
    mode_name match {
      case "stage_sqoop_lx"|"stage_sqoop_dd"=>DataCheckMysqlService.data_check_mysql(properties,mode_name,batch_date)
    }
    DruidDataSourceUtils.close()
  }


  /**
   * 初始化组件信息
   */

  val initCompent =(pro: Properties)=>{
    val hdfs_master_path = pro.getProperty("hdfs_master")
    val cm_config_path = pro.getProperty("cm_config")
    val isHa = Integer.parseInt(pro.getProperty("isHa"))
    val email_config_path = pro.getProperty("email_config")
    val old_impala_config = pro.getProperty("old_impala_config")
    val emr_impala_config = pro.getProperty("emr_impala_config")
    val pro_cm = pro.getProperty("pro_mysql_config")
    val hdfs_hive_jdbc_config_path = pro.getProperty("hdfs_hive_jdbc_config_path")
    DruidDataSourceUtils.initDataSource(pro_cm,hdfs_master_path,"pro_mysql",isHa)
    DruidDataSourceUtils.initDataSource(cm_config_path,hdfs_master_path,"cm_mysql",isHa)
    DruidDataSourceUtils.initDataSource(hdfs_hive_jdbc_config_path,hdfs_master_path,"hive",isHa)
    DruidDataSourceUtils.initDataSource(old_impala_config,hdfs_master_path,"old_impala",isHa)
    DruidDataSourceUtils.initDataSource(emr_impala_config,hdfs_master_path,"EMR_impala",isHa)
    SendEmailUtil.initEmailSession(email_config_path,hdfs_master_path,isHa)
  }

  /**
   * 查询校验规则
   */
  val get_Check_SQl_list: (String,String) => List[DataWatchRule] = (batch_date:String,last_batch_date: String)=>{
    val test_cm = DruidDataSourceUtils.test_dataSource.getConnection
    JDBCUtils.executeSQL(
      s"""
        |select
        |task_name,
        |mode_name,
        |param_list,
        |sql1,
        |sql2,
        |engine_type,
        |recivers_email,
        |reboot_ids
        |from
        |flink_config.stage_check_rule
        |
        |""".stripMargin,test_cm).map(it=>{
      DataWatchRule(
        it.getOrElse("task_name",""),
        it.getOrElse("mode_name",""),
        it.getOrElse("param_list",""),
        dealSqlText(it.getOrElse("sql1",""),it.getOrElse("param_list",""),batch_date,last_batch_date),
        dealSqlText(it.getOrElse("sql2",""),it.getOrElse("param_list",""),batch_date,last_batch_date),
        it.getOrElse("engine_type",""),
        it.getOrElse("recivers_email",""),
        it.getOrElse("reboot_ids","").split(",").map("'"+_+"'").mkString(",")
      )
    })

  }


  def dealSqlText(sql:String, task_param:String,  batch_date: String, last_batch_date: String): String = {
    var temp_sql = sql
    if (StringUtils.isNoneBlank(task_param)) {
      val params = task_param.split("#")
      var param = Map[String, String]()
      params.foreach(it => {
        if (it.split("=").length > 1) {
          param += (it.split("=")(0).replaceAll("\\{", "").replaceAll("}", "") -> it.split("=")(1))
        } else {
          param += (it.split("=")(0).replaceAll("\\{", "").replaceAll("}", "") -> "")
        }
      })
      param.keySet.foreach(
        item => {
          temp_sql = temp_sql.replaceAll(item, param.getOrElse(item, ""))
        }
      )
    }
   temp_sql.replaceAll("@date", s"'${batch_date}'").replaceAll("@last_date", s"'${last_batch_date}'")
  }


}
