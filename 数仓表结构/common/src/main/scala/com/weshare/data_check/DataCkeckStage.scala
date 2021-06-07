package com.weshare.data_check

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.logging.Logger
import java.util.{Calendar, Date, Properties}

import com.weshare.utils.{DruidDataSourceUtils, JDBCUtils, PropertiesUtils, SendEmailUtil, SentMessage}
import org.apache.commons.lang3.StringUtils
import org.slf4j
import org.slf4j.LoggerFactory

/**
 * created by chao.guo on 2021/4/19
 **/
object DataCkeckStage {
  private val logger: slf4j.Logger = LoggerFactory.getLogger(DataCkeckStage.getClass)

  // 初始化 impala 数据源
  // 初始化mysql 连接
  // 校验数据 将校验结果写入MySQl

  //tablename mysql    stage

  case class stage_data_check(var tableName:String,var p_type:String,var d_date:String,var num:Int)
  def main(args: Array[String]): Unit = {
    //


    val mode_type=args(0)
    val properties = PropertiesUtils.propertiesLoad(mode_type)
    println(mode_type)
    println(properties)
    initCompent(properties)
    logger.info(s"${properties}")
    val listTableName = getTableName
    val s_d_date=args(1)
    val e_d_date=args(2)
    val data_check_table_ods = properties.getProperty("stage_data_ckeck_ods")
    val data_check_table_stage = properties.getProperty("stage_data_ckeck_stage")
    val cm_mysql = DruidDataSourceUtils.getConection("cm_mysql")
    val statement = cm_mysql.createStatement()
    statement.execute(s"delete from ${data_check_table_ods} where d_date between '${s_d_date}' and '${e_d_date}'")
    statement.execute(s"delete from ${data_check_table_stage} where d_date between '${s_d_date}' and '${e_d_date}'")
    statement.close()
    val update_sql_ods=s"insert into  ${data_check_table_ods} (table_name,p_type,d_date,num) values(?,?,?,?)"
    val update_sql_stage=s"insert into  ${data_check_table_stage} (table_name,p_type,d_date,num) values(?,?,?,?)"
    val pre_old_statement_ods = cm_mysql.prepareStatement(update_sql_ods)
    val pre_statement_stage = cm_mysql.prepareStatement(update_sql_stage)
    val old_impala = DruidDataSourceUtils.getConection("old_impala")
    val emr_impala = DruidDataSourceUtils.getConection("EMR_impala")
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val s_d_date_clande =format.parse(s_d_date)
    val calendar = Calendar.getInstance()
    calendar.setTime(s_d_date_clande)
    val e_d_date_clande =format.parse(e_d_date)
    while (calendar.getTime.getTime.compareTo(e_d_date_clande.getTime)<=0){
      val d_date=format.format(calendar.getTime)
      listTableName.foreach(it=>{
        logger.error(s"ods 表名:${it}:${d_date}开始")
        dealSinkToMysql(it,"ods",old_impala,d_date,d_date,pre_old_statement_ods)
        dealSinkToMysql(it,"stage",emr_impala,d_date,d_date,pre_statement_stage)
        logger.error(s"ods 表名:${it}:${d_date}结束")
      })
      calendar.add(Calendar.DAY_OF_YEAR,1)
    }



    /**
     * 发送邮件信息
     */
    dealResultAndSendMessage(data_check_table_ods,data_check_table_stage,s_d_date,e_d_date,cm_mysql,properties)
    pre_old_statement_ods.close()
    pre_statement_stage.close()
    cm_mysql.close()
    DruidDataSourceUtils.close()
  }


val dealResultAndSendMessage=(data_check_table_ods:String,data_check_table_stage:String,s_d_date:String,e_d_date:String,cm_mysql:Connection,properties:Properties)=>{
  val hql =
    s"""
       |select
       |if(a.table_name is null ,b.table_name,a.table_name) as table_name,
       |if(a.p_type is null ,b.p_type,a.p_type) as p_type,
       |if(a.d_date is null ,b.d_date,a.d_date) as d_date,
       |if(a.num is null,0,a.num) as ods_num,
       |if(b.num is null ,0,b.num) as stage_num,
       |if(a.num is null,0,a.num)-if(b.num is null,0,b.num) as diff_num
       |from
       |(
       |select * from ${data_check_table_ods} where d_date >='${s_d_date}' and d_date<='${e_d_date}'
       |)a
       |left join
       |(
       |select * from ${data_check_table_stage} where d_date >='${s_d_date}' and d_date<='${e_d_date}'
       |)b on a.table_name=b.table_name and a.p_type=b.p_type and a.d_date=b.d_date
       |where abs(if(a.num is null,0,a.num)-if(b.num is null,0,b.num))>0
       |union all
       |select
       |if(a.table_name is null ,b.table_name,a.table_name) as table_name,
       |if(a.p_type is null ,b.p_type,a.p_type) as p_type,
       |if(a.d_date is null ,b.d_date,a.d_date) as d_date,
       |if(a.num is null,0,a.num) as ods_num,
       |if(b.num is null ,0,b.num) as stage_num,
       |if(a.num is null,0,a.num)-if(b.num is null,0,b.num) as diff_num
       |from
       |(
       |select * from ${data_check_table_ods} where d_date >='${s_d_date}' and d_date<='${e_d_date}'
       |)a
       |right join
       |(
       |select * from ${data_check_table_stage} where d_date >='${s_d_date}' and d_date<='${e_d_date}'
       |)b on a.table_name=b.table_name and a.p_type=b.p_type and a.d_date=b.d_date
       |where abs(if(a.num is null,0,a.num)-if(b.num is null,0,b.num))>0
       |order by table_name, d_date,p_type
       |""".stripMargin
  logger.error("{}",hql)

  val result: List[Map[String, String]]= JDBCUtils.executeSQL(hql,cm_mysql).distinct
  var html = initHtmlStr(result)
  if(result.nonEmpty){
    //初始化
    SentMessage.sendMessage(init_Mesage(result),"text",properties,"select * from flink_config.robot_person_info where isEnable=1 and id=2")
  }else{
    html=s"ods 和 stage 层数据校验通过,时间范围:${s_d_date}到${e_d_date}"
  }
  SendEmailUtil.sendMessage(html,"ods与stage层数据校验")

}


  val init_Mesage=(result: List[Map[String, String]])=>{
    val buffer = new StringBuffer("ods和EMR stage 层数据部分对比结果，请相关同事注意！").append("\n")
                  .append("> table_name,p_type,d_date,ods_num,stage_num,diff_num").append("\n")
    result.take(10).foreach(it=>{
      buffer.append(s""">${it.getOrElse("table_name","")},${it.getOrElse("p_type","")},${it.getOrElse("d_date","")},${it.getOrElse("ods_num","")},${it.getOrElse("stage_num","")},${it.getOrElse("diff_num","")}""").append("\n")
    })
    buffer.toString
  }


  /**
   * 查询指定数据库的表统计信息写入mysql
   */
  val dealSinkToMysql=(table_name:String,dbName:String,implaConnection:Connection,s_d_date:String,e_d_date:String,pre_statement:PreparedStatement)=>{
    val impala_query = implaConnection.createStatement()
    var whereCondin=" and 1=1 "
    if(table_name.contains("ecas_order")){
      whereCondin=" And order_status='S'"
    }
    impala_query.execute(s"refresh ${dbName}.${table_name}")
    val list:List[Map[String,String]]= JDBCUtils.executeSQL(s"""
                            |select
                            |"${dbName}.${table_name}" as table_name,
                            |p_type,
                            |d_date,
                            |count(due_bill_no) as num
                            |from ${dbName}.${table_name}
                            |where d_date between '${s_d_date}' and '${e_d_date}'
                            |${whereCondin}
                            |group by p_type,d_date
                            |order by p_type,d_date
                            |""".stripMargin,
      implaConnection)
    val result_list = list.map(it => {
      stage_data_check(
        table_name,
        it.getOrElse("p_type", ""),
        it.getOrElse("d_date", ""),
        it.getOrElse("num", "0").toInt
      )
    })
    sinkMysql(pre_statement,result_list)




  }








  val initHtmlStr=(result:List[Map[String, String]])=>{
  val buffer = new StringBuffer(s"${html_head}").append("<table class='gridtable'>")
    result.foreach(it=>{
      buffer.append( s"""
        |<tr>
        |<td>${it.getOrElse("table_name","")}</td>
        |<td>${it.getOrElse("p_type","")}</td>
        |<td>${it.getOrElse("d_date","")}</td>
        |<td>ods:${it.getOrElse("ods_num","0")}</td>
        |<td>stage:${it.getOrElse("stage_num","0")}</td>
        |<td>相差:${it.getOrElse("diff_num","0")}</td>
        |</tr>
        |
        |""".stripMargin
      )
    })
    buffer.append("</table>").append("</html>").toString
  }




  /**
   * 写出数据
   */
  val sinkMysql=(pre_statement: PreparedStatement,result_list:List[stage_data_check])=>{
  result_list.foreach(domain=>{
    pre_statement.setString(1,domain.tableName)
    pre_statement.setString(2,domain.p_type)
    pre_statement.setString(3,domain.d_date)
    pre_statement.setInt(4,domain.num)
    pre_statement.addBatch()
  })
    pre_statement.executeBatch()


}














  /**
   * 数据对照
    * @param
   */

val initCompent =(pro: Properties)=>{
  val hdfs_master_path = pro.getProperty("hdfs_master")
  val cm_config_path = pro.getProperty("cm_config")
  val isHa = Integer.parseInt(pro.getProperty("isHa"))
  val email_config_path = pro.getProperty("email_config")
  val old_impala_config = pro.getProperty("old_impala_config")
  val emr_impala_config = pro.getProperty("emr_impala_config")
  val hdfs_hive_jdbc_config_path = pro.getProperty("hdfs_hive_jdbc_config_path")
  DruidDataSourceUtils.initDataSource(cm_config_path,hdfs_master_path,"cm_mysql",isHa)
  DruidDataSourceUtils.initDataSource(hdfs_hive_jdbc_config_path,hdfs_master_path,"hive",isHa)
  DruidDataSourceUtils.initDataSource(old_impala_config,hdfs_master_path,"old_impala",isHa)
  DruidDataSourceUtils.initDataSource(emr_impala_config,hdfs_master_path,"EMR_impala",isHa)
  SendEmailUtil.initEmailSession(email_config_path,hdfs_master_path,isHa)
}

def getTableName(): List[String] ={
  List[String]("ecas_loan","ecas_order","ecas_repay_hst","ecas_repay_schedule","ecas_order_hst",
    "ecas_loan_asset","ecas_order_asset","ecas_repay_hst_asset","ecas_repay_schedule_asset")
}

  val html_head ="""
  <!DOCTYPE html>
    <html lang='en'>
      <head>
        <meta charset='UTF-8'>
          <meta name='viewport' content='width=device-width, initial-scale=1.0'>
            <meta http-equiv='X-UA-Compatible' content='ie=edge'>
              <title>Document</title>
            </head>

            <body>
              <style type='text/css'>
                body {
                margin: 20px;
                font-family: Verdana, Geneva, Arial, Helvetica, sans-serif;
  font-size: 12px;
  }

  .style1 {
  width: 800px;
  height: 140px;
  margin: 0px;
  margin-bottom: 20px;
  border: 1px solid #96C2F1;
  word-wrap:break-word;
  display: none
  }
  .h6-button {
  margin-bottom: 10px;
  cursor: pointer;
  }
  .btnDisplay {
  width: 800px;
  height: 140px;
  margin: 0px;
  margin-bottom: 20px;
  border: 1px solid #96C2F1;
  word-wrap:break-word;
  display: block
  }

  table.gridtable {
  font-family: verdana, arial, sans-serif;
  font-size: 11px;
  color: #333333;
  border-width: 1px;
  border-color: #666666;
  border-collapse: collapse;
  }

  table.gridtable th {
  border-width: 1px;
  padding: 8px;
  border-style: solid;
  border-color: #666666;
  background-color: #dedede;
  }

  table.gridtable td {
  border-width: 1px;
  padding: 8px;
  border-style: solid;
  border-color: #666666;
  background-color: #ffffff;
  }
  </style>
"""
}

