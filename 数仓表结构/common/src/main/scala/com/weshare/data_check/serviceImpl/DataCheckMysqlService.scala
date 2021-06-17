package com.weshare.data_check.serviceImpl

import java.sql.{Connection, Statement}
import java.util.Properties
import java.util.concurrent.{Callable, Executors, Future}

import com.weshare.data_check.Handler.{HtmlPageHandler, MesHandler}
import com.weshare.utils.{DruidDataSourceUtils, JDBCUtils, SendEmailUtil}
import org.apache.commons.lang3.StringUtils

import scala.collection.immutable.TreeMap
import scala.collection.mutable

/**
 * created by chao.guo on 2021/5/10
 *
 * hive 中抽取的数据和mysql 数据 做对比
 *
 *
 **/
object DataCheckMysqlService {
  /**
   * stage 层数据校验
   * flink_config.stage_check_mysql
   */
  private val field_map: Map[String, String] = Map[String, String](
    "loan_num" -> "每日放款笔数汇总",
    "loan_prin_sum" -> "每日放款本金汇总",
    "over_due_num" -> "每日新增逾期笔数汇总",
    "repay_hst_repay_count" -> "每日实还表还款笔数汇总",
    "repay_hst_repay_prin" -> "每日实还表实还本金汇总",
    "repay_hst_repay_inter" -> "每日实还实还利息汇总",
    "repay_schedule_paid_count" -> "每日还款计划还款笔数汇总",
    "repay_schedule_paid_prin" -> "每日还款计划还款本金汇总",
    "repay_schedule_paid_inter" -> "每日还款计划还款利息汇总",
    "order_repay_amount" -> "每日订单表款款金额汇总"
  )



  val ods_sqoop_product = Map[String, String](
    "ods_sqoop_dd" -> "ods_dd_mysql_sqoop",
    "ods_sqoop_lx" -> "ods_lx_mysql_sqoop"
  )


  // mysql 数据统计无分区
  val mysql_sql_map=Map[String,String](
    "ecas_loan"->
      """
        |select product_code,
        |count(due_bill_no) as loan_num ,
        |sum(loan_init_prin) as loan_prin_sum,
        |sum(if(overdue_date=@date,1,0)) as over_due_num
        |from @table where active_date=@date and  product_code in (@product_code_list) group by product_code
        |""".stripMargin,

    "ecas_loan_asset"->
      """
        |
        |select product_code,
        |count(due_bill_no) as loan_num ,
        |sum(loan_init_prin) as loan_prin_sum,
        |sum(if(overdue_date=@date,1,0)) as over_due_num
        | from @table where active_date=@date and product_code in (@product_code_list)  group by product_code
        |
        |""".stripMargin,
    "ecas_repay_schedule"->
      """
        | select product_code,
        |count(due_bill_no) as  repay_schedule_paid_count,
        |sum(paid_term_pric) as repay_schedule_paid_prin,
        |sum(paid_term_int) as repay_schedule_paid_inter
        |from @table where paid_out_date=@date  and curr_term>0 and  product_code in (@product_code_list)  group by product_code
        |
        |
        |""".stripMargin,
    "ecas_repay_schedule_asset"->
      """
        |select product_code,
        |count(due_bill_no) as  repay_schedule_paid_count,
        |sum(paid_term_pric) as repay_schedule_paid_prin,
        |sum(paid_term_int) as repay_schedule_paid_inter
        |from @table where paid_out_date=@date  and curr_term>0 and  product_code in (@product_code_list)  group by product_code
        |
        |""".stripMargin,
    "ecas_repay_hst"->
      """
        |select
        |loan.product_code as product_code,
        |count(loan.due_bill_no) as repay_hst_repay_count,
        |sum(repay_hst.repay_prin) as repay_hst_repay_prin,
        |sum(repay_hst.repay_inter) as repay_hst_repay_inter
        |from
        |(select due_bill_no,
        |sum(if(bnp_type='Pricinpal',repay_amt,0)) as repay_prin,
        |sum(if(bnp_type='Interest',repay_amt,0)) as repay_inter
        | from ecas_repay_hst where batch_date =@date  and term>0
        | group by due_bill_no
        | )repay_hst
        |inner join (select due_bill_no,product_code from ecas_loan where product_code in (@product_code_list) and org='15601') loan on repay_hst.due_bill_no =  loan.due_bill_no
        |group by loan.product_code
        |""".stripMargin,
    "ecas_repay_hst_asset" ->
      """
        |select
        |loan.product_code as product_code,
        |count(loan.due_bill_no) as repay_hst_repay_count,
        |sum(repay_hst.repay_prin) as repay_hst_repay_prin,
        |sum(repay_hst.repay_inter) as repay_hst_repay_inter
        |from
        |(select due_bill_no,
        |sum(if(bnp_type='Pricinpal',repay_amt,0)) as repay_prin,
        |sum(if(bnp_type='Interest',repay_amt,0)) as repay_inter
        | from ecas_repay_hst_asset where batch_date =@date  and term>0
        | group by due_bill_no
        | )repay_hst
        |inner join (select due_bill_no,product_code from ecas_loan_asset where product_code in (@product_code_list) and org='15601') loan on repay_hst.due_bill_no =  loan.due_bill_no
        |group by loan.product_code
        |
        |
        |""".stripMargin,
    "ecas_order"->
      """
        |select
        |loan.product_code,
        |sum(txn_amt) as order_repay_amount
        |from
        |(select due_bill_no,txn_amt from ecas_order where business_date=@date and status='S' and loan_usage IN ( 'N', 'O', 'PD','R','W','M','I','D','B','F','T'))ord
        |inner join (select due_bill_no,product_code from ecas_loan where product_code in (@product_code_list) and org='15601') loan on ord.due_bill_no =  loan.due_bill_no
        |group by loan.product_code
        |
        |
        |""".stripMargin,
    "ecas_order_asset"->
      """
        |select
        |loan.product_code,
        |sum(txn_amt) as order_repay_amount
        |from
        |(select due_bill_no,txn_amt from ecas_order_asset where business_date=@date and status='S' and loan_usage IN ( 'N', 'O', 'PD','R','W','M','I','D','B','F','T'))ord
        |inner join (select due_bill_no,product_code from ecas_loan_asset where product_code in (@product_code_list) and org='15601') loan on ord.due_bill_no =  loan.due_bill_no
        |group by loan.product_code
        |
        |
        |""".stripMargin
  )

  // hive 数据带分区

  val hive_sql_map=Map[String,String](
    "ecas_loan"->
      """
        |select product_code,
        |count(due_bill_no) as loan_num ,
        |sum(loan_init_prin) as loan_prin_sum,
        |sum(if(overdue_date=@date,1,0)) as over_due_num
        | from @table where active_date=@date  and d_date=@date and  product_code in (@product_code_list)  group by product_code
        |
        |""".stripMargin,
    "ecas_loan_asset"->
      """
        |
        |select product_code,
        |count(due_bill_no) as loan_num ,
        |sum(loan_init_prin) as loan_prin_sum,
        |sum(if(overdue_date=@date,1,0)) as over_due_num
        | from @table where active_date=@date and d_date=@date and  product_code in (@product_code_list)  group by product_code
        |
        |
        |""".stripMargin,
    "ecas_repay_schedule"->
      """
        |select product_code,
        |count(due_bill_no) as  repay_schedule_paid_count,
        |sum(paid_term_pric) as repay_schedule_paid_prin,
        |sum(paid_term_int) as repay_schedule_paid_inter
        |from @table where paid_out_date=@date and d_date=@date  and  product_code in (@product_code_list)  and curr_term>0 group by product_code
        |
        |
        |""".stripMargin,
    "ecas_repay_schedule_asset"->
      """
        |select product_code,
        |count(due_bill_no) as  repay_schedule_paid_count,
        |sum(paid_term_pric) as repay_schedule_paid_prin,
        |sum(paid_term_int) as repay_schedule_paid_inter
        |from @table where paid_out_date=@date  and d_date=@date  and curr_term>0 and  product_code in (@product_code_list) group by product_code
        |
        |
        |""".stripMargin,
    "ecas_repay_hst"->
      """
        |select
        |loan.product_code as product_code,
        |count(loan.due_bill_no) as repay_hst_repay_count,
        |sum(repay_hst.repay_prin) as repay_hst_repay_prin,
        |sum(repay_hst.repay_inter) as repay_hst_repay_inter
        |from
        |(select due_bill_no,
        |sum(if(bnp_type='Pricinpal',repay_amt,0)) as repay_prin,
        |sum(if(bnp_type='Interest',repay_amt,0)) as repay_inter
        | from ecas_repay_hst where batch_date =@date and d_date=@date  and term>0
        | group by due_bill_no
        | )repay_hst
        |inner join (select due_bill_no,product_code from ecas_loan where d_date=@date ) loan on repay_hst.due_bill_no =  loan.due_bill_no
        |where loan.product_code in (@product_code_list)
        |
        |group by loan.product_code
        |
        |
        |""".stripMargin,
    "ecas_repay_hst_asset" ->
      """
        |select
        |loan.product_code as product_code,
        |count(loan.due_bill_no) as repay_hst_repay_count,
        |sum(repay_hst.repay_prin) as repay_hst_repay_prin,
        |sum(repay_hst.repay_inter) as repay_hst_repay_inter
        |from
        |(select due_bill_no,
        |sum(if(bnp_type='Pricinpal',repay_amt,0)) as repay_prin,
        |sum(if(bnp_type='Interest',repay_amt,0)) as repay_inter
        | from ecas_repay_hst_asset where batch_date =@date  and d_date=@date  and term>0
        | group by due_bill_no
        | )repay_hst
        |inner join (select due_bill_no,product_code from ecas_loan_asset where d_date=@date ) loan on repay_hst.due_bill_no =  loan.due_bill_no
        |where loan.product_code in (@product_code_list)
        |group by loan.product_code
        |
        |
        |""".stripMargin,
    "ecas_order"->
      """
        |select
        |loan.product_code,
        |sum(txn_amt) as order_repay_amount
        |from
        |(select due_bill_no,txn_amt from ecas_order where business_date=@date and d_date=@date and status='S' and loan_usage IN ( 'N', 'O', 'PD','R','W','M','I','D','B','F','T'))ord
        |inner join (select due_bill_no,product_code from ecas_loan where d_date=@date) loan on ord.due_bill_no =  loan.due_bill_no
        |where loan.product_code in (@product_code_list)
        |group by loan.product_code
        |
        |""".stripMargin,
    "ecas_order_asset"->
      """
        |select
        |loan.product_code,
        |sum(txn_amt) as order_repay_amount
        |from
        |(select due_bill_no,txn_amt from ecas_order_asset where business_date=@date and d_date=@date  and status='S' and loan_usage IN ( 'N', 'O', 'PD','R','W','M','I','D','B','F','T'))ord
        |inner join (select due_bill_no,product_code from ecas_loan_asset where d_date=@date) loan on ord.due_bill_no =  loan.due_bill_no
        |where loan.product_code in (@product_code_list)
        |group by loan.product_code
        |
        |""".stripMargin
  )






  def data_check_mysql(pro:Properties,modeName:String,batch_date:String)={
    val test_mysql = DruidDataSourceUtils.test_dataSource.getConnection

    val ods_dd_mysql_sqoop = pro.getProperty("ods_dd_mysql_sqoop").split(",").map(it=>s"'${it}'").mkString(",")
    val ods_lx_mysql_sqoop = pro.getProperty("ods_lx_mysql_sqoop").split(",").map(it=>s"'${it}'").mkString(",")
    var procode_list=ods_lx_mysql_sqoop
    if("stage_sqoop_dd".equals(modeName)){
      procode_list=ods_dd_mysql_sqoop
    }else {
      procode_list=ods_lx_mysql_sqoop
    }
    val statement = test_mysql.createStatement()
    val delete_sql=s"delete from  flink_config.ods_data_watch_emr where batch_date='${batch_date}' and product_code in (${procode_list})"
    statement.executeUpdate(delete_sql)
    statement.close()
    // 查询生产mysql 数据库

   inert_into_data_toMysql_f(mysql_sql_map,procode_list,batch_date,"pro_mysql","insert","ecasdb")
    // 查询hive

  inert_into_data_toMysql_f(hive_sql_map,procode_list,batch_date,"EMR_impala","update","stage")
    // 统计信息
    getResultData(batch_date,test_mysql,procode_list)

  }





  /**
   * 获取 结果信息数据
   * @param batch_date
   * @param test_mysql
   * @param procode_list
   */
  def getResultData(batch_date:String,test_mysql:Connection,procode_list:String)={
    val str= s"UPDATE flink_config.ods_data_watch_emr set is_success = if(mysql_value=hive_value,1,2) where batch_date='${batch_date}' and product_code in (${procode_list})"
    val statement = test_mysql.createStatement()
    statement.execute(str)
    // 获取对不上的结果数据
    val data_list = JDBCUtils.executeSQL(
      s"""
         |select
         |product_code,
         |table_name,
         |field_name,
         |mysql_value,
         |hive_value,
         |batch_date
         |from flink_config.ods_data_watch_emr
         |where batch_date='${batch_date}' and product_code in ($procode_list) and is_success='2'
         |""".stripMargin, test_mysql).map(map => {
      TreeMap[String, String](
        "product_code" -> map.getOrElse("product_code", ""),
        "table_name" -> map.getOrElse("table_name", ""),
        "field_name" -> map.getOrElse("field_name", ""),
        "mysql_value" -> map.getOrElse("mysql_value", ""),
        "hive_value" -> map.getOrElse("hive_value", ""),
        "result" -> "失败",
        "hivesql" -> hive_sql_map.getOrElse(map.getOrElse("table_name", "").replaceAll("@product_code_list",procode_list).replaceAll("@table",s"${map.getOrElse("table_name", "")}").replaceAll("@date",s"'${batch_date}'"), "")
      )
    })

    val th_arrays = Array[String]("product_code","table_name","field_name","mysql_value","hive_value","result","hivesql")
    val message = HtmlPageHandler.initHtmlPage("EMR stage和mysql抽数每日数据对比", batch_date, data_list,th_arrays)
    if(StringUtils.isNoneEmpty(message)){
      SendEmailUtil.sendMessage(message,"数据中台EMR ---ods和mysql抽数每日数据对比")
    }
    /**
     * 生成报告
     */
    dataReport(statement,procode_list,batch_date,s"flink_config.ods_data_watch_emr",test_mysql)
    statement.close()
    test_mysql.close()
  }




  def dataReport(statement: Statement,procode_list:String,batch_date:String,test_mysql_table :String,test_mysql:Connection): Unit = {

    val sql =
      s"""
         |
         |select
         |field_name,
         |project_name,
         |sum(mysql_value) as total_value
         |from
         |(
         |select
         |case when field_name='每日放款笔数汇总' then '今日放款笔数'
          when field_name='每日放款本金汇总' then '今日放款本金'
          when field_name='每日实还表还款笔数汇总' then '今日还款笔数'
          when field_name='每日实还表实还本金汇总' then '今日实还本金'
          else '其他' end as field_name,
         |case when  product_code in ('001901','001902','001903','001904','001905','001906','001907') then '中铁'
         | when  product_code in ('002001','002002','002003','002004','002005','002006','002007') then '乐信国民二期'
         | when  product_code in ('001801','001802') then '乐信国民一期'
         | when product_code in ('001601','001602','001603') then '汇通'
         | when product_code in ('001701','001702') then '瓜子'
         | when product_code in ('DIDI201908161538') then '滴滴'
         | when product_code in ('002401','002402') then '乐信国民三期'
         | else '其他' end as project_name,
         |cast(mysql_value as decimal(15,2))as mysql_value
         |from
         |${test_mysql_table}
         |where batch_date='${batch_date}'
         |and product_code in (${procode_list})
         |and field_name in ('每日放款笔数汇总','每日放款本金汇总','每日实还表还款笔数汇总','每日实还表实还本金汇总') and
         |table_name in ('ecas_loan','ecas_repay_hst')
         |) temp
         |group by field_name,project_name
         |""".stripMargin
    println(sql)

    val listData = JDBCUtils.executeSQL(
      sql, test_mysql)
    val resultData: List[Map[String, String]] = listData.groupBy(map => map.getOrElse("project_name", "")).mapValues(iter => {
      val iterator = iter.iterator
      var map = Map[String, String]()
      while (iterator.hasNext) {
        val resMap = iterator.next()
        val field_name = resMap.getOrElse("field_name", "")
        val total_value = resMap.getOrElse("total_value", "")
        map += (field_name -> total_value)
        map = map ++ resMap
      }
      map
    }).values.toList
    // 生产html 报告模版
    if (resultData.nonEmpty) {
      val htmlStr = HtmlPageHandler.initHtmlPage("EMR 每日数据汇报", batch_date, resultData, Array[String]("project_name", "今日还款笔数", "今日实还本金", "今日放款本金", "今日放款笔数"))
      SendEmailUtil.sendMessage(htmlStr,"EMR 每日新增数据汇总")

    }

    /**
     * 给企业微信的机器人发送消息
     */
    MesHandler.sendMessageToRebootPerson(batch_date,
      resultData,
      Array[String]("project_name", "今日还款笔数", "今日实还本金", "今日放款本金", "今日放款笔数"),
      test_mysql,
      "select * from flink_config.robot_person_info where isEnable=1 and id in (1,2)")
    test_mysql.close()
  }










  /**
   *
   * @param table_map 表集合
   * @param procode_list 产品号
   * @param batch_date 批次日期
   * @param mode  模式 是修改还是插入
   */
  def inert_into_data_toMysql_f(

                                table_map:Map[String,String],
                                procode_list:String,
                                batch_date:String,
                                executor_type:String,
                                mode:String,
                                db_name:String
                               ): Unit ={
    val service = Executors.newFixedThreadPool(10)
    var futureList = mutable.ListBuffer[Future[String]]()
    table_map.keySet.foreach(it=>{
      val f: Future[String] = service.submit(new Callable[String] {
        override def call(): String = {
          println(s"数据库${db_name},表${it}正在执行数据校验,校验数据日期:${batch_date},产品id:${procode_list}")
          val sql = table_map.getOrElse(it,"").replaceAll("@product_code_list",procode_list).replaceAll("@table",s"${it.toLowerCase}").replaceAll("@date",s"'${batch_date}'")
          println(sql)
          val connection = DruidDataSourceUtils.getConection(executor_type)
          println(connection.getClientInfo)
          val sql_data_list=JDBCUtils.executeSQL(sql,connection,db_name)
          val cmConnection = DruidDataSourceUtils.getConection("cm_mysql")
          inert_into_data_toMysql(sql_data_list,cmConnection,batch_date,it.toLowerCase,mode)
          cmConnection.close()
          connection.close()
          s"${Thread.currentThread.getName}:ok"

        }

      })
      futureList+=f
    })
    futureList.foreach(it=>{
      val str = it.get()
      println(str)
    })
    // 防止关闭线程池的时候出现 异常
    try {
      println("关闭线程")
      service.shutdownNow()
    }catch {
      case _: Throwable =>{
      }
    }
  }




  /**
   * 将执行完的mysql 数据统计插入到mysql中
   * @param mysql_data
   * @param test_mysql
   * @param batch_date
   * @param table_name
   */
  def inert_into_data_toMysql(mysql_data:List[Map[String,String]],test_mysql:Connection,batch_date:String,table_name:String,mode:String)={
    mode match {
        //mysql 是新增
      case "insert" =>{
        val str = s"insert into flink_config.ods_data_watch_emr (product_code,table_name,field_name,mysql_value,batch_date) value (?,?,?,?,?)"
        val pre_statment = test_mysql.prepareStatement(str)
        for(elem<-mysql_data){
          val product_code= elem.getOrElse("PRODUCT_CODE",elem.getOrElse("PRODUCT_CODE".toLowerCase,""))
          for (key <- elem.keySet if !StringUtils.equals("product_code",key.toLowerCase())) {
            pre_statment.setString(1,product_code)
            pre_statment.setString(2,table_name)
            pre_statment.setString(3,field_map.getOrElse(key,""))
            pre_statment.setString(4,elem.getOrElse(key,""))
            pre_statment.setString(5,batch_date)
            pre_statment.addBatch()
          }
          pre_statment.executeBatch()
        }
        pre_statment.close()
      }
        // hive 是修改
      case "update"=>{
        val str = s"update flink_config.ods_data_watch_emr set hive_value=? where product_code =? and table_name=? and field_name=? and batch_date=?"
        val pre_statment = test_mysql.prepareStatement(str)
        for(elem<-mysql_data){
          val product_code= elem.getOrElse("product_code","")
          for (key <- elem.keySet if !StringUtils.equals("product_code",key)) {
            pre_statment.setString(1,elem.getOrElse(key,""))
            pre_statment.setString(2,product_code)
            pre_statment.setString(3,table_name)
            pre_statment.setString(4,field_map.getOrElse(key,""))
            pre_statment.setString(5,batch_date)
            pre_statment.addBatch()
          }
          pre_statment.executeBatch()
        }
        pre_statment.close()

      }
    }
      // 借据表统计指标


  }

}
