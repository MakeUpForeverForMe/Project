package com.weshare.repair_data

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties, UUID}

import com.weshare.repair_data.mode.{Loan, RepairMode, RepaySchedule}
import com.weshare.utils.{DruidDataSourceUtils, JDBCUtils, ProertiesUtils, SendEmailUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import scala.util.Properties

/**
 * created by chao.guo on 2020/11/25
 * // 汇通修数程序
 * 1  取T-1 日的借据表
 *
 *
 **/
object RepairMain {
  def main(args: Array[String]): Unit = {
    //通过spark查询 kudu 表 获取要修的借据，放款日期  结束日期
    val spark = SparkSession.builder()
     //.master("local[*]")
     /* .master("yarn")*/
      .config("hive.exec.dynamic.partition","true")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("hive.exec.max.dynamic.partitions","100000")
      .config("hive.exec.max.dynamic.partitions.pernode","100000")
      .config("hive.exec.max.created.files","100000")
      //.config("hive.metastore.uris","thrift://10.83.0.47:9083")
      .appName("RepairMain")
      .enableHiveSupport()
      .getOrCreate()
    //val batch_date="2020-10-02"
    //val task_subject=;
    spark.sparkContext.setLogLevel("error")
    val cluster_type=args(0)
    val format = new SimpleDateFormat("yyyy-MM-dd")
   //  1 查询现在所有的问题借据
    //数据校验执行impala 查询
    val batch_date = args(1)
    val properties = ProertiesUtils.propertiesLoad(cluster_type)
    initCompent(properties)
    val ht_repair_table = properties.getProperty("ht_repair_table")
    val connection = DruidDataSourceUtils.getConection("cm_mysql")
    connection.setAutoCommit(false)
    val statement = connection.prepareStatement(s"insert into  ${ht_repair_table} (id,due_bill_no,register_date,active_date,repair_flag,repair_date) values(?,?,?,?,?,?) on DUPLICATE KEY UPDATE repair_flag=1,repair_date='${format.format(new Date())}'")
    val repairModes =getRepairMode(spark,connection,batch_date,properties)
    repairModes.foreach(it=>{
      val due_bill_no = it.due_bill_no
      val start_move_date = it.active_date
     val end_move_date = it.register_date  // 注册时当天是有数据的 所以应该 为<
      //val end_move_date ="2019-12-01"
      val start_calendar_ = Calendar.getInstance()
      start_calendar_.setTime(format.parse(start_move_date))
      val end_calendar_ = Calendar.getInstance()
      end_calendar_.setTime(format.parse(end_move_date))
       //查询借据 查询还款计划
      val scheduleDs = querySchedule(spark,due_bill_no,end_move_date)
      val loanDs = queryLoan(spark,due_bill_no,end_move_date)
      // 循环初始化 借据和还款计划
      while (start_calendar_.getTime.compareTo(end_calendar_.getTime)<0){
        println(s"due_bill_no:${due_bill_no},start_date:${format.format(start_calendar_.getTime)}")
        val start = format.format(start_calendar_.getTime)
        val temp_loands = initLoan(spark,loanDs,start,format)
        val temp_scheduleds = initRepaySchedule(spark,scheduleDs,start)
        saveLoan(temp_loands)
        saveRepaySchedule(temp_scheduleds)
        start_calendar_.add(Calendar.DAY_OF_YEAR,1) // 日期加1
      }
      it.repair_date=format.format(new Date())
      it.repair_flag=1
      statement.setString(1,due_bill_no)
      statement.setString(2,due_bill_no)
      statement.setString(3,end_move_date)
      statement.setString(4,start_move_date)
      statement.setInt(5,it.repair_flag)
      statement.setString(6,it.repair_date)
      statement.addBatch()
    })
    connection.commit()
    statement.executeBatch()
    SendEmailUtil.sendMessage(repairModes.mkString("\n"),
      "汇通修复晚到数据",
    "chao.guo@weshareholdings.com,yunan.huang@weshareholdings.com")


    statement.close()
    connection.close()
    spark.stop()

  }

  /**
   * 追加插入数据
   * @param ds
   */
  def saveLoan(ds:Dataset[Loan])={
    ds.select("org","loan_id","acct_nbr","acct_type",
      "cust_id","due_bill_no","apply_no","register_date",
      "request_time","loan_type","loan_status","loan_init_term",
      "curr_term","remain_term","loan_init_prin","totle_int","totle_term_fee","totle_svc_fee",
      "unstmt_prin","paid_principal","paid_interest","paid_svc_fee","paid_term_fee","paid_penalty","paid_mult","active_date"
      ,"paid_out_date","terminal_date","terminal_reason_cd","loan_code","register_id","interest_rate","penalty_rate","loan_expire_date"
      ,"loan_age_code","past_extend_cnt","past_shorten_cnt","contract_no","overdue_date","max_cpd","max_cpd_date","max_dpd","max_dpd_date","cpd_begin_date"
      ,"loan_fee_def_id","purpose","product_code","pre_age_cd_gl","age_code_gl","normal_int_acru","totle_mult_fee","totle_penalty","is_int_accural_ind","collect_out_date","create_time"
      ,"create_user","loan_settle_reason","repay_term","overdue_term","count_overdue_term","max_overdue_term","max_overdue_prin","overdue_days","count_overdue_days",
      "max_overdue_days","reduce_prin","reduce_interest","reduce_svc_fee","reduce_term_fee","reduce_penalty","reduce_mult_amt","overdue_prin"
      ,"overdue_interest","overdue_svc_fee","overdue_term_fee","overdue_penalty","overdue_mult_amt","svc_fee_rate","term_fee_rate","acq_id","cycle_day"
      ,"goods_princ","sync_date","capital_plan_no","lst_upd_time","lst_upd_user","capital_type","d_date","p_type").coalesce(1).write.mode(SaveMode.Append).insertInto("ods.ecas_loan_ht_repair")


  }

  //追加插入数据
  def saveRepaySchedule(ds:Dataset[RepaySchedule]): Unit ={
    ds.select("org","schedule_id","due_bill_no","curr_bal","loan_init_prin","loan_init_term","curr_term","due_term_prin","due_term_int"
      ,"due_term_fee","due_svc_fee","due_penalty","due_mult_amt","paid_term_pric","paid_term_int","paid_term_fee","paid_svc_fee","paid_penalty"
      ,"paid_mult_amt","reduced_amt","reduce_term_prin","reduce_term_int","reduce_term_fee","reduce_svc_fee","reduce_penalty","reduce_mult_amt"
      ,"penalty_acru","paid_out_date","paid_out_type","start_interest_date","pmt_due_date","origin_pmt_due_date","product_code","schedule_status",
      "grace_date","create_time","create_user","lst_upd_time","lst_upd_user","jpa_version","out_side_schedule_no","d_date","p_type").coalesce(1).write.mode(SaveMode.Append).insertInto("ods.ecas_repay_schedule_ht_repair")

  }


  /**
   * 初始化组件信息
   * 初始化连接池信息
   *
   * @param pro
   */
  def initCompent(pro:Properties): Unit ={
    val hdfs_master_path = pro.getProperty("hdfs_master")
    val cm_config_path = pro.getProperty("cm_config")
    val isHa = Integer.parseInt(pro.getProperty("isHa"))
    val email_config_path = pro.getProperty("email_config")
    val hdfs_hive_jdbc_config_path = pro.getProperty("hdfs_hive_jdbc_config_path")
    DruidDataSourceUtils.initDataSource(cm_config_path,hdfs_master_path,"cm_mysql",isHa)
    DruidDataSourceUtils.initDataSource(hdfs_hive_jdbc_config_path,hdfs_master_path,"hive",isHa)
    SendEmailUtil.initEmailSession(email_config_path,hdfs_master_path,isHa)
  }



  /**
   * 校验数据 并将数据插入到kudu 表
   * @param conn
   * @param batch_date
   */
  def getRepairMode(spark: SparkSession,conn:Connection,batch_date:String,properties: Properties)={
    // 查询出ods 快照日里面 有问题的借据
    val ht_repair_table = properties.getProperty("ht_repair_table")
    import spark.implicits._
    val df = spark.sql(
      s"""
        |
        |select
        |a.due_bill_no as id,
        |a.due_bill_no,
        |a.register_date,
        |a.active_date
        |from
        |(select due_bill_no,active_date,register_date from ods.ecas_loan where d_Date='${batch_date}' and p_type='ddht' and  product_code in ('001601','001602','001603')
        |and active_date!=register_date )a
        |
        |""".stripMargin).coalesce(1).as[(String, String, String, String)]
 // 查询mysql 获取已修复的数据
      val repirList = JDBCUtils.executeSQL(s"select due_bill_no from ${ht_repair_table} where repair_flag=1",conn)
      spark.sparkContext.broadcast(repirList)
      df.filter(it=>{!repirList.contains(it._1)}).mapPartitions(it=>{//过滤掉 已经处理过的借据
        var list = List[RepairMode]()
        while (it.hasNext) {
          val tuple = it.next()
          list= RepairMode(
            tuple._1,
            tuple._2,
            tuple._3,
            tuple._4,
            0,
            null
          )::list

        }
        list.iterator

      }).coalesce(1).collect().toList
  }






  /**
   * 计算当前的跑批时间
   * @param string
   * @return
   */
  def clcalLastBatchDate(string: String):String={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(string)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_YEAR, -1)
    val lastdate = calendar.getTime
    format.format(lastdate)
  }

  /**
   * 根据借据号 查询loan 表数据
   * @param due_bill_no
   * @param register_date
   */
  def queryLoan(sparkSession: SparkSession,due_bill_no:String,register_date:String)={
    import sparkSession.implicits._
    val loan_query =
      s"""
         |select
         |*
         |from
         |ods.ecas_loan
         |where due_bill_no='${due_bill_no}'
         |and p_type='ddht'
         |and d_date=date_add('${register_date}',1)
         |""".stripMargin
    sparkSession.sql(loan_query).as[Loan]
  }


  /**
   * 根据借据号和 注册日期 查询改日的还款计划
   * @param sparkSession
   * @param due_bill_no
   * @param register_date
   * @return
   */
  def querySchedule (sparkSession: SparkSession,due_bill_no:String,register_date:String)={
    import sparkSession.implicits._
    val repay_schedule_query =
      s"""
         |select
         |org                   ,
         |schedule_id           ,
         |due_bill_no           ,
         |curr_bal              ,
         |loan_init_prin        ,
         |loan_init_term        ,
         |curr_term             ,
         |due_term_prin         ,
         |due_term_int          ,
         |due_term_fee          ,
         |due_svc_fee           ,
         |due_penalty           ,
         |due_mult_amt          ,
         | paid_term_pric,
         |paid_term_int,
         |paid_term_fee,
         |paid_svc_fee ,
         |paid_penalty ,
         | paid_mult_amt ,
         |reduced_amt           ,
         |reduce_term_prin      ,
         |reduce_term_int       ,
         |reduce_term_fee       ,
         |reduce_svc_fee        ,
         |reduce_penalty        ,
         |reduce_mult_amt       ,
         |penalty_acru          ,
         |paid_out_date ,
         |paid_out_type ,
         |start_interest_date   ,
         |pmt_due_date          ,
         |origin_pmt_due_date   ,
         |product_code          ,
         |schedule_status ,
         |grace_date            ,
         |create_time           ,
         |create_user           ,
         |lst_upd_time          ,
         |lst_upd_user          ,
         |jpa_version           ,
         |out_side_schedule_no ,
         |d_date,
         |p_type
         |from
         |ods.ecas_repay_schedule
         |where due_bill_no='${due_bill_no}'
         |and p_type='ddht'
         |and d_date=date_add('${register_date}',1)
         |""".stripMargin
    sparkSession.sql(repay_schedule_query).as[RepaySchedule]


  }


  /***
   * 初始化还款计划   变化还款计划 从查询出的还款计划  上做初始化
   * @param sparkSession
   * @param dataset
   * @param start_Date
   * @return
   */

  def initRepaySchedule(sparkSession: SparkSession,dataset: Dataset[RepaySchedule],start_Date:String)={
    import sparkSession.implicits._
    dataset.mapPartitions(it=>{
      var schedules = List[RepaySchedule]()
      while (it.hasNext) {
        val schedule = it.next()
        // 数据初始化
        if(StringUtils.isNoneBlank(schedule.paid_out_date)&& start_Date.compareTo(schedule.paid_out_date)<0){
          schedule.paid_term_pric=BigDecimal.valueOf(0)
          schedule.paid_term_int=BigDecimal.valueOf(0)
          schedule.paid_term_fee=BigDecimal.valueOf(0)
          schedule.paid_svc_fee=BigDecimal.valueOf(0)
          schedule.paid_penalty=BigDecimal.valueOf(0)
          schedule.paid_mult_amt=BigDecimal.valueOf(0)
          schedule.paid_out_date=null
          schedule.paid_out_type=null
          schedule.schedule_status="N"
        }
        schedule.d_date=start_Date
        schedules=schedule::schedules
      }
      schedules.iterator

    })
  }

  /**
   * 初始化loan 表数据 根据时间  注意给我们的借据不能是有还款
   *
   * @param sparkSession
   * @param dataset
   * @param start_Date
   */
  def initLoan(sparkSession: SparkSession,dataset: Dataset[Loan],start_Date:String,format: SimpleDateFormat)={
    import sparkSession.implicits._
    dataset.mapPartitions(it=>{
      var loans = List[Loan]()
        while (it.hasNext) {
          val loan = it.next()
          val start_date_ = Calendar.getInstance()
          val pmt_date_ = Calendar.getInstance()
          start_date_.setTime(format.parse(start_Date)) // 当前批次日期
          pmt_date_.setTime(format.parse(loan.active_date)) //放款日+1个月 即为第一个应还日
          pmt_date_.add(Calendar.MONTH,1)
          if(start_date_.getTime.compareTo(pmt_date_.getTime)<0){ // 平推日期小于 应还款日 设置 期数 和借据状态
            loan.curr_term=1
            loan.loan_status="N"
          }
          loan.d_date=start_Date
          loans=loan::loans
        }
      loans.iterator
    })
  }



}
