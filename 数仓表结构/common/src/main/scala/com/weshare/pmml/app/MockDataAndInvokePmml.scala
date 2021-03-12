package com.weshare.pmml.app

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import com.weshare.pmml.dao.PmmlDaoImpl
import com.weshare.pmml.domain.{PmmlMode, Scheduler}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.json4s.jackson.Json

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.util.Random
import scala.math.BigDecimal


/**
  * Created by mouzwang on 2021-03-03 15:15
  */
object MockDataAndInvokePmml {

  def main(args: Array[String]): Unit = {
    if (args.size < 2) {
      println("illegal args error : args length is not correct!")
      return
    }
    val sparkSession = SparkSession.builder()
      .appName("MockDataAndInvokePmml" + UUID.randomUUID())
//      .master("local[4]")
//      .config("hive.metastore.uris", "thrift://10.83.0.47:9083")
      .enableHiveSupport()
      .getOrCreate()
    val batchDate :String = args(0)
    val totalCounts : Int= args(1).toInt
    val tableName = "eagle.one_million_random_risk_data"
    var counter = -1
    while(counter < totalCounts){
      if(counter == -1){
        counter += 1
        RunJob.runJob(sparkSession,batchDate,tableName,counter.toString)
      } else {
        //查询mysql库中资产池的总剩余本金
        PmmlDaoImpl.initializeDataSource()
        //创建90%-95%(不包含95)的随机数
        val randomNumber = (90 + Random.nextInt(5)) * 0.01
        //根据总剩余本金来造数
        val poolRemainPrincipal = PmmlDaoImpl.getPoolTotalPrincipal()
        val mockRemainPrincipal = (poolRemainPrincipal * randomNumber).setScale(0, BigDecimal.RoundingMode.FLOOR)
        println("mock:" + mockRemainPrincipal)
        //先将计数器+1,创建模拟数据,写入到输入表中
        val resultDS = mockData(mockRemainPrincipal, sparkSession, 100000, counter.toString)
        counter += 1
        resultDS.withColumn("cycle_key",functions.lit(counter))
          .write.mode(SaveMode.Overwrite).insertInto("eagle.one_million_random_risk_data")
        resultDS.persist()
        //调用风控模型,将预测结果数据写入到结果表中
        RunJob.runJob(sparkSession,batchDate,tableName,counter.toString)
        //更新mysql库中资产池的总剩余本金
        val paidPrincipal = getPaidPrincipal(sparkSession, counter.toString)
        val remainPrincipal = poolRemainPrincipal.-(paidPrincipal)
        PmmlDaoImpl.updatePoolTotalPrincipal(remainPrincipal)
      }
    }
    sparkSession.close()
  }

  /**
    * 根据新的资产池剩余本金来造模拟数据
    *
    * @param mockRemainPrincipal
    */
  def mockData(mockRemainPrincipal: BigDecimal, sparkSession: SparkSession, billCounts: Int, counter : String)
  = {
    import sparkSession.implicits._
    sparkSession.sql("set hive.exec.dynamic.partition=true")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    val principalList = generateRandomPrincipal(mockRemainPrincipal, billCounts)
    val principalArray = principalList.toArray
    val originDs = sparkSession.sql(
      s"""
         | select * from eagle.one_million_random_risk_data where cycle_key = '$counter' limit $billCounts
       """.stripMargin).drop("cycle_key").as[PmmlMode]
    val resultDs = originDs.coalesce(1).map(pmmlMode => {
      var index = 0
      pmmlMode.due_bill_no = UUID.randomUUID().toString.replace("-", "").toLowerCase()
      pmmlMode.loan_init_principal = principalArray(index)
      pmmlModeProcess(pmmlMode)
      index += 1
      pmmlMode
    })
    resultDs
  }

  def pmmlModeProcess(pmmlMode: PmmlMode) = {
    pmmlMode.loan_active_date = increaseOneMonth(pmmlMode.loan_active_date)
    pmmlMode.settled = "否"
    pmmlMode.paid_out_date = null
    pmmlMode.loan_out_reason = null
    pmmlMode.paid_out_type = null
    val list = RunJob.changeJsonToMap(pmmlMode,pmmlMode.schedule)
    val listBuffer = ListBuffer[Map[String,String]]()
    for (map <- list) {
      val mutableMap = Map[String,String]()
      mutableMap ++= map
      val resultMap = scheduleProcess(mutableMap,pmmlMode)
      listBuffer.append(resultMap)
    }
    pmmlMode.schedule = changeListMapToJson(listBuffer)
    pmmlMode.Loan_status = "N"
    pmmlMode.paid_amount = BigDecimal(0)
    pmmlMode.paid_principal = BigDecimal(0)
    pmmlMode.paid_interest = BigDecimal(0)
    pmmlMode.paid_penalty = BigDecimal(0)
    pmmlMode.paid_svc_fee = BigDecimal(0)
    pmmlMode.paid_term_fee = BigDecimal(0)
    pmmlMode.paid_mult = BigDecimal(0)
    pmmlMode.remain_principal = pmmlMode.loan_init_principal
    pmmlMode.remain_interest = pmmlMode.loan_init_principal.*(pmmlMode.loan_init_interest_rate)
    pmmlMode.remain_svc_fee = BigDecimal(0)
    pmmlMode.remain_term_fee = BigDecimal(0)
    pmmlMode.remain_amount = pmmlMode.loan_init_principal.+(pmmlMode.remain_interest)
    pmmlMode.overdue_principal = BigDecimal(0)
    pmmlMode.overdue_interest= BigDecimal(0)
    pmmlMode.overdue_svc_fee= BigDecimal(0)
    pmmlMode.overdue_term_fee= BigDecimal(0)
    pmmlMode.overdue_penalty= BigDecimal(0)
    pmmlMode.overdue_mult_amt= BigDecimal(0)
    pmmlMode.overdue_date_first = null
    pmmlMode.overdue_date_start = null
    pmmlMode.overdue_days = BigDecimal(0)
    pmmlMode.overdue_date = null
    pmmlMode.dpd_begin_date = null
    pmmlMode.dpd_days = BigDecimal(0)
    pmmlMode.dpd_days_count = BigDecimal(0)
    pmmlMode.dpd_days_max = BigDecimal(0)
    pmmlMode.overdue_term = BigDecimal(0)
    pmmlMode.overdue_terms_count = BigDecimal(0)
    pmmlMode.overdue_terms_max = BigDecimal(0)
    pmmlMode.overdue_principal_accumulate = BigDecimal(0)
    pmmlMode.overdue_principal_max = BigDecimal(0)
  }

  def scheduleProcess(map : Map[String,String],pmmlMode: PmmlMode)
  : Map[String,String] = {
    map.update("due_bill_no",pmmlMode.due_bill_no)
    map.update("loan_active_date",increaseOneMonth(map.getOrElse("loan_active_date","9999-01-01")))
    map.update("start_interest_date",increaseOneMonth(map.getOrElse("start_interest_date","9999-01-01")))
    map.update("should_repay_date",increaseOneMonth(map.getOrElse("should_repay_date","9999-01-01")))
    map.update("should_repay_principal",(pmmlMode.loan_init_principal./(pmmlMode.loan_init_term)).toString())
    map.update("should_repay_interest",(pmmlMode.loan_init_principal.*(pmmlMode.loan_init_interest_rate)./(pmmlMode.loan_init_term))
      .toString())
    map.update("should_repay_term_fee","0")
    map.update("should_repay_svc_fee","0")
    map.update("should_repay_penalty","0")
    map.update("should_repay_mult_amt","0")
    map.update("should_repay_penalty_acru","0")
    map.update("should_repay_amount", (pmmlMode.loan_init_principal./(pmmlMode.loan_init_term) + pmmlMode.loan_init_principal.*(pmmlMode.loan_init_interest_rate)./(pmmlMode.loan_init_term)).toString)
    map.update("schedule_status","N")
    map.update("schedule_status_cn","N")
    map.update("schedule_status_cn","N")
    map.update("paid_out_date",null)
    map.update("paid_out_type",null)
    map.update("paid_out_type_cn",null)
    map.update("paid_amount","0")
    map.update("paid_principal","0")
    map.update("paid_interest","0")
    map.update("paid_term_fee","0")
    map.update("paid_svc_fee","0")
    map.update("paid_penalty","0")
    map.update("paid_mult","0")
    map.update("reduce_amount","0")
    map.update("reduce_principal","0")
    map.update("reduce_interest","0")
    map.update("reduce_term_fee","0")
    map.update("reduce_svc_fee","0")
    map.update("reduce_penalty","0")
    map.update("reduce_mult_amt","0")
    map
  }

  /**
    * 将 ListMap 转换成Json 对象
    * @param schedulers
    */
  def changeListMapToJson(schedulers:ListBuffer[Map[String,String]]) ={
    var result = Map[String, Map[String,String]]()
    schedulers.foreach(it=>{
      val loan_term = it.getOrElse("loan_term","")
      result +=(loan_term->it)
    })
    import org.json4s.DefaultFormats
    Json(DefaultFormats).write(result)
  }

  def getPaidPrincipal(sparkSession: SparkSession,counter:String) : BigDecimal = {
    val result = sparkSession.sql(
      s"""
         | select sum(paid_principal) as paid_principal
         | from eagle.predict_repay_day where cycle_key='$counter'
       """.stripMargin).take(1)(0).getAs[java.math.BigDecimal](0)
    BigDecimal(result)
  }

  /**
    * 创建总和为总本金余额的10万个非零随机数
    * 此算法相当于将( mockRemainPrincipal / 10 ) 份10元随机分配给10万条数据,因为要求数据不能为0,所以直接给每条数据初始化一个10,
    * 然后循环((mockRemainPrincipal - 10*100000) / 10) 次
    *
    * @param mockRemainPrincipal
    * @return
    */
  def generateRandomPrincipal(mockRemainPrincipal: BigDecimal, billCounts: Int): List[BigDecimal] = {
    val array = new Array[BigDecimal](billCounts)
    for (i <- 0 until billCounts) {
      array(i) = 10
    }
    val cycleCounter = (mockRemainPrincipal./(BigDecimal(10))).-(BigDecimal(billCounts))
    for (i <- 1 to cycleCounter.toInt) {
      //随机选中第1到10万条中的某一条数据,给他加10
      val randomIndex = Random.nextInt(billCounts)
      array(randomIndex) = array(randomIndex).+(BigDecimal(10))
    }
    array.toList
  }

  def increaseOneMonth(dateStr: String): String = {
    val cal = Calendar.getInstance()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(dateStr)
    cal.setTime(date)
    cal.add(Calendar.MONTH,1)
    format.format(cal.getTime)
  }
}
