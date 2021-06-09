package com.weshare.pmml.app

import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import com.weshare.data_check.Handler.MesHandler
import com.weshare.pmml.dao.PmmlDaoImpl
import com.weshare.pmml.domain.{PmmlMode, PmmlParam, Scheduler}
import com.weshare.utils.{PropertiesUtils, SentMessage}
import io.netty.util.internal.ThreadLocalRandom
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.json4s.jackson.Json
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.util.Random
import scala.math.BigDecimal
import scala.math.BigDecimal.RoundingMode


/**
  * Created by mouzwang on 2021-03-03 15:15
  */
object MockDataAndInvokePmml {
  private val logger: Logger = LoggerFactory.getLogger(MockDataAndInvokePmml.getClass)

  val sourceTableName = "eagle.one_million_random_risk_data"
  val targetTableName = "eagle.predict_schedule"

  def main(args: Array[String]): Unit = {
    if (args.size < 3) {
      println("illegal args error : args length is not correct!")
      return
    }
    val sparkSession = SparkSession.builder()
      .appName("MockDataAndInvokePmml" + System.currentTimeMillis())
      .config("hive.execution.engine","spark")
      .config("hive.exec.dynamic.partition","true")
      .config("hive.exec.dynamic.partition.mode","nonstrict")
      .config("hive.exec.max.dynamic.partitions","100000")
      .config("hive.exec.max.dynamic.partitions.pernode","100000")
      .config("hive.exec.max.created.files","100000")
      .config("hive.merge.size.per.task","268435456")
      .config("hive.merge.mapredfiles","true")
      .config("hive.merge.smallfiles.avgsize","268435456")
      .config("mapreduce.input.fileinputformat.split.minsize","268435456")

/*      .master("local[4]")
       .config("hive.metastore.uris", "thrift://10.83.0.47:9083")*/
      .enableHiveSupport()
      .getOrCreate()
    //sparkSession.sql("set hive.execution.engine=spark")
    val batchDate: String = args(0)
    //val total_count=args(1).toInt
    val project_id=args(1) // 项目编号
    var start_counter:Int= 0;
    val run_cluster_type=args(2) // TEST PRO EMR

    //初始化配置文件
    val pro = PropertiesUtils.propertiesLoad(run_cluster_type)
    // 初始化数据连接池
    PmmlDaoImpl.initializeDataSource(pro)
    // 获取所有Pmml文件的位置
    val param = PmmlDaoImpl.getPmmlParamMode(project_id)
    // 初始化参数列表
    param.batch_date=batchDate
    param.hdfs_master=pro.getProperty("hdfs_master")
    param.tablename=sourceTableName
    param.isHa=pro.getProperty("isHa").toInt


      RunJob.runJob(sparkSession, param)
      // 模拟放款
    val avgLoanInitPrincipal = getAvgLoanInitPrincipal(sparkSession,"0",batchDate,project_id) // 只查询一次 基于第0次
    //判断当前是否处于循环期
    logger.error(s"avgLoanInitPrincipal:${avgLoanInitPrincipal}")
    if(PmmlParamDeal.is_cycle_Stage(param,batchDate)){ // 处于循环期, 没有循环期的项目 循环期开始日期等于 循环期结束日期
      //
     val total_count = PmmlParamDeal.cycle_diff_terms(param)
      logger.error(s"项目:{},处于循环期,可用于模拟新增放款次数:${}",param.projectId,total_count)
      while (start_counter<total_count){
        val poolRemainPrincipal = PmmlDaoImpl.getPoolTotalPrincipal(project_id)
        if(poolRemainPrincipal <= 0 ) {
          logger.warn("the asset pool is empty ! total remain principal :{}" , param.available_amount)
          return
        }
      // 根据随机总剩余本金与平均放款金额计算借据数
        val billCounts : Int = (poolRemainPrincipal / avgLoanInitPrincipal).toInt
        // 获取可用期次造数
        val resultDS = mockData(param, sparkSession, billCounts,poolRemainPrincipal)
        start_counter += 1
        resultDS.
          withColumn("cycle_key", functions.lit(start_counter))

          .select("due_bill_no","product_id","risk_level","loan_init_principal","loan_init_term","loan_init_interest_rate",
            "credit_coef","loan_init_penalty_rate","loan_active_date","settled","paid_out_date",
            "loan_out_reason","paid_out_type","schedule","Loan_status","paid_amount", "paid_principal","paid_interest","paid_penalty",
            "paid_svc_fee","paid_term_fee","paid_mult", "remain_amount","remain_principal", "remain_interest","remain_svc_fee", "remain_term_fee","overdue_principal","overdue_interest",
            "overdue_svc_fee","overdue_term_fee","overdue_penalty", "overdue_mult_amt","overdue_date_first",
            "overdue_date_start","overdue_days","overdue_date","dpd_begin_date","dpd_days","dpd_days_count",
            "dpd_days_max","collect_out_date","overdue_term","overdue_terms_count","overdue_terms_max",
            "overdue_principal_accumulate","overdue_principal_max","creditlimit","edu","degree","homests",
            "marriage","mincome","homeincome","zxhomeincome","custtype","worktype","workduty","worktitle",
            "idcard_area","risklevel","scorerange","rn","biz_date","project_id","cycle_key"
          ).repartition(10).
          //write.partitionBy("biz_date","project_id","cycle_key").parquet("/user/hive/eagle.one_million_random_risk_data/")
        //sparkSession.sql(s"load data inpath '/user/hive/eagle.one_million_random_risk_data/biz_date=${batchDate}/project_id=${param.projectId}/cycle_key=${}'")
        //
          write.mode(SaveMode.Overwrite).insertInto("eagle.one_million_random_risk_data")
        resultDS.unpersist()
          param.cycle_key=start_counter.toString
        //调用风控模型,将预测结果数据写入到结果表中
        RunJob.runJob(sparkSession, param)
      }
    }
    sparkSession.close()
    //发送邮件通知
    SentMessage.sendMessage(s"EMR-模型预测（project_id：${project_id}）,biz_date:${batchDate}","text",pro,"select * from flink_config.robot_person_info where isEnable=1 and id=2")

  }





  /**
    * 根据新的资产池剩余本金来造模拟数据
    *
    * @param
    */
  def mockData(param:PmmlParam, sparkSession: SparkSession, billCounts: Int,poolRemainPrincipal:BigDecimal)
  = {
    import sparkSession.implicits._
    /*sparkSession.sql("set hive.exec.dynamic.partition=true")*/
    /*sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")*/
   // val principalList: List[BigDecimal] = generateRandomPrincipal(poolRemainPrincipal, billCounts)
   // val principalArray = principalList.toArray
   // val broadCast = sparkSession.sparkContext.broadcast(principalList)
    //获取要限制的期数
    logger.error(s"billCounts:${billCounts},poolRemainPrincipal:${poolRemainPrincipal}")

    val originDs = sparkSession.sql(
      s"""
         |
         |
         |
         | select *
         | from
         |  eagle.one_million_random_risk_data where cycle_key = '0' and
         |  project_id='${param.projectId}' and biz_date='${param.batch_date}'
         | and rn <=${billCounts}

       """.stripMargin)
    originDs.cache()

    val next_month = cycleNextMonth(PmmlParamDeal.isCurrentMonth(param.batch_date,param.cycle_key.toInt),param.batch_date)
    var loan_terms = PmmlParamDeal.cycle_loan_terms(param, next_month)
    if(loan_terms.isEmpty){
      loan_terms=List(1,1)
    }
    logger.error(s"模拟新增放款月为:${next_month}可用于造数的期次为${loan_terms}")
    val resultDs = originDs.drop("cycle_key").as[PmmlMode].mapPartitions(
      it=>{
        var list =ListBuffer[PmmlMode]()
       // val principalLists= broadCast.value
        //val length = cacleListSize(principalLists)-1
        while (it.hasNext) {
          val pmmlMode =it.next()
          //val index = Random.nextInt(length-1)
          pmmlMode.due_bill_no = UUID.randomUUID().toString.replace("-", "").toLowerCase()
          pmmlMode.loan_init_principal = getLoan_init_Principal(poolRemainPrincipal,billCounts)
          //处理期数
          if(pmmlMode.loan_init_term>loan_terms.max){
            pmmlMode.loan_init_term=loan_terms(ThreadLocalRandom.current().nextInt(loan_terms.length))
          }
          // 模拟生成借据的放款日期 放款日期 +放款期次  应小于等于 项目结束日期
          pmmlMode.loan_active_date = randomDateOfThisMonth(param,next_month,pmmlMode.loan_init_term)
          logger.error(s"放款日:${pmmlMode.loan_active_date},放款期次:${pmmlMode.loan_init_term}")
          pmmlModeProcess(pmmlMode,next_month)
          list +=pmmlMode
        }
        list.iterator
      /*pmmlMode => {

     /* index += 1*/
      pmmlMode*/
    })
    originDs.unpersist()
    resultDs
  }

  def cycleNextMonth(counter:Int,batch_date:String):String={
    val cal = Calendar.getInstance()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(batch_date)
    cal.setTime(date)
    cal.add(Calendar.MONTH, counter)
    format.format(cal.getTime)
  }


  /**
   * 计算数组长度
   * @param list
   * @return
   */
  def cacleListSize(list :List[BigDecimal] ):Int={
    var len=0;
    list.foreach(_=>len=len+1)
    len
}


  def pmmlModeProcess(pmmlMode: PmmlMode,next_month:String) = {


    /*val tempDate = increaseMonthByTerm(pmmlMode.loan_active_date,"1")*/

    pmmlMode.settled = "否"
    pmmlMode.risk_level=pmmlMode.risklevel
    pmmlMode.paid_out_date = null
    pmmlMode.loan_out_reason = null
    pmmlMode.paid_out_type = null
    val list = RunJob.changeJsonToMap(pmmlMode, pmmlMode.schedule)
    val listBuffer=list.filter(it=>{
      it.getOrElse("loan_term","0").toInt<=pmmlMode.loan_init_term
    }).map(map=>{
      val mutableMap = Map[String, String]()
      mutableMap ++= map
      scheduleProcess(mutableMap, pmmlMode)
      //listBuffer.append(resultMap)
    }).toList

    /*val listBuffer = ListBuffer[Map[String, String]]()
    for (map <- list) {
      val mutableMap = Map[String, String]()
      mutableMap ++= map
      val resultMap = scheduleProcess(mutableMap, pmmlMode)
      listBuffer.append(resultMap)
    }*/
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
    pmmlMode.overdue_interest = BigDecimal(0)
    pmmlMode.overdue_svc_fee = BigDecimal(0)
    pmmlMode.overdue_term_fee = BigDecimal(0)
    pmmlMode.overdue_penalty = BigDecimal(0)
    pmmlMode.overdue_mult_amt = BigDecimal(0)
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

  def scheduleProcess(map: Map[String, String], pmmlMode: PmmlMode)
  : Map[String, String] = {
    map.update("due_bill_no", pmmlMode.due_bill_no)
    map.update("loan_init_principal", pmmlMode.loan_init_principal.toString())
    map.update("loan_init_term", pmmlMode.loan_init_term.toString())
    map.update("loan_active_date", pmmlMode.loan_active_date)
    map.update("start_interest_date", pmmlMode.loan_active_date)
    map.update("should_repay_date", increaseMonthByTerm(pmmlMode.loan_active_date,map.getOrElse
    ("loan_term","1")))
    map.update("grace_date",getGraceDate(increaseMonthByTerm(pmmlMode.loan_active_date,map.getOrElse
    ("loan_term","1"))))
    map.update("should_repay_principal", (pmmlMode.loan_init_principal / pmmlMode.loan_init_term).setScale
    (2,RoundingMode.FLOOR).toString())
    map.update("should_repay_interest", (pmmlMode.loan_init_principal * pmmlMode.loan_init_interest_rate /
    pmmlMode.loan_init_term).setScale(2,RoundingMode.FLOOR).toString())
    map.update("should_repay_term_fee", "0")
    map.update("should_repay_svc_fee", "0")
    map.update("should_repay_penalty", "0")
    map.update("should_repay_mult_amt", "0")
    map.update("should_repay_penalty_acru", "0")
    map.update("should_repay_amount", (BigDecimal(map.getOrElse("should_repay_principal","0")) + BigDecimal
    (map.getOrElse("should_repay_interest","0"))).toString())
    map.update("schedule_status", "N")
    map.update("schedule_status_cn", "N")
    map.update("schedule_status_cn", "N")
    map.update("paid_out_date", null)
    map.update("paid_out_type", null)
    map.update("paid_out_type_cn", null)
    map.update("paid_amount", "0")
    map.update("paid_principal", "0")
    map.update("paid_interest", "0")
    map.update("paid_term_fee", "0")
    map.update("paid_svc_fee", "0")
    map.update("paid_penalty", "0")
    map.update("paid_mult", "0")
    map.update("reduce_amount", "0")
    map.update("reduce_principal", "0")
    map.update("reduce_interest", "0")
    map.update("reduce_term_fee", "0")
    map.update("reduce_svc_fee", "0")
    map.update("reduce_penalty", "0")
    map.update("reduce_mult_amt", "0")
    map
  }

  /**
    * 将 ListMap 转换成Json 对象
    *
    * @param schedulers
    */
  def changeListMapToJson(schedulers: List[Map[String, String]]) = {
    var result = Map[String, Map[String, String]]()
    schedulers.foreach(it => {
      val loan_term = it.getOrElse("loan_term", "")
      result += (loan_term -> it)
    })
    import org.json4s.DefaultFormats
    Json(DefaultFormats).write(result)
  }

  def getRemainPrincipal(sparkSession: SparkSession, counter: String,batch_date:String,project_id:String)
  : BigDecimal = {
    val result = sparkSession.sql(
      s"""
         | select sum(should_repay_principal - paid_principal) as remain_principal
         | from eagle.predict_repay_day where cycle_key='$counter' and biz_date='${batch_date}' and project_id='${project_id}'
       """.stripMargin).take(1)(0).getAs[java.math.BigDecimal](0)
    BigDecimal(result)
  }

  /**
    * 取输入表中的借据平均放款金额
    * @param sparkSession
    * @param counter
    * @return
    */
  def getAvgLoanInitPrincipal(sparkSession: SparkSession, counter: String,batch_date:String,project_id:String): BigDecimal = {
    val result = sparkSession.sql(
      s"""
         | select sum(loan_init_principal) / count(due_bill_no)
         | from eagle.one_million_random_risk_data where cycle_key='$counter' and biz_date='${batch_date}' and project_id='${project_id}'
       """.stripMargin).take(1)(0).getAs[java.math.BigDecimal](0)
    BigDecimal(result)
  }

  /**
   * 基于总本金生成放款本金
   */
  val getLoan_init_Principal=(mockRemainPrincipal: BigDecimal, billCounts: Int)=>{
    val factor = 0.97
    val avgPrincipal =  ((mockRemainPrincipal / billCounts) * factor).setScale(0,RoundingMode.FLOOR)
    val random=((mockRemainPrincipal / billCounts) * (1-factor)).setScale(0,RoundingMode.FLOOR).toInt
    avgPrincipal+BigDecimal(ThreadLocalRandom.current().nextInt(random))
  }



  /**
    * 创建总和为总本金余额的指定个数非零随机数
    * 先算出平均分配额度,将平均分配额度的一半(默认系数为0.5)给每条借据初始化额度
    * 此算法相当于将( mockRemainPrincipal / 平均分配额度 ) 份平均金额随机分配给指定条数数据,因为要求数据不能为0,所以直接给每条数据初始化一个平均分配额度的一半金额,
    * 然后循环((mockRemainPrincipal - 平均额度一半*条数) / 平均额度一半) 次
    *
    * @param mockRemainPrincipal
    * @return
    */
  def generateRandomPrincipal(mockRemainPrincipal: BigDecimal, billCounts: Int) = {
      val factor = 0.9
      val array = new Array[BigDecimal](billCounts)
      val avgPrincipal =  ((mockRemainPrincipal / billCounts) * factor).setScale(0,RoundingMode.FLOOR)
      val random=((mockRemainPrincipal / billCounts) * (1-factor)).setScale(0,RoundingMode.FLOOR).toInt
      array.map(_=>avgPrincipal+BigDecimal(Random.nextInt(random))).toList
    /*  array.map(_=>avgPrincipal).toList*/
      //回收数组长度

      /*val factor = 0.5
      val array = new Array[BigDecimal](billCounts)
      val avgPrincipal =  ((mockRemainPrincipal / billCounts) * factor).setScale(0,RoundingMode.FLOOR)
      for (i <- 0 until billCounts) {
        array(i) = 10
      }
      //为了取整,重新计算虚拟剩余本金
      val newMockRemainPrincipal = (avgPrincipal  * billCounts) / factor
      val cycleCounter = (newMockRemainPrincipal / avgPrincipal) - BigDecimal(billCounts)
      for (i <- 1 to cycleCounter.toInt) {
        //随机选中第1到总条数中的某一条数据,给他加上分配金额
        val randomIndex = Random.nextInt(billCounts)
        array(randomIndex) = array(randomIndex) + avgPrincipal
      }*/
      //array.toList
  }

  def getGraceDate(dateStr : String) = {
    val cal = Calendar.getInstance()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(dateStr)
    cal.setTime(date)
    cal.add(Calendar.DATE,3)
    format.format(cal.getTime)
  }

  def increaseMonthByTerm(dateStr: String,loanTerm : String): String = {
    val cal = Calendar.getInstance()
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(dateStr)
    cal.setTime(date)
    cal.add(Calendar.MONTH, loanTerm.toInt)
    format.format(cal.getTime)
  }

  /**
   * 生成随机数
   * @param
   * @param
   * @param
   * @return
   */
  val randomDateOfThisMonth=(pmmlParam: PmmlParam,dateStr : String,loan_init_term:BigDecimal)  => {
    val yearMonthFormat = new SimpleDateFormat("yyyy-MM-")
    val str = yearMonthFormat.format(yearMonthFormat.parse(dateStr))
    val month = str.split("-")(1)
    var randomDay : String = null
    var random : Int = 0
    if("02" .equals(month) ) {
      random = ThreadLocalRandom.current().nextInt(27) + 1
      randomDay = if (random < 10) "0" + random.toString else random.toString
    } else {
      random = ThreadLocalRandom.current().nextInt(30) + 1
      randomDay = if (random < 10) "0" + random.toString else random.toString
    }
    //判断最后一个应还日 是否会大于项目结束日期  如果大于则重新设置随机数 以保证 放款日在项目结束日期之前
    calculationMonth(str + randomDay,pmmlParam.project_end_date,loan_init_term)
  }

  /**
   * 计算放款月
   */
  val calculationMonth=(loan_active_date:String,project_end_date:String,loan_init_term:BigDecimal)=>{
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val loan_active_ = simpleDateFormat.parse(loan_active_date)
    val calendar = Calendar.getInstance()
    calendar.setTime(loan_active_)
    calendar.add(Calendar.MONTH,loan_init_term.toInt)
    val calendar2 = Calendar.getInstance()
    val project_end_ = simpleDateFormat.parse(project_end_date)
    calendar2.setTime(project_end_)
    if(calendar.getTime.compareTo(calendar2.getTime)>0){
      calendar.set(Calendar.DAY_OF_MONTH,ThreadLocalRandom.current().nextInt(calendar2.get(Calendar.DAY_OF_MONTH)))
    }
    calendar.add(Calendar.MONTH,-loan_init_term.toInt)
    simpleDateFormat.format(calendar.getTime)
  }
}
