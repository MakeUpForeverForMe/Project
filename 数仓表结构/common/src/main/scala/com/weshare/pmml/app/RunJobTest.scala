package com.weshare.pmml.app

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, JSONObject}
import com.weshare.pmml.domain.{PmmlMode, Scheduler}
import com.weshare.utils.PmmlUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession, functions}
import org.dmg.pmml.FieldName
import org.jpmml.evaluator.ModelEvaluator
import org.json4s.jackson.Json

import scala.util.Random

/**
 * created by chao.guo on 2021/3/5
 **/
object RunJobTest {

  /**
   * 针对还款计划 进行预测  将结果输出到对应的输出表
   * @param spark
   * @param batch_date
   * @param tablename
   * @param cycle_key
   */

  def runJob(spark:SparkSession, batch_date: String, tablename:String, cycle_key:String)={
    import spark.implicits._
    val ds: Dataset[PmmlMode] = spark.sql(s"select * from  ${tablename} where cycle_key='${cycle_key}' and due_bill_no='1120083016290638395224'").drop("cycle_key").as[PmmlMode]
    val setedBills = ds.filter(it=>StringUtils.equals("F",it.Loan_status)).mapPartitions(it=>{
      var modes = List[Map[String,String]]()
      while (it.hasNext) {
        val mode = it.next()
        modes=modes ++ changeJsonToMap(mode,mode.schedule)
      }
      modes.iterator
    })
    //未结清的借据
    val unSettle = ds.filter(it => {
      !StringUtils.equals("F", it.Loan_status)
    }).mapPartitions(it => {
      var modes = List[Map[String, String]]()
      val evaluator: ModelEvaluator[_] = PmmlUtils.initPmmUtils("model.pmml")
      while (it.hasNext) {
        val mode: PmmlMode = it.next()
        // 初始化信息
        val initMap: Map[String, Object] = initModeMap(mode)
        val schedules = mode.schedule
        //转换结果为schedule_list
        var schedule_list: List[Map[String, String]] = changeJsonToMap(mode,schedules)
        //while (!isHasnoDeal(schedule_list))
        schedule_list = dealScheduleList(evaluator,mode, schedule_list, initMap)
        modes = modes ++ schedule_list
        //println(schedule_list)
      }
      modes.iterator
    }).mapPartitions(it=>{
      var strings = List[String]()
      while (it.hasNext) {
        val next = it.next()
        import org.json4s.DefaultFormats
        strings=  Json(DefaultFormats).write(next)::strings
      }
      strings.iterator
    })
   unSettle.coalesce(1).write.text("file:///E:\\html\\test\\")

    /* val result = unSettle.unionByName(setedBills).mapPartitions(it => {
       var schedulers = List[com.weshare.pmml.domain.Scheduler]()
       while (it.hasNext) {
         val map = it.next()
         val scheduler: Scheduler = mapToDoMain(map)
         schedulers = scheduler :: schedulers
       }
       schedulers.iterator
     })*/

    /*spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")
    spark.sql("set hive.exec.max.created.files=100000")
    /* println("还款计划条数:",result.count())*/
    //result.show(100,false)
    result.
    withColumn("biz_date",functions.lit(batch_date))
      .withColumnRenamed("product_id_vt","product_id")
      .withColumn("cycle_key",functions.lit(cycle_key)).
    select("due_bill_no","schedule_id","loan_active_date","loan_init_principal","loan_init_term","loan_term","start_interest_date",
    "curr_bal","should_repay_date","should_repay_date_history","grace_date","should_repay_amount","should_repay_principal",
    "should_repay_interest","should_repay_term_fee","should_repay_svc_fee","should_repay_penalty","should_repay_mult_amt",
    "should_repay_penalty_acru","schedule_status","schedule_status_cn","paid_out_date","paid_out_type",
    "paid_out_type_cn","paid_amount","paid_principal","paid_interest","paid_term_fee","paid_svc_fee",
    "paid_penalty","paid_mult","reduce_amount","reduce_principal","reduce_interest","reduce_term_fee",
    "reduce_svc_fee","reduce_penalty","reduce_mult_amt","s_d_date","e_d_date","effective_time","expire_time",
    "biz_date","product_id","cycle_key"
    ).
    write.mode(SaveMode.Overwrite).insertInto("eagle.predict_schedule")*/
    // 计算实还数据
    // calRepayDay(spark,batch_date,cycle_key)
    //result.persist()
  }

  /**
   * 计算实还数据
   * @param spark
   * @param batch_date
   * @param cycle_key
   */
  def calRepayDay(spark: SparkSession, batch_date: String, cycle_key:String): Unit ={
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql("set hive.exec.max.dynamic.partitions=100000")
    spark.sql("set hive.exec.max.dynamic.partitions.pernode=100000")
    spark.sql("set hive.exec.max.created.files=100000")
    spark.sql(
      s"""
         |
         |insert overwrite table eagle.predict_repay_day partition(biz_date,product_id,cycle_key)
         |select
         |should_repay_date,
         |paid_out_date ,
         |sum(should_repay_principal) as should_repay_principal,
         |sum(should_repay_interest) as should_repay_interest,
         |sum(should_repay_amount) as should_repay_amount,
         |sum(paid_principal) as paid_principal,
         |sum(paid_interest) as paid_interest,
         |sum(paid_amount) as paid_amount,
         |"${batch_date}" as biz_date,
         |product_id as product_id,
         |'${cycle_key}' as cycle_key
         |from
         |eagle.predict_schedule
         |where cycle_key="${cycle_key}" and biz_Date="${batch_date}"
         |group by product_id,should_repay_date,paid_out_date
         |
         |""".stripMargin

    )


  }



  /**
   * map 转换为scheduler
   * @param map
   * @return
   */
  def mapToDoMain(map:Map[String,String]):Scheduler ={
    com.weshare.pmml.domain.Scheduler(
      map.getOrElse("due_bill_no",""),
      map.getOrElse("schedule_id",""),
      map.getOrElse("loan_active_date",""),
      BigDecimal.apply(map.getOrElse("loan_init_principal","")),
      BigDecimal.apply(map.getOrElse("loan_init_term","")),
      BigDecimal.apply(map.getOrElse("loan_term","")),
      map.getOrElse("start_interest_date",""),
      map.getOrElse("curr_bal",""),
      map.getOrElse("should_repay_date",""),
      map.getOrElse("should_repay_date_history",""),
      map.getOrElse("grace_date",""),
      BigDecimal.apply(map.getOrElse("should_repay_amount","0")),
      BigDecimal.apply(map.getOrElse("should_repay_principal","0")),
      BigDecimal.apply(map.getOrElse("should_repay_interest","0")),
      BigDecimal.apply(map.getOrElse("should_repay_term_fee","0")),
      BigDecimal.apply(map.getOrElse("should_repay_svc_fee","0")),
      BigDecimal.apply(map.getOrElse("should_repay_penalty","0")),
      BigDecimal.apply(map.getOrElse("should_repay_mult_amt","0")),
      BigDecimal.apply(map.getOrElse("should_repay_penalty_acru","0")),
      map.getOrElse("schedule_status",""),
      map.getOrElse("schedule_status_cn",""),
      map.getOrElse("paid_out_date",""),
      map.getOrElse("paid_out_type",""),
      map.getOrElse("paid_out_type_cn",""),
      BigDecimal.apply(map.getOrElse("paid_amount","0")),
      BigDecimal.apply(map.getOrElse("paid_principal","0")),
      BigDecimal.apply(map.getOrElse("paid_interest","0")),
      BigDecimal.apply(map.getOrElse("paid_term_fee","0")),
      BigDecimal.apply(map.getOrElse("paid_svc_fee","0")),
      BigDecimal.apply(map.getOrElse("paid_penalty","0")),
      BigDecimal.apply(map.getOrElse("paid_mult","0")),
      BigDecimal.apply(map.getOrElse("reduce_amount","0")),
      BigDecimal.apply(map.getOrElse("reduce_principal","0")),
      BigDecimal.apply(map.getOrElse("reduce_interest","0")),
      BigDecimal.apply(map.getOrElse("reduce_term_fee","0")),
      BigDecimal.apply(map.getOrElse("reduce_svc_fee","0")),
      BigDecimal.apply(map.getOrElse("reduce_penalty","0")),
      BigDecimal.apply(map.getOrElse("reduce_mult_amt","0")),
      map.getOrElse("s_d_date",""),
      map.getOrElse("e_d_date",""),
      map.getOrElse("effective_time",""),
      map.getOrElse("expire_time",""),
      map.getOrElse("product_id","")
    )




  }


  /**
   * 判断 改还款计划list 是否还存在未处理的还款计划
   * @param schedule_list
   * @return
   */
  def isHasnoDeal(schedule_list:List[Map[String,String]])={
    var paid_term=true;
    schedule_list.foreach(it=>{
      if(StringUtils.equals("N",it.getOrElse("schedule_status",""))){
        paid_term=false
      }
    })
    paid_term

  }

  /**
   * 计算改还款计划list 判断该笔借据是否是一次 未还 一次未还则 该笔借据的所有还款计划状态为N
   * @param schedule_list
   * @return
   */
  def caclPaid_term(schedule_list:List[Map[String,String]])={
    var paid_term=0;
    schedule_list.foreach(it=>{
      if(StringUtils.equals("N",it.getOrElse("schedule_status",""))){
        paid_term+=1
      }
    })
    paid_term

  }

  /**
   * 对 还款计划List进行转换
   * 调用evaluator.evaluate() 方法 得到风控模型返回的概率  入参为整条记录借据上的值 + （还款计划已还期数最大一期 or )
   * 未还期数第一期
   *
   * @param evaluator
   * @param mode
   * @param schedule_list
   * @param initMap
   * @return
   */
  def dealScheduleList(evaluator: ModelEvaluator[_],mode:PmmlMode,schedule_list:List[Map[String,String]], initMap: Map[String, Object]) ={
    var temp: List[Map[String, String]] = List[Map[String,String]]()
    if(null!=schedule_list&&schedule_list.nonEmpty){
      var term_map =Map[String,Object]()
      term_map = getMinTerm(schedule_list)
      term_map+=("previous_status"->"N")
      term_map+=("paid_out_date"->null)
      term_map+=("loan_term"->"1")
      term_map+=("paid_principal"->"0")
      term_map+=("paid_interest"->"0")
      term_map+=("schedule_status"->"N")

      //如果该笔借据一期都没有还 则加第一期进入
     /* if(caclPaid_term(schedule_list).compare(mode.loan_init_term.toInt)==0 ){
        term_map = getMinTerm(schedule_list)
        term_map+=("previous_status"->"N")
      }else{
        term_map = getMaxPaidOutTerm(schedule_list)
        term_map+=("previous_status"->term_map.getOrElse("schedule_status","N"))
      }*/
      val mincome= if(mode.mincome.compare(BigDecimal.valueOf(0))==0) "1000" else {mode.mincome.toString()}
      term_map+=("mincome"->mincome)
      term_map+=("creditlimit"->mode.creditlimit)
      term_map+=("scoreRange"->mode.scorerange)
      term_map+=("riskLevel"->mode.risklevel)
      val paramMap = term_map++initMap
      //println(paramMap)
      import scala.collection.JavaConversions._
      import org.json4s.DefaultFormats
      println(Json(DefaultFormats).write(paramMap))
      var map = evaluator.evaluate(changeMapKey(paramMap)).filter(it => {
        !StringUtils.equals("target", it._1.getValue)
      })
        .map(it => (it._1.getValue, it._2.toString)).toMap
      map+=("due_bill_no"->paramMap.getOrElse("due_bill_no","").toString)
      map+=("loan_term"->paramMap.getOrElse("loan_term","").toString)
      map+=("schedule_status"->paramMap.getOrElse("schedule_status","").toString)
      map+=("paid_out_Date"->paramMap.getOrElse("paid_out_Date","").toString)
      temp=map::temp
      //修改对应期数的值
      //判断是否只有一期
      /*val resultMap = getMaxTarget(map) // 预测完的结果
      if(mode.loan_init_term==1 ){ // 如果只有一期
        val schedule = schedule_list(0)
        if(!StringUtils.equals("F",schedule.getOrElse("schedule_status","N"))){ // 仅有一期 且未还
          temp =updateSchedule(schedule,resultMap)::Nil
        }
      }
      else { //大于1期 则取当前期数的下一期
        schedule_list.foreach(it=>{
          val next_term = (Integer.valueOf(term_map.getOrElse("loan_term","").toString)+1).toString
          if(it.getOrElse("loan_term","").equals(next_term)){ //等于当前预测期数的下一期
            temp= updateSchedule(it,resultMap)::temp // 改变下一期的值
          }else if (StringUtils.equals(it.getOrElse("loan_term",""),term_map.getOrElse("loan_term","").toString) && StringUtils.equals("N",it.getOrElse("schedule_status","N"))){
            // 保持原来的还款计划
            temp= updateSchedule(it,resultMap)::temp

          }else{
            temp= it::temp
          }
        })
      }*/
    }
    temp
  }






  /**
   * 根据概率计算 本期还款计划的结清日期 金额等
   * @param schedule
   * @param result
   * @return
   */
  def updateSchedule(schedule:Map[String,String],result:(String,String)) ={
    var res = Map[String,String]()
    result._1 match {
      case "N" => // 正常还款
        res= Map[String,String]("schedule_status"->"F",
          "paid_out_date"->schedule.getOrElse("should_repay_date",""),
          "paid_principal"->schedule.getOrElse("should_repay_principal",""),
          "paid_interest"->schedule.getOrElse("should_repay_interest",""),
          "paid_amount"->schedule.getOrElse("should_repay_amount",""),

          "paid_out_type"->"NORMAL_SETTLE",
          "schedule_status_cn"->"已还清",
          "paid_out_type_cn"->"正常结清"
        );

      case "F" =>{  //提前还款  应还款日 - 1到 15之间的一个随机数 做paid_out_date
        res= Map[String,String]("schedule_status"->"F",
          "paid_out_date"-> clcalDate(schedule.getOrElse("should_repay_date",""),-Random.nextInt(16)),
          "paid_principal"->schedule.getOrElse("should_repay_principal",""),
          "paid_interest"->schedule.getOrElse("should_repay_interest",""),
          "paid_amount"->schedule.getOrElse("should_repay_amount",""),
          "paid_out_type"->"PRE_SETTLE",
          "schedule_status_cn"->"已还清",
          "paid_out_type_cn"->"提前结清"
        )
      }

      case "O" =>{ // 逾期
        res= Map[String,String]("schedule_status"->"O"
          ,"schedule_status_cn"->"逾期"
        )

      }
      case "Ov" => {// 逾期还款  应还款日加上一个1到15 的随机数
        res= Map[String,String]("schedule_status"->"F",
          "paid_out_date"-> clcalDate(schedule.getOrElse("should_repay_date",""),Random.nextInt(16)),
          "paid_principal"->schedule.getOrElse("should_repay_principal",""),
          "paid_interest"->schedule.getOrElse("should_repay_interest",""),
          "paid_amount"->schedule.getOrElse("should_repay_amount",""),
          "paid_out_type"->"OVERDUE_SETTLE",
          "schedule_status_cn"->"已还清",
          "paid_out_type_cn"->"逾期结清"
        )
      }
    }
    res=schedule++res
    res
  }

  /**
   * 计算日期
   * @param string
   * @param day
   * @return
   */
  def clcalDate(string: String,day:Int):String={
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val date = format.parse(string)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_YEAR, day)
    val lastdate = calendar.getTime
    format.format(lastdate)
  }
  /**
   * 获取预测完风控结果返回 结果中的最大值
   * @param map
   * @return
   */
  def getMaxTarget(map:Map[String, String]) ={
    val tuple = map.map(it => {
      it._1.replaceAll("probability\\(", "").replaceAll("\\)", "") match {
        case "0" => ("N", it._2)
        case "1" => ("F", it._2)
        case "2" => ("O", it._2)
        case "3" => ("Ov", it._2)
      }
    }).maxBy(it => it._2)
    tuple

  }



  /**
   * 输入参数类型 要求Map的key 为 FieldName
   * @param map
   * @return
   */
  def changeMapKey(map:Map[String,Object]) ={
    map.map(it=>{
      val name: FieldName = FieldName.create(it._1)
      (name,
        if(null==it._2)"" else it._2.toString
      )

    })
  }

  /**
   * 将一条记录 转换成Map
   *
   * @param mode
   * @return
   */
  def initModeMap(mode: PmmlMode) ={
    val initMap = Map[String, Object](
      "due_bill_no" -> mode.due_bill_no,
      "product_id" -> mode.product_id,
      "loan_init_principal" -> mode.loan_init_principal,
      "loan_init_term" -> mode.loan_init_term,
      "loan_init_interest_rate" -> mode.loan_init_interest_rate,
      "credit_coef" -> mode.credit_coef,
      "loan_init_penalty_rate" -> mode.loan_init_penalty_rate,
      "loan_active_date" -> mode.loan_active_date,
      "settled" -> mode.settled,
      "loan_out_reason" -> mode.loan_out_reason,
      "paid_out_type" -> mode.paid_out_type,
      //"schedule" -> mode.schedule,
      "Loan_status" -> mode.Loan_status,
      "paid_amount" -> mode.paid_amount,
      "paid_principal" -> mode.paid_principal,
      "paid_interest" -> mode.paid_interest,
      "paid_penalty" -> mode.paid_penalty,
      "paid_svc_fee" -> mode.paid_svc_fee,
      "paid_term_fee" -> mode.paid_term_fee,
      "paid_mult" -> mode.paid_mult,
      "remain_amount" -> mode.remain_amount,
      "remain_principal" -> mode.remain_principal,
      "remain_interest" -> mode.remain_interest,
      "remain_svc_fee" -> mode.remain_svc_fee,
      "remain_term_fee" -> mode.remain_term_fee,
      "overdue_principal" -> mode.overdue_principal,
      "overdue_interest" -> mode.overdue_interest,
      "overdue_svc_fee" -> mode.overdue_svc_fee,
      "overdue_term_fee" -> mode.overdue_term_fee,
      "overdue_penalty" -> mode.overdue_penalty,
      "overdue_mult_amt" -> mode.overdue_mult_amt,
      "overdue_date_first" -> mode.overdue_date_first,
      "overdue_date_start" -> mode.overdue_date_start,
      "overdue_days" -> mode.overdue_days,
      "overdue_date" -> mode.overdue_date,
      "dpd_begin_date" -> mode.dpd_begin_date,
      "dpd_days" -> mode.dpd_days,
      "dpd_days_count" -> mode.dpd_days_count,
      "dpd_days_max" -> mode.dpd_days_max,
      "collect_out_date" -> mode.collect_out_date,
      "overdue_term" -> mode.overdue_term,
      "overdue_terms_count" -> mode.overdue_terms_count,
      "overdue_terms_max" -> mode.overdue_terms_max,
      "overdue_principal_accumulate" -> mode.overdue_principal_accumulate,
      "biz_date" -> mode.biz_date,
      "edu" -> mode.edu,
      "degree" -> mode.degree,
      "homests" -> mode.homests,
      "marriage" -> mode.marriage,
      "homeincome" -> mode.homeincome,
      "zxhomeincome" -> mode.zxhomeincome,
      "custtype" -> mode.custtype,
      "worktype" -> mode.worktype,
      "workduty" -> mode.workduty,
      "worktitle" -> mode.worktitle,
      "idcard_area" -> mode.idcard_area
    )


    initMap




  }

  /**
   * 解析json 找出已还期数最大的一期返回
   * @param schedules
   */
  def changeJsonToMap(mode:PmmlMode,schedules:String) ={
    val array: JSONObject = JSON.parseObject(schedules)
    var result = List[Map[String, String]]()

    for (elem <- 1.to(array.size())) {
      var map = Map[String, String]()
      map+=("product_id"->mode.product_id)
      val json = array.getJSONObject(elem.toString)
      val keys = json.keySet().iterator()
      while (keys.hasNext){
        val key = keys.next()
        val value = json.getString(key)
        map+=(key->value)
      }
      result=map::result
    }
    result
  }

  /**
   * 获取还款计划的最大期数
   * @param schedules
   */
  def getMaxPaidOutTerm(schedules:List[Map[String, String]]) ={
    /* if( schedules.filter(it => StringUtils.equals("F",it.getOrElse("schedule_status", "")))==null || schedules.filter(it => StringUtils.equals("F",it.getOrElse("schedule_status", ""))).isEmpty){
       println(schedules)
     }*/
    /* val stringToStrings = schedules.filter(it => {StringUtils.equals("F",it.getOrElse("schedule_status", "")) || StringUtils.equals("O",it.getOrElse("schedule_status", ""))})
     println(stringToStrings)*/
    schedules.filter(it => {StringUtils.equals("F",it.getOrElse("schedule_status", "")) || StringUtils.equals("O",it.getOrElse("schedule_status", ""))}).maxBy(it => it.getOrElse("loan_term", "").toInt)

  }
  /**
   * 获取还款计划的最大期数
   * @param schedules
   */
  def getMinTerm(schedules:List[Map[String, String]]) ={
    schedules.minBy(it => it.getOrElse("loan_term", ""))
  }




}
