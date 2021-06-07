package com.weshare.pmml.app

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.weshare.pmml.domain.PmmlParam
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.DateUtils
import org.slf4j.{Logger, LoggerFactory}

/**
 * created by chao.guo on 2021/4/1
 **/
object PmmlParamDeal {
private val logger: Logger = LoggerFactory.getLogger(PmmlParamDeal.getClass)

  /***
   * 判断目前阶段属于该项目的哪个阶段
   * 1 循环放款期 预测第0次数据 并模拟生成借据
   * 2 处于摊还期  仅预测还款  不模拟生成借据
   */
  val is_cycle_Stage: (PmmlParam, String) => Boolean = (param:PmmlParam, batch_date:String) =>{
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val current_date = format.parse(batch_date)
 /*   val cycle_start_date = format.parse(param.cycle_start_date)
    val cycle_end_date = format.parse(param.cycle_end_date)*/
    val amortization_period_start_date = format.parse(param.amortization_period_start_date)
    val amortization_period_end_date = format.parse(param.amortization_period_end_date)
    var flag: Boolean = true
    if(current_date.getTime >=amortization_period_start_date.getTime
      && current_date.getTime <=amortization_period_end_date.getTime
      || StringUtils.equals(param.amortization_period_start_date,param.amortization_period_end_date)
    ){ // 摊还期内可不进行造数
         flag=false
    }
    flag
  }

  /**
   * 计算可放款期次
   */
  val cycle_loan_terms: (PmmlParam, String) => List[Int] = (param:PmmlParam, currentmonth:String)=>{
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val current_date = format.parse(currentmonth)
    //DateUtils.truncate(current_date, Calendar.MONTH)
    val cal = Calendar.getInstance()
    cal.setTime(DateUtils.truncate(current_date, Calendar.MONTH))
    val cycle_end_date = format.parse(param.project_end_date)
    val cal2 = Calendar.getInstance()
    cal2.setTime(cycle_end_date)
    //cal2.
    val diffMonth=cal2.get(Calendar.MONTH)-cal.get(Calendar.MONTH)
    val diffYear=cal2.get(Calendar.YEAR)-cal.get(Calendar.YEAR)
    val diffTerm = diffYear*12+diffMonth
    val loan_term=param.loan_terms.split(",").map(it=>it.toInt).filter(it=>it<=diffTerm)
    logger.warn("batch_date:{},可用期数:{},项目结束日期:{},月份差值:{}",currentmonth,loan_term,param.project_end_date,diffTerm.toString)
    loan_term.toList
  }

  // 计算循环期次
  val cycle_diff_terms: (PmmlParam) => Int = (param:PmmlParam)=>{
   // 摊还期结束日期 -当前批次日期  计算循环次数
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val current_date = format.parse(param.batch_date)
    val cal = Calendar.getInstance()
    cal.setTime(current_date)
    val cycle_end_date = format.parse(param.cycle_end_date)
    val cal2 = Calendar.getInstance()
    cal2.setTime(cycle_end_date)
    val diffMonth=cal2.get(Calendar.MONTH)-cal.get(Calendar.MONTH)
    val diffYear=cal2.get(Calendar.YEAR)-cal.get(Calendar.YEAR)
    var between_month = diffYear * 12 + diffMonth
   //判断当前日期是否不是月末
    val current_month = Calendar.getInstance()
    current_month.setTime(current_date)
    current_month.add(Calendar.MONTH,1)
    current_month.set(Calendar.DAY_OF_MONTH,1)
    current_month.add(Calendar.DAY_OF_MONTH,-1)
    println(format.format(current_month.getTime))
    //如果当前日期小于月末日期3天以内不进行加
     // 且摊还期开始日期不等于结束日期
    if(!StringUtils.equals(param.cycle_start_date,param.cycle_end_date)&&  (cal.get(Calendar.DAY_OF_YEAR)-current_month.get(Calendar.DAY_OF_YEAR))< -5){
      between_month=between_month+1
    }
    logger.error("可用于循环放款的期次为:{}",between_month)
    //if()
    between_month
  }

  /**
   * 判断当前批次日期是否远远小于月末  当远远小于月末时 先模拟本月批次日期之后的日期放款
   */
  val isCurrentMonth=(batch_date:String,start_count:Int)=>{
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val current_date = format.parse(batch_date)
    val cal = Calendar.getInstance()
    cal.setTime(current_date)
    val current_month = Calendar.getInstance()
    current_month.setTime(current_date)
    current_month.add(Calendar.MONTH,1)
    current_month.set(Calendar.DAY_OF_MONTH,1)
    current_month.add(Calendar.DAY_OF_MONTH,-1)
    println(format.format(current_month.getTime))
    //如果当前日期小于月末日期3天以内不进行加
    // 且摊还期开始日期不等于结束日期
    if((cal.get(Calendar.DAY_OF_YEAR)-current_month.get(Calendar.DAY_OF_YEAR)) > -3){
      start_count
    }else{
      start_count+1
    }
  }
}
