package com.weshare.pmml.app

import com.alibaba.fastjson.{JSON, JSONObject}
import org.json4s.jackson.Json

import scala.util.Random


/**
 * created by chao.guo on 2021/3/5
 **/
object Test {
  def main(args: Array[String]): Unit = {
while (true) {
println(Math.abs(Random.nextInt(4)))
}

  }
    /*val fields = classOf[Scheduler_info].getDeclaredFields
    var list =List[String]()
    fields.foreach(it=>list="\""+it.getName+"\""::list)
    println(list.reverse.mkString(","))
    println(list.reverse)*/
/*val schedule_str="{\"5\":{\"schedule_id\":\"000015916124511admin000082000074\",\"due_bill_no\":\"1120060711432704977954\",\"loan_active_date\":\"2020-06-07\",\"loan_init_principal\":\"10\",\"loan_init_term\":\"6\",\"loan_term\":\"5\",\"start_interest_date\":\"2020-10-06\",\"curr_bal\":\"1.85\",\"should_repay_date\":\"2020-11-06\",\"grace_date\":\"2020-11-09\",\"should_repay_amount\":\"1.85\",\"should_repay_principal\":\"1.74\",\"should_repay_interest\":\"0.11\",\"should_repay_term_fee\":\"0\",\"should_repay_svc_fee\":\"0\",\"should_repay_penalty\":\"0\",\"should_repay_mult_amt\":\"0\",\"should_repay_penalty_acru\":\"0\",\"schedule_status\":\"N\",\"schedule_status_cn\":\"正常\",\"paid_amount\":\"0\",\"paid_principal\":\"0\",\"paid_interest\":\"0\",\"paid_term_fee\":\"0\",\"paid_svc_fee\":\"0\",\"paid_penalty\":\"0\",\"paid_mult\":\"0\",\"reduce_amount\":\"0\",\"reduce_principal\":\"0\",\"reduce_interest\":\"0\",\"reduce_term_fee\":\"0\",\"reduce_svc_fee\":\"0\",\"reduce_penalty\":\"0\",\"reduce_mult_amt\":\"0\",\"s_d_date\":\"2020-06-07\",\"e_d_date\":\"2020-11-06\",\"effective_time\":\"2020-06-08 18:34:12\",\"expire_time\":\"2020-11-07 11:35:01\"},\"6\":{\"schedule_id\":\"000015916124511admin000082000082\",\"due_bill_no\":\"1120060711432704977954\",\"loan_active_date\":\"2020-06-07\",\"loan_init_principal\":\"10\",\"loan_init_term\":\"6\",\"loan_term\":\"6\",\"start_interest_date\":\"2020-11-06\",\"curr_bal\":\"1.85\",\"should_repay_date\":\"2020-12-06\",\"grace_date\":\"2020-12-09\",\"should_repay_amount\":\"1.85\",\"should_repay_principal\":\"1.77\",\"should_repay_interest\":\"0.08\",\"should_repay_term_fee\":\"0\",\"should_repay_svc_fee\":\"0\",\"should_repay_penalty\":\"0\",\"should_repay_mult_amt\":\"0\",\"should_repay_penalty_acru\":\"0\",\"schedule_status\":\"N\",\"schedule_status_cn\":\"正常\",\"paid_amount\":\"0\",\"paid_principal\":\"0\",\"paid_interest\":\"0\",\"paid_term_fee\":\"0\",\"paid_svc_fee\":\"0\",\"paid_penalty\":\"0\",\"paid_mult\":\"0\",\"reduce_amount\":\"0\",\"reduce_principal\":\"0\",\"reduce_interest\":\"0\",\"reduce_term_fee\":\"0\",\"reduce_svc_fee\":\"0\",\"reduce_penalty\":\"0\",\"reduce_mult_amt\":\"0\",\"s_d_date\":\"2020-06-07\",\"e_d_date\":\"2020-12-06\",\"effective_time\":\"2020-06-08 18:34:12\",\"expire_time\":\"2020-12-07 11:41:45\"},\"3\":{\"schedule_id\":\"000015916124511admin000082000056\",\"due_bill_no\":\"1120060711432704977954\",\"loan_active_date\":\"2020-06-07\",\"loan_init_principal\":\"10\",\"loan_init_term\":\"6\",\"loan_term\":\"3\",\"start_interest_date\":\"2020-08-06\",\"curr_bal\":\"0\",\"should_repay_date\":\"2020-09-06\",\"grace_date\":\"2020-09-09\",\"should_repay_amount\":\"1.85\",\"should_repay_principal\":\"1.65\",\"should_repay_interest\":\"0.2\",\"should_repay_term_fee\":\"0\",\"should_repay_svc_fee\":\"0\",\"should_repay_penalty\":\"0\",\"should_repay_mult_amt\":\"0\",\"should_repay_penalty_acru\":\"0\",\"schedule_status\":\"F\",\"schedule_status_cn\":\"已还清\",\"paid_out_date\":\"2020-09-06\",\"paid_out_type\":\"NORMAL_SETTLE\",\"paid_out_type_cn\":\"正常结清\",\"paid_amount\":\"1.85\",\"paid_principal\":\"1.65\",\"paid_interest\":\"0.2\",\"paid_term_fee\":\"0\",\"paid_svc_fee\":\"0\",\"paid_penalty\":\"0\",\"paid_mult\":\"0\",\"reduce_amount\":\"0\",\"reduce_principal\":\"0\",\"reduce_interest\":\"0\",\"reduce_term_fee\":\"0\",\"reduce_svc_fee\":\"0\",\"reduce_penalty\":\"0\",\"reduce_mult_amt\":\"0\",\"s_d_date\":\"2020-09-06\",\"e_d_date\":\"3000-12-31\",\"effective_time\":\"2020-09-07 11:30:04\",\"expire_time\":\"3000-12-31 00:00:00\"},\"2\":{\"schedule_id\":\"000015916124511admin000082000048\",\"due_bill_no\":\"1120060711432704977954\",\"loan_active_date\":\"2020-06-07\",\"loan_init_principal\":\"10\",\"loan_init_term\":\"6\",\"loan_term\":\"2\",\"start_interest_date\":\"2020-07-06\",\"curr_bal\":\"0\",\"should_repay_date\":\"2020-08-06\",\"grace_date\":\"2020-08-09\",\"should_repay_amount\":\"1.85\",\"should_repay_principal\":\"1.6\",\"should_repay_interest\":\"0.25\",\"should_repay_term_fee\":\"0\",\"should_repay_svc_fee\":\"0\",\"should_repay_penalty\":\"0\",\"should_repay_mult_amt\":\"0\",\"should_repay_penalty_acru\":\"0\",\"schedule_status\":\"F\",\"schedule_status_cn\":\"已还清\",\"paid_out_date\":\"2020-08-02\",\"paid_out_type\":\"PRE_SETTLE\",\"paid_out_type_cn\":\"提前结清\",\"paid_amount\":\"1.85\",\"paid_principal\":\"1.6\",\"paid_interest\":\"0.25\",\"paid_term_fee\":\"0\",\"paid_svc_fee\":\"0\",\"paid_penalty\":\"0\",\"paid_mult\":\"0\",\"reduce_amount\":\"0\",\"reduce_principal\":\"0\",\"reduce_interest\":\"0\",\"reduce_term_fee\":\"0\",\"reduce_svc_fee\":\"0\",\"reduce_penalty\":\"0\",\"reduce_mult_amt\":\"0\",\"s_d_date\":\"2020-08-02\",\"e_d_date\":\"3000-12-31\",\"effective_time\":\"2020-08-03 11:53:59\",\"expire_time\":\"3000-12-31 00:00:00\"},\"1\":{\"schedule_id\":\"000015916124511admin000082000039\",\"due_bill_no\":\"1120060711432704977954\",\"loan_active_date\":\"2020-06-07\",\"loan_init_principal\":\"10\",\"loan_init_term\":\"6\",\"loan_term\":\"1\",\"start_interest_date\":\"2020-06-07\",\"curr_bal\":\"0\",\"should_repay_date\":\"2020-07-06\",\"grace_date\":\"2020-07-09\",\"should_repay_amount\":\"1.84\",\"should_repay_principal\":\"1.55\",\"should_repay_interest\":\"0.29\",\"should_repay_term_fee\":\"0\",\"should_repay_svc_fee\":\"0\",\"should_repay_penalty\":\"0\",\"should_repay_mult_amt\":\"0\",\"should_repay_penalty_acru\":\"0\",\"schedule_status\":\"F\",\"schedule_status_cn\":\"已还清\",\"paid_out_date\":\"2020-07-04\",\"paid_out_type\":\"PRE_SETTLE\",\"paid_out_type_cn\":\"提前结清\",\"paid_amount\":\"1.84\",\"paid_principal\":\"1.55\",\"paid_interest\":\"0.29\",\"paid_term_fee\":\"0\",\"paid_svc_fee\":\"0\",\"paid_penalty\":\"0\",\"paid_mult\":\"0\",\"reduce_amount\":\"0\",\"reduce_principal\":\"0\",\"reduce_interest\":\"0\",\"reduce_term_fee\":\"0\",\"reduce_svc_fee\":\"0\",\"reduce_penalty\":\"0\",\"reduce_mult_amt\":\"0\",\"s_d_date\":\"2020-07-04\",\"e_d_date\":\"3000-12-31\",\"effective_time\":\"2020-07-05 22:24:07\",\"expire_time\":\"3000-12-31 00:00:00\"},\"4\":{\"schedule_id\":\"000015916124511admin000082000065\",\"due_bill_no\":\"1120060711432704977954\",\"loan_active_date\":\"2020-06-07\",\"loan_init_principal\":\"10\",\"loan_init_term\":\"6\",\"loan_term\":\"4\",\"start_interest_date\":\"2020-09-06\",\"curr_bal\":\"0\",\"should_repay_date\":\"2020-10-06\",\"grace_date\":\"2020-10-09\",\"should_repay_amount\":\"1.85\",\"should_repay_principal\":\"1.69\",\"should_repay_interest\":\"0.16\",\"should_repay_term_fee\":\"0\",\"should_repay_svc_fee\":\"0\",\"should_repay_penalty\":\"0\",\"should_repay_mult_amt\":\"0\",\"should_repay_penalty_acru\":\"0\",\"schedule_status\":\"F\",\"schedule_status_cn\":\"已还清\",\"paid_out_date\":\"2020-10-06\",\"paid_out_type\":\"NORMAL_SETTLE\",\"paid_out_type_cn\":\"正常结清\",\"paid_amount\":\"1.85\",\"paid_principal\":\"1.69\",\"paid_interest\":\"0.16\",\"paid_term_fee\":\"0\",\"paid_svc_fee\":\"0\",\"paid_penalty\":\"0\",\"paid_mult\":\"0\",\"reduce_amount\":\"0\",\"reduce_principal\":\"0\",\"reduce_interest\":\"0\",\"reduce_term_fee\":\"0\",\"reduce_svc_fee\":\"0\",\"reduce_penalty\":\"0\",\"reduce_mult_amt\":\"0\",\"s_d_date\":\"2020-10-06\",\"e_d_date\":\"3000-12-31\",\"effective_time\":\"2020-10-07 11:51:24\",\"expire_time\":\"3000-12-31 00:00:00\"}}"
    val stringToStrings = changeJsonToMap(schedule_str)
    val value = changeListMapToJson(stringToStrings)
    println(value)



  }
  def changeJsonToMap(schedules:String) ={
    val array: JSONObject = JSON.parseObject(schedules)
    var result = List[Map[String, String]]()

    for (elem <- 1.to(array.size())) {
      var map = Map[String, String]()
      map+=("product_id"->"001801")
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
   * 将 ListMap 转换成Json 对象
   * @param schedulers
   */
  def changeListMapToJson(schedulers:List[Map[String,String]]) ={
    var result = Map[String, Map[String,String]]()
    schedulers.foreach(it=>{
      val loan_term = it.getOrElse("loan_term","")
      result +=(loan_term->it)
    })
    import org.json4s.DefaultFormats
    Json(DefaultFormats).write(result)
  }*/

}
