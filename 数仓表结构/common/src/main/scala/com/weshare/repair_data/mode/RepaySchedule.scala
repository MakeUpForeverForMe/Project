package com.weshare.repair_data.mode

/**
 * created by chao.guo on 2020/11/26
 **/

/*
  还款计划实体
 */
case class RepaySchedule(
                         var org  :String                 ,
                         var schedule_id  :String           ,
                         var due_bill_no  :String           ,
                         var curr_bal :String               ,
                         var loan_init_prin :BigDecimal,
                         var loan_init_term  :java.lang.Integer,
                         var curr_term     :java.lang.Integer,
                         var due_term_prin :BigDecimal,
                         var due_term_int  :BigDecimal,
                         var due_term_fee  :BigDecimal,
                         var due_svc_fee  :BigDecimal,
                         var due_penalty  :BigDecimal,
                         var due_mult_amt  :BigDecimal,
                         var paid_term_pric :BigDecimal,
                         var paid_term_int :BigDecimal,
                         var paid_term_fee  :BigDecimal,
                         var paid_svc_fee   :BigDecimal,
                         var paid_penalty   :BigDecimal,
                         var paid_mult_amt  :BigDecimal,
                         var reduced_amt  :BigDecimal,
                         var reduce_term_prin   :BigDecimal,
                         var reduce_term_int :BigDecimal,
                         var reduce_term_fee:BigDecimal,
                         var reduce_svc_fee  :BigDecimal,
                         var reduce_penalty :BigDecimal,
                         var reduce_mult_amt :BigDecimal,
                         var penalty_acru  :BigDecimal,
                         var paid_out_date   :String,
                         var paid_out_type  :String  ,
                         var start_interest_date :String  ,
                         var pmt_due_date    :String    ,
                         var origin_pmt_due_date :String  ,
                         var product_code  :String      ,
                         var schedule_status :String     ,
                         var grace_date   :String     ,
                         var create_time   :Long        ,
                         var create_user  :String        ,
                         var lst_upd_time    :Long      ,
                         var lst_upd_user :String       ,
                         var jpa_version    :java.lang.Integer       ,
                         var out_side_schedule_no :String ,
                         var d_date :String  ,
                         var p_type :String
                        )



