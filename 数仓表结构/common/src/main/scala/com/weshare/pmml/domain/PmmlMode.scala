package com.weshare.pmml.domain

/**
 * created by chao.guo on 2021/2/26
 **/
case class PmmlMode(
                     var due_bill_no:String,
                     var product_id:String,
                     var risk_level:String,
                     var loan_init_principal:BigDecimal,
                     var loan_init_term:BigDecimal,
                     var loan_init_interest_rate:BigDecimal,
                     var credit_coef:BigDecimal,
                     var loan_init_penalty_rate:BigDecimal,
                     var loan_active_date:String,
                     var settled:String,
                     var paid_out_date:String,
                     var loan_out_reason:String,
                     var paid_out_type:String,
                     var schedule:String,
                     var Loan_status:String,
                     var paid_amount:BigDecimal,
                     var paid_principal:BigDecimal,
                     var paid_interest:BigDecimal,
                     var paid_penalty:BigDecimal,
                     var paid_svc_fee:BigDecimal,
                     var paid_term_fee:BigDecimal,
                     var paid_mult:BigDecimal,
                     var remain_amount:BigDecimal,
                     var remain_principal:BigDecimal,
                     var remain_interest:BigDecimal,
                     var remain_svc_fee:BigDecimal,
                     var remain_term_fee:BigDecimal,
                     var overdue_principal:BigDecimal,
                     var overdue_interest:BigDecimal,
                     var overdue_svc_fee:BigDecimal,
                     var overdue_term_fee:BigDecimal,
                     var overdue_penalty:BigDecimal,
                     var overdue_mult_amt:BigDecimal,
                     var overdue_date_first:String,
                     var overdue_date_start:String,
                     var overdue_days:BigDecimal,
                     var overdue_date:String,
                     var dpd_begin_date:String,
                     var dpd_days:BigDecimal,
                     var dpd_days_count:BigDecimal,
                     var dpd_days_max:BigDecimal,
                     var collect_out_date:String,
                     var overdue_term:BigDecimal,
                     var overdue_terms_count:BigDecimal,
                     var overdue_terms_max:BigDecimal,
                     var overdue_principal_accumulate:BigDecimal,
                     var overdue_principal_max:BigDecimal,
                     var biz_date:String,
                     var creditlimit:BigDecimal,
                     var edu:String,
                     var degree:String,
                     var homests:String,
                     var marriage:String,
                     var mincome:BigDecimal,
                     var homeincome:String,
                     var zxhomeincome:BigDecimal,
                     var custtype:String,
                     var worktype:String,
                     var workduty:String,
                     var worktitle:String,
                     var idcard_area:String,
                     var risklevel:String,
                     var scorerange:String
                   )


case class Scheduler(
                      var due_bill_no:String,
                      var schedule_id:String,
                      var loan_active_date:String,
                      var loan_init_principal:BigDecimal,
                      var loan_init_term:BigDecimal,
                      var loan_term:BigDecimal,
                      var start_interest_date:String,
                      var curr_bal:String,
                      var should_repay_date:String,
                      var should_repay_date_history:String,
                      var grace_date:String,
                      var should_repay_amount:BigDecimal,
                      var should_repay_principal:BigDecimal,
                      var should_repay_interest:BigDecimal,
                      var should_repay_term_fee:BigDecimal,
                      var should_repay_svc_fee:BigDecimal,
                      var should_repay_penalty:BigDecimal,
                      var should_repay_mult_amt:BigDecimal,
                      var should_repay_penalty_acru:BigDecimal,
                      var schedule_status:String,
                      var schedule_status_cn:String,
                      var paid_out_date:String,
                      var paid_out_type:String,
                      var paid_out_type_cn:String,
                      var paid_amount:BigDecimal,
                      var paid_principal:BigDecimal,
                      var paid_interest:BigDecimal,
                      var paid_term_fee:BigDecimal,
                      var paid_svc_fee:BigDecimal,
                      var paid_penalty:BigDecimal,
                      var paid_mult:BigDecimal,
                      var reduce_amount:BigDecimal,
                      var reduce_principal:BigDecimal,
                      var reduce_interest:BigDecimal,
                      var reduce_term_fee:BigDecimal,
                      var reduce_svc_fee:BigDecimal,
                      var reduce_penalty:BigDecimal,
                      var reduce_mult_amt:BigDecimal,
                      var s_d_date:String,
                      var e_d_date:String,
                      var effective_time:String,
                      var expire_time:String,
                      var product_id_vt :String
                    )
