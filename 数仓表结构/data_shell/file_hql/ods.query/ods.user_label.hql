-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


insert overwrite table ods.user_label
select
  b.user_hash_no                                      as user_hash_no,
  a.project_name                                      as project_name,
  a.project_id                                        as project_id,
  a.order_id                                          as order_id,
  a.pro_code                                          as pro_code,
  a.ret_msg                                           as ret_msg,
  a.apply_by_id_last_year_fit                         as apply_by_id_last_year_fit,
  a.apply_by_id_last_year                             as apply_by_id_last_year,
  a.apps_of_con_fin_pro_last_two_year_fit             as apps_of_con_fin_pro_last_two_year_fit,
  a.apps_of_con_fin_pro_last_two_year                 as apps_of_con_fin_pro_last_two_year,
  a.usage_rate_of_compus_loan_app_fit                 as usage_rate_of_compus_loan_app_fit,
  a.usage_rate_of_compus_loan_app                     as usage_rate_of_compus_loan_app,
  a.apps_of_con_fin_org_last_two_year_fit             as apps_of_con_fin_org_last_two_year_fit,
  a.apps_of_con_fin_org_last_two_year                 as apps_of_con_fin_org_last_two_year,
  a.reject_times_of_credit_apply_last4_m_fit          as reject_times_of_credit_apply_last4_m_fit,
  a.reject_times_of_credit_apply_last4_m              as reject_times_of_credit_apply_last4_m,
  a.overdue_times_of_credit_apply_last4_m_fit         as overdue_times_of_credit_apply_last4_m_fit,
  a.overdue_times_of_credit_apply_last4_m             as overdue_times_of_credit_apply_last4_m,
  a.sum_credit_of_web_loan_fit                        as sum_credit_of_web_loan_fit,
  a.sum_credit_of_web_loan                            as sum_credit_of_web_loan,
  a.count_of_register_apps_of_fin_last_m_fit          as count_of_register_apps_of_fin_last_m_fit,
  a.count_of_register_apps_of_fin_last_m              as count_of_register_apps_of_fin_last_m,
  a.count_of_uninstall_apps_of_loan_last3_m_fit       as count_of_uninstall_apps_of_loan_last3_m_fit,
  a.count_of_uninstall_apps_of_loan_last3_m           as count_of_uninstall_apps_of_loan_last3_m,
  a.days_of_location_upload_lats9_m_fit               as days_of_location_upload_lats9_m_fit,
  a.days_of_location_upload_lats9_m                   as days_of_location_upload_lats9_m,
  a.account_for_in_town_in_work_day_last_m_fit        as account_for_in_town_in_work_day_last_m_fit,
  a.account_for_in_town_in_work_day_last_m            as account_for_in_town_in_work_day_last_m,
  a.count_of_installition_of_loan_app_last2_m_fit     as count_of_installition_of_loan_app_last2_m_fit,
  a.count_of_installition_of_loan_app_last2_m         as count_of_installition_of_loan_app_last2_m,
  a.risk_of_device_fit                                as risk_of_device_fit,
  a.risk_of_device                                    as risk_of_device,
  a.account_for_wifi_use_time_span_last5_m_fit        as account_for_wifi_use_time_span_last5_m_fit,
  a.account_for_wifi_use_time_span_last5_m            as account_for_wifi_use_time_span_last5_m,
  a.count_of_notices_of_fin_message_last9_m_fit       as count_of_notices_of_fin_message_last9_m_fit,
  a.count_of_notices_of_fin_message_last9_m           as count_of_notices_of_fin_message_last9_m,
  a.days_of_earliest_register_of_loan_app_last9_m_fit as days_of_earliest_register_of_loan_app_last9_m_fit,
  a.days_of_earliest_register_of_loan_app_last9_m     as days_of_earliest_register_of_loan_app_last9_m,
  a.loan_amt_last3_m_fit                              as loan_amt_last3_m_fit,
  a.loan_amt_last3_m                                  as loan_amt_last3_m,
  a.overdue_loans_of_more_than1_day_last6_m_fit       as overdue_loans_of_more_than1_day_last6_m_fit,
  a.overdue_loans_of_more_than1_day_last6_m           as overdue_loans_of_more_than1_day_last6_m,
  a.amt_of_perfermance_loans_last3_m_fit              as amt_of_perfermance_loans_last3_m_fit,
  a.amt_of_perfermance_loans_last3_m                  as amt_of_perfermance_loans_last3_m,
  a.last_financial_query_fit                          as last_financial_query_fit,
  a.last_financial_query                              as last_financial_query,
  a.average_daily_open_times_of_fin_apps_last_m_fit   as average_daily_open_times_of_fin_apps_last_m_fit,
  a.average_daily_open_times_of_fin_apps_last_m       as average_daily_open_times_of_fin_apps_last_m,
  a.times_of_uninstall_fin_apps_last15_d_fit          as times_of_uninstall_fin_apps_last15_d_fit,
  a.times_of_uninstall_fin_apps_last15_d              as times_of_uninstall_fin_apps_last15_d,
  a.account_for_install_bussiness_apps_last4_m_fit    as account_for_install_bussiness_apps_last4_m_fit,
  a.account_for_install_bussiness_apps_last4_m        as account_for_install_bussiness_apps_last4_m,
  a.city_level_model                                  as city_level_model,
  a.consume_model                                     as consume_model,
  a.education_model                                   as education_model,
  a.marriage_model                                    as marriage_model,
  a.financial_model                                   as financial_model,
  a.income_level                                      as income_level,
  a.blk_list1                                         as blk_list1,
  a.blk_list2                                         as blk_list2,
  a.blk_list_loc                                      as blk_list_loc,
  a.virtual_malicious_status                          as virtual_malicious_status,
  a.counterfeit_agency_status                         as counterfeit_agency_status,
  a.forgedid_status                                   as forgedid_status,
  a.gamer_arbitrage_status                            as gamer_arbitrage_status,
  a.id_theft_status                                   as id_theft_status,
  a.hit_deadbeat_list                                 as hit_deadbeat_list,
  a.fraud_industry                                    as fraud_industry,
  a.cat_pool                                          as cat_pool,
  a.suspicious_device                                 as suspicious_device,
  a.abnormal_payment                                  as abnormal_payment,
  a.abnormal_account                                  as abnormal_account,
  a.account_hacked                                    as account_hacked,
  a.score_level                                       as score_level,
  a.pass                                              as pass,
  a.associated_partner_evaluation_rating              as associated_partner_evaluation_rating,
  a.vendor_attributes                                 as vendor_attributes,
  a.multi_borrowing                                   as multi_borrowing,
  a.gambling_preference                               as gambling_preference,
  a.gaming_preference                                 as gaming_preference,
  a.multimedia_preference                             as multimedia_preference,
  a.social_preference                                 as social_preference,
  a.create_time                                       as create_time,
  a.update_time                                       as update_time
from (
  select
    *
  from (
    select
      *,row_number() over(partition by order_id,project_id order by id desc) rn
    from stage.t_personas
  ) as a
  where a.rn=1
) as a
left join (
  select distinct
    user_hash_no,
    due_bill_no,
    product_id
  from ods.loan_apply
) as b
on  a.pro_code = b.product_id
and a.order_id = b.due_bill_no
-- limit 10
;
