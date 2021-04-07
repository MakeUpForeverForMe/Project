set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;
set hive.groupby.orderby.position.alias=true;
-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=64000000;      -- 64M
set hive.merge.smallfiles.avgsize=64000000; -- 64M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;



insert overwrite table dm_eagle.assets_distribution partition(product_id)
select
  cast(d.capital_id as string )                                                  as capital_id,
  cast(d.channel_id as string )                                                  as channel_id,
  cast(d.project_id as string )                                                  as project_id,
  cast(a.order_id as String)                                                     as bill_no,
  cast(a.order_id as String )                                                    as apply_no,
  null                                                                           as credit_id,
  a.order_id                                                                     as use_credit_id,
  cast(c.age as string)                                                          as age,
  case b.sex
  when '男' then '2'
  when '女' then '3'
  else b.sex end                                                                 as gender,
  case
  when b.income_month * 12 <= 20000                   then '31'
  when b.income_month * 12  between 20001 and 50000   then '32'
  when b.income_month * 12  between 50001 and 100000  then '33'
  when b.income_month * 12  between 100001 and 150000 then '34'
  when b.income_month * 12  between 150001 and 200000 then '35'
  when b.income_month * 12  between 200001 and 250000 then '36'
  when b.income_month * 12  between 250001 and 300000 then '37'
  when b.income_month * 12  between 300001 and 350000 then '38'
  when b.income_month * 12  between 350001 and 400000 then '39'
  when b.income_month * 12  > 400000                  then '40'
  else cast(b.income_month * 12 as string) end                                   as income,
  case
  when b.education_ws = '硕士及以上' then '43'
  when b.education_ws = '大学本科'   then '42'
  when b.education_ws = '大专及以下' then '41'
  when b.education_ws = '未知'       then 'NULL'
  else b.education_ws end                                                        as education,
  b.idcard_area                                                                  as region,
  b.idcard_province                                                              as province,
  cast(c.loan_amount_apply as decimal(15,0))                                     as bill_credit_line,
  c.loan_terms                                                                   as bill_term,
  cast(c.interest_rate as decimal(15,8))                                         as bill_interest_rate,
  cast(if(c.credit_coef is null,c.interest_rate,c.credit_coef) as decimal(15,8)) as credit_coef,
  b.sex                                                                          as bill_sex,
  c.age                                                                          as bill_age,
  b.idcard_area                                                                  as bill_id_card_area,
  b.idcard_province                                                              as bill_province,
  b.education                                                                    as bill_education,
  b.education_ws                                                                 as bill_education_ws,
  cast(b.income_month*12 as decimal(15,4))                                       as bill_annual_income,
  cast(a.score_level as string)                                                  as score_level,
  cast(a.city_level_model as string)                                             as city_level,
  cast(a.financial_model as string)                                              as financ_ability,
  cast(a.consume_model as string)                                                as consumption_level,
  cast(a.marriage_model as string)                                               as marriage_status,
  cast(a.education_model as string)                                              as edu_level,
  a.income_level                                                                 as income_level,
  a.apply_by_id_last_year                                                        as apply_by_id_last_year,
  a.apps_of_con_fin_pro_last_two_year                                            as apps_of_con_fin_pro_last_two_year,
  a.usage_rate_of_compus_loan_app                                                as usage_rate_of_compus_loan_app,
  a.apps_of_con_fin_org_last_two_year                                            as apps_of_con_fin_org_last_two_year,
  a.reject_times_of_credit_apply_last4_m                                         as reject_times_of_credit_apply_last4_m,
  a.overdue_times_of_credit_apply_last4_m                                        as overdue_times_of_credit_apply_last4_m,
  cast(a.sum_credit_of_web_loan as int)                                          as sum_credit_of_web_loan,
  a.count_of_register_apps_of_fin_last_m                                         as count_of_register_apps_of_fin_last_m,
  a.count_of_uninstall_apps_of_loan_last3_m                                      as count_of_uninstall_apps_of_loan_last3_m,
  a.days_of_location_upload_lats9_m                                              as days_of_location_upload_lats9_m,
  a.account_for_in_town_in_work_day_last_m                                       as account_for_in_town_in_work_day_last_m,
  a.count_of_installition_of_loan_app_last2_m                                    as count_of_installition_of_loan_app_last2_m,
  a.risk_of_device                                                               as risk_of_device,
  a.account_for_wifi_use_time_span_last5_m                                       as account_for_wifi_use_time_span_last5_m,
  a.count_of_notices_of_fin_message_last9_m                                      as count_of_notices_of_fin_message_last9_m,
  a.days_of_earliest_register_of_loan_app_last9_m                                as days_of_earliest_register_of_loan_app_last9_m,
  cast(a.loan_amt_last3_m as int)                                                as loan_amt_last3_m,
  a.overdue_loans_of_more_than1_day_last6_m                                      as overdue_loans_of_more_than1_day_last6_m,
  cast(a.amt_of_perfermance_loans_last3_m as int)                                as amt_of_perfermance_loans_last3_m,
  a.last_financial_query                                                         as last_financial_query,
  a.average_daily_open_times_of_fin_apps_last_m                                  as average_daily_open_times_of_fin_apps_last_m,
  a.times_of_uninstall_fin_apps_last15_d                                         as times_of_uninstall_fin_apps_last15_d,
  a.account_for_install_bussiness_apps_last4_m                                   as account_for_install_bussiness_apps_last4_m,
  a.blk_list1                                                                    as blk_list1,
  a.blk_list2                                                                    as blk_list2,
  a.blk_list_loc                                                                 as blk_list_loc,
  a.virtual_malicious_status                                                     as virtual_malicious_status,
  a.counterfeit_agency_status                                                    as counterfeit_agency_status,
  a.forgedid_status                                                              as forged_id_status,
  a.gamer_arbitrage_status                                                       as gamer_arbitrage_status,
  a.id_theft_status                                                              as id_theft_status,
  a.hit_deadbeat_list                                                            as hit_deadbeat_list,
  a.fraud_industry                                                               as fraud_industry,
  a.cat_pool                                                                     as cat_pool,
  a.suspicious_device                                                            as suspicious_device,
  a.abnormal_payment                                                             as abnormal_payment,
  a.abnormal_account                                                             as abnormal_account,
  a.account_hacked                                                               as account_hacked,
  a.ret_msg                                                                      as ret_msg,
  cast(a.associated_partner_evaluation_rating as int)                            as associated_partner_evaluation_rating,
  cast(a.vendor_attributes as int)                                               as vendor_attributes,
  cast(a.multi_borrowing as int)                                                 as multi_borrowing,
  cast(a.gambling_preference as int)                                             as gambling_preference,
  cast(a.gaming_preference as int)                                               as gaming_preference,
  cast(a.multimedia_preference as int)                                           as multimedia_preference,
  cast(a.social_preference as int)                                               as social_preference,
  a.pro_code                                                                     as product_id
from (
  select
    *
  from ods.user_label
) as a
left join (
  select
    *
  from (
    select
      *,row_number() over(partition by user_hash_no,product_id order by user_hash_no desc) as rn
    from ods.customer_info
    where product_id in ('001801','001802','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007','002401','002402')
  ) as a
  where a.rn = 1
) as b
on  a.pro_code     = b.product_id
and a.user_hash_no = b.user_hash_no
left join(
  select distinct
    user_hash_no,
    age,
    due_bill_no,
    product_id,
    credit_coef,
    loan_amount_apply,
    loan_terms,
    interest_rate
  from ods.loan_apply
  where product_id in ('001801','001802','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007','002401','002402')
) as c
on  a.pro_code = c.product_id
and a.order_id = c.due_bill_no
left join (
  select distinct
         capital_id,
         channel_id,
         project_id,
         product_id_vt,
         product_id
         from (
           select
             max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
             max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
             max(if(col_name = 'project_id',   col_val,null)) as project_id,
             max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
             max(if(col_name = 'product_id',   col_val,null)) as product_id
           from dim.data_conf
           where col_type = 'ac'
           group by col_id
        )tmp
) as d
on a.pro_code = d.product_id
;
