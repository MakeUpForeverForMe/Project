-- 设置 Container 大小
set hive.tez.container.size=2048;
set tez.am.resource.memory.mb=2048;
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




insert overwrite table ods.t_10_basic_asset_stage partition(project_id)
select distinct
  0                                             as id,
  cast(null as string)                          as import_id,
  loan_info.asset_type                          as asset_type,
  loan_info.due_bill_no                         as serial_number,
  lending.contract_no                           as contract_code,
  loan_info.loan_init_principal                 as contract_amount,
  lending.interest_rate_type                    as interest_rate_type,
  lending.loan_init_interest_rate               as contract_interest_rate,
  0                                             as base_interest_rate,
  0                                             as fixed_interest_rate,
  0                                             as fixed_interest_diff,
  cast(null as string)                          as interest_rate_ajustment,
  lending.loan_issue_date                       as loan_issue_date,
  lending.loan_expiry_date                      as loan_expiry_date,
  first_repay.first_repay_date                  as frist_repayment_date,
  lending.loan_type                             as repayment_type,
  '月'                                          as repayment_frequency,
  lending.cycle_day                             as loan_repay_date,
  lending.tail_amount                           as tail_amount,
  lending.tail_amount_rate                      as tail_amount_rate,
  lending.loan_usage                            as consume_use,
  lending.guarantee_type                        as guarantee_type,
  loan_info.e_d_date                            as extract_date,
  loan_info.remain_principal                    as extract_date_principal_amount,
  lending.loan_init_interest_rate *  100        as loan_cur_interest_rate,
  cust.borrower_type                            as borrower_type,
  cust.name                                     as borrower_name,
  cust.idcard_type                              as document_type,
  cust.idcard_no                                as document_num,
  cust.cust_rating                              as borrower_rating,
  cust.job_type                                 as borrower_industry,
  cust.mobie                                    as phone_num,
  cust.sex                                      as sex,
  cust.birthday                                 as birthday,
  cust.age                                      as age,
  cust.resident_province                        as province,
  cust.resident_city                            as city,
  cust.marriage_status                          as marital_status,
  cust.resident_county                          as country,
  cust.income_year                              as annual_income,
  0                                             as income_debt_rate,
  cust.education_ws                             as education_level,
  loan_info.loan_term_remain                    as period_exp,
  loan_info.account_age                         as account_age,
  lending.contract_term                         as residual_maturity_con,
  lending.contract_term - loan_info.account_age as residual_maturity_ext,
  to_date(date_sub(current_timestamp(),1))      as statistics_date,
  cast(null as string)                          as extract_interest_date,
  nvl(loan_info.overdue_days,0)                 as curr_over_days,
  nvl(loan_info.loan_term_remain,0)             as remain_counts,
  loan_info.remain_principal                    as remain_amounts,
  loan_info.remain_interest                     as remain_interest,
  loan_info.remain_othAmounts                   as remain_other_amounts,
  loan_info.loan_init_term                      as periods,
  cast(null as string)                          as period_amounts,
  cast(null as string)                          as bus_product_id,
  cast(null as string)                          as bus_product_name,
  cast(null as string)                          as shoufu_amount,
  cast(null as string)                          as selling_price,
  lending.loan_init_interest_rate / 365         as contract_daily_interest_rate,
  cast(null as string)                          as repay_plan_cal_rule,
  365                                           as contract_daily_interest_rate_count,
  -- todo total_investment_amount
  0                                             as total_investment_amount,
  lending.loan_init_interest_rate / 12          as contract_month_interest_rate,
  cast(null as string)                          as status_change_log,
  cast(null as string)                          as package_filter_id,
  cast(null as string)                          as virtual_asset_bag_id,
  nvl(risk.wind_control_status,'Yes')           as wind_control_status,
  nvl(risk.wind_control_status_pool,'Yes')      as wind_control_status_pool,
  nvl(risk.cheat_level,-1)                      as cheat_level,
  nvl(risk.score_range,-1)                      as score_range,
  nvl(risk.score_level,-1)                      as score_level,
  cast(null as string)                          as create_time,
  cast(null as string)                          as update_time,
  loan_info.data_source                         as data_source,
  cust.resident_address                         as address,
  lending.mortgage_rate                         as mortgage_rates,
  loan_info.project_id                          as project_id
from (
  select * from ods.loan_info_abs
  where e_d_date = '3000-12-31'
) as loan_info
inner join ods.customer_info_abs as cust
on  loan_info.project_id  = cust.project_id
and loan_info.due_bill_no = cust.due_bill_no
inner join ods.loan_lending_abs as lending
on  loan_info.project_id  = lending.project_id
and loan_info.due_bill_no = lending.due_bill_no
left join (
  select
    project_id,
    due_bill_no,
    min(should_repay_date) as first_repay_date
  from ods.repay_schedule_abs
  group by project_id,due_bill_no
) as first_repay
on  loan_info.project_id  = first_repay.project_id
and loan_info.due_bill_no = first_repay.due_bill_no
left join (
  select
    project_id,
    due_bill_no,
    max(if(map_key = 'wind_control_status',map_val,'Yes'))      as wind_control_status,
    max(if(map_key = 'wind_control_status_pool',map_val,'Yes')) as wind_control_status_pool,
    max(if(map_key = 'score_range',map_val,-1))                 as score_range,
    max(if(map_key = 'cheat_level',map_val,-1))                 as cheat_level,
    max(if(map_key = 'score_level',map_val,-1))                 as score_level
  from ods.risk_control_abs
  where source_table in ('t_asset_wind_control_history')
    and map_key in ('wind_control_status','wind_control_status_pool','cheat_level','score_range','score_level')
  group by project_id,due_bill_no
) as risk
on  loan_info.project_id  = risk.project_id
and loan_info.due_bill_no = risk.due_bill_no
-- limit 10
;
