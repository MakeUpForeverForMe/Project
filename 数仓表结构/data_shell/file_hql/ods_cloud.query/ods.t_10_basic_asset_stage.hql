set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
insert overwrite table ods_new_s.t_10_basic_asset_stage
select
  0                                                   as id,
  cast(null as string)                                as import_id,
  loan_info.product_id                                as project_id,
  project_info.asset_type                             as asset_type,
  loan_info.due_bill_no                               as serial_number,
  lending.contract_no                                 as contract_code,
  lending.loan_init_principal                         as contract_amount,
  lending.interest_rate_type                          as interest_rate_type,
  lending.loan_init_interest_rate                     as contract_interest_rate,
  0                                                   as base_interest_rate,
  0                                                   as fixed_interest_rate,
  0                                                   as fixed_interest_diff,
  cast(null as string)                                as interest_rate_ajustment,
  lending.loan_issue_date                             as loan_issue_date,
  lending.loan_expiry_date                            as loan_expiry_date,
  first_repay.first_repay_date                        as frist_repayment_date,
  lending.loan_type                                   as repayment_type,
  '月'                                                as repayment_frequency,
  lending.cycle_day                                   as loan_repay_date,
  lending.tail_amount                                 as tail_amount,
  lending.tail_amount_rate                            as tail_amount_rate,
  lending.loan_usage                                  as consume_use,
  lending.guarantee_type                              as guarantee_type,
  max(loan_info.e_d_date)                             as extract_date,
  loan_info.remain_principal                          as extract_date_principal_amount,
  lending.loan_init_interest_rate *  100              as loan_cur_interest_rate,
  cust.cutomer_type                                   as borrower_type,
  cust.name                                           as borrower_name,
  cust.idcard_type                                    as document_type,
  cust.idcard_no                                      as document_num,
  cust.cust_rating                                    as borrower_rating,
  cust.job_type                                       as borrower_industry,
  cust.mobie                                          as phone_num,
  cust.sex                                            as sex,
  cust.birthday                                       as birthday,
  cust.age                                            as age,
  cust.resident_province                              as province,
  cust.resident_city                                  as city,
  cust.marriage_status                                as marital_status,
  cust.resident_county                                as country,
  cust.income_year                                    as annual_income,
  0                                                   as income_debt_rate,
  cust.education_ws                                   as education_level,
  loan_info.loan_term_remain                          as period_exp,
  loan_info.account_age                               as account_age,
  lending.contract_term                               as residual_maturity_con,
  lending.contract_term - loan_info.account_age       as residual_maturity_ext,
  to_date(date_sub(current_timestamp(),1))            as statistics_date,
  cast(null as string)                                as extract_interest_date,
  nvl(loan_info.overdue_days,0)                       as curr_over_days,
  nvl(loan_info.loan_term_remain,0)                   as remain_counts,
  loan_info.remain_principal                          as remain_amounts,
  loan_info.remain_interest                           as remain_interest,
  loan_info.remain_othAmounts                         as remain_other_amounts,
  lending.loan_init_term                              as periods,
  cast(null as string)                                as period_amounts,
  cast(null as string)                                as bus_product_id,
  cast(null as string)                                as bus_product_name,
  cast(null as string)                                as shoufu_amount,
  cast(null as string)                                as selling_price,
  lending.loan_init_interest_rate / 365               as contract_daily_interest_rate,
  cast(null as string)                                as repay_plan_cal_rule,
  365                                                 as contract_daily_interest_rate_count,
  --todo total_investment_amount
  0                                                   as total_investment_amount,
  lending.loan_init_interest_rate / 12                as contract_month_interest_rate,
  cast(null as string)                                as status_change_log,
  cast(null as string)                                as package_filter_id,
  cast(null as string)                                as virtual_asset_bag_id,
  nvl(risk.map_value['wind_control_status'],'Yes')         as wind_control_status,
  nvl(risk.map_value['wind_control_status_pool'],'Yes')    as wind_control_status_pool,
  nvl(risk.map_value['cheat_level'],-1)               as cheat_level,
  nvl(risk.map_value['score_range'],-1)               as score_range,
  nvl(risk.map_value['score_level'],-1)               as score_level,
  cast(null as string)                                as create_time,
  cast(null as string)                                as update_time,
  1                                                   as data_source,
  cust.resident_address                               as address,
  lending.mortgage_rate                               as mortgage_rates
from (
  select * from ods_new_s.loan_info_cloud
  where e_d_date = '3000-12-31'
) loan_info
left join ods_new_s.loan_lending_cloud lending
on loan_info.due_bill_no = lending.due_bill_no
inner join dim_new.project_info project_info
on loan_info.product_id = project_info.project_id
inner join dim_new.project_due_bill_no pro_due
on loan_info.due_bill_no = pro_due.due_bill_no
left join (
  select
    due_bill_no,
    min(should_repay_date) as first_repay_date
  from ods_new_s.repay_schedule_inter
  group by due_bill_no
) first_repay
on loan_info.due_bill_no = first_repay.due_bill_no
inner join ods_new_s.customer_info_cloud cust
on loan_info.due_bill_no = cust.due_bill_no
left join (
  select distinct due_bill_no,first_value(map_value) over(partition by due_bill_no order by biz_date desc) as map_value
  from ods_new_s.risk_control where source_table='t_asset_wind_control_history'
) risk
on loan_info.due_bill_no = risk.due_bill_no
group by
loan_info.due_bill_no,
loan_info.product_id,
project_info.asset_type,
loan_info.due_bill_no,
lending.contract_no,
lending.loan_init_principal,
lending.interest_rate_type,
lending.loan_init_interest_rate,
lending.loan_issue_date,
lending.loan_expiry_date,
risk.map_value,
first_repay.first_repay_date,
lending.loan_type,
lending.cycle_day,
lending.loan_usage,
lending.guarantee_type,
loan_info.remain_principal,
cust.cutomer_type,
cust.name,
cust.idcard_type,
cust.idcard_no,
cust.mobie,
cust.sex,
cust.birthday,
cust.age,
cust.resident_province,
cust.resident_city,
cust.marriage_status,
cust.resident_county,
cust.income_year,
cust.education_ws,
loan_info.loan_term_remain,
loan_info.account_age,
lending.contract_term,
loan_info.overdue_days,
loan_info.remain_principal,
loan_info.remain_interest,
loan_info.remain_othAmounts,
lending.loan_init_term,
cust.resident_address,
lending.tail_amount,
lending.tail_amount_rate,
cust.cust_rating,
cust.job_type,
lending.mortgage_rate
;
