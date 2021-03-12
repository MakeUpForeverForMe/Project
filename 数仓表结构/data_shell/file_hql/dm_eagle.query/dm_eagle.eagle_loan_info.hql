set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.support.quoted.identifiers=None;     -- 设置可以使用正则表达式查找字段


insert overwrite table dm_eagle${db_suffix}.eagle_loan_info partition(biz_date = '${ST9}',product_id)
select
  loan_apply.user_hash_no,
  loan_apply.cust_id,
  loan_info.due_bill_no,
  loan_lending.contract_no,
  loan_lending.apply_no,
  loan_lending.loan_usage,
  loan_info.loan_active_date,
  loan_lending.cycle_day,
  loan_lending.loan_expire_date,
  loan_lending.loan_type,
  loan_lending.loan_type_cn,
  loan_info.loan_init_term,
  loan_info.loan_term,
  loan_info.loan_term_repaid,
  loan_info.loan_term_remain,
  if(loan_info.due_bill_no in (
      '1120081714554090143221',
      '1120092814474954123907',
      '1120101507502092992722'
    ),loan_info.loan_status,
    case
    when repay_schedule.pre_count = loan_info.loan_init_term   then 'F'
    when repay_schedule.over_count > 0                         then 'O'
    else 'N' end
  ) as loan_status,
  if(loan_info.due_bill_no in (
      '1120081714554090143221',
      '1120092814474954123907',
      '1120101507502092992722'
    ),loan_info.loan_status,
    case
    when repay_schedule.pre_count = loan_info.loan_init_term   then '已还清'
    when repay_schedule.over_count > 0                         then '逾期'
    else '正常' end
  ) as loan_status_cn,
  loan_info.loan_out_reason,
  loan_info.paid_out_type,
  loan_info.paid_out_type_cn,
  loan_info.paid_out_date,
  loan_info.terminal_date,
  loan_lending.loan_init_principal,
  loan_lending.loan_init_interest_rate,
  nvl(loan_apply.credit_coef,loan_lending.loan_init_interest_rate) as credit_coef,
  loan_info.loan_init_interest,
  loan_lending.loan_init_term_fee_rate,
  loan_info.loan_init_term_fee,
  loan_lending.loan_init_svc_fee_rate,
  loan_info.loan_init_svc_fee,
  loan_lending.loan_init_penalty_rate,
  nvl(repay_schedule.should_repay_penalty,0) as loan_penalty_accumulate,
  loan_info.paid_amount,
  loan_info.paid_principal,
  loan_info.paid_interest,
  loan_info.paid_penalty,
  loan_info.paid_svc_fee,
  loan_info.paid_term_fee,
  loan_info.paid_mult,
  loan_info.remain_amount,
  loan_info.remain_principal,
  loan_info.remain_interest,
  loan_info.remain_svc_fee,
  loan_info.remain_term_fee,
  nvl(repay_schedule.should_repay_penalty,0) - loan_info.paid_penalty as remain_penalty,
  loan_info.overdue_principal,
  loan_info.overdue_interest,
  loan_info.overdue_svc_fee,
  loan_info.overdue_term_fee,
  loan_info.overdue_penalty,
  loan_info.overdue_mult_amt,
  loan_info.overdue_date_first,
  loan_info.overdue_date_start,
  loan_info.overdue_days,
  loan_info.overdue_date,
  loan_info.dpd_begin_date,
  loan_info.dpd_days,
  overdue_days_count.dpd_days_count,
  loan_info.dpd_days_max,
  loan_info.collect_out_date,
  loan_info.overdue_term,
  loan_info.overdue_terms_count,
  loan_info.overdue_terms_max,
  loan_info.overdue_principal_accumulate,
  loan_info.overdue_principal_max,
  loan_info.product_id
from (
  select `(s_d_date|e_d_date|effective_time|expire_time|is_settled)?+.+`
  from ods_new_s${db_suffix}.loan_info
  where 1 > 0
    and '${ST9}' between s_d_date and date_sub(e_d_date,1)  ${hive_param_str}
) as loan_info
left join (
  select *
  from ods_new_s${db_suffix}.loan_lending
  where 1 > 0
    and biz_date <= date_add('${ST9}',3)   ${hive_param_str}
) as loan_lending
on loan_info.due_bill_no = loan_lending.due_bill_no
left join (
  select distinct
    cust_id,
    user_hash_no,
    due_bill_no,
    credit_coef,
    product_id
  from ods_new_s.loan_apply
  where 1 > 0
    and biz_date <= date_add('${ST9}',5)   ${hive_param_str}
) as loan_apply
on  loan_info.product_id  = loan_apply.product_id
and loan_info.due_bill_no = loan_apply.due_bill_no
left join (
  select
    due_bill_no,
    sum(dpd_days_count) as dpd_days_count
  from (
    select
      due_bill_no,
      cast(max(overdue_days) as string) as dpd_days_count
    from ods_new_s${db_suffix}.loan_info
    where 1 > 0
      and s_d_date <= '${ST9}'  ${hive_param_str}
      -- and s_d_date <= '2020-07-13'
      -- and due_bill_no = '1000000465'
      -- and due_bill_no = '1120062009364346847397'
    group by due_bill_no,overdue_date_start
  ) as tmp
  group by due_bill_no
) as overdue_days_count
on loan_info.due_bill_no = overdue_days_count.due_bill_no
left join (
  select
    due_bill_no,
    sum(should_repay_penalty)                    as should_repay_penalty,
    sum(if(schedule_status = 'F',1,0))           as pre_count,
    sum(if(schedule_status = 'O',1,0))           as over_count
  from ods_new_s${db_suffix}.repay_schedule
  where 1 > 0
    and s_d_date <= '${ST9}' and '${ST9}' < e_d_date  ${hive_param_str}
  group by due_bill_no
) as repay_schedule
on loan_info.due_bill_no = repay_schedule.due_bill_no
-- where 1 > 0
--   and loan_info.due_bill_no = '1000000465'
-- limit 1
;
