-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
-- 设置可以使用正则匹配 `(a|b)?+.+`
set hive.support.quoted.identifiers=None;




insert overwrite table ods${db_suffix}.loan_info partition(is_settled = 'no',product_id)
select
  due_bill_no,
  apply_no,
  loan_active_date,
  loan_init_principal,
  loan_init_term,
  max(loan_term)         over(partition by due_bill_no order by s_d_date) as loan_term,
  max(should_repay_date) over(partition by due_bill_no order by s_d_date) as should_repay_date,
  loan_term_repaid,
  loan_term_remain,
  loan_init_interest,
  loan_init_term_fee,
  loan_init_svc_fee,
  loan_status,
  loan_status_cn,
  loan_out_reason,
  paid_out_type,
  paid_out_type_cn,
  paid_out_date,
  terminal_date,
  paid_amount,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_svc_fee,
  paid_term_fee,
  paid_mult,
  remain_amount,
  remain_principal,
  remain_interest,
  remain_svc_fee,
  remain_term_fee,
  overdue_principal,
  overdue_interest,
  overdue_svc_fee,
  overdue_term_fee,
  overdue_penalty,
  overdue_mult_amt,
  min(overdue_date_start) over(partition by due_bill_no order by s_d_date) as overdue_date_first,
  overdue_date_start,
  overdue_days,
  overdue_date,
  overdue_date_start as dpd_begin_date,
  overdue_days as dpd_days,
  0 as dpd_days_count,
  max(overdue_days) over(partition by due_bill_no order by s_d_date) as dpd_days_max,
  collect_out_date as collect_out_date,
  overdue_term,
  count(distinct if(overdue_days > 0,overdue_term,null)) over(partition by due_bill_no order by s_d_date)    as overdue_terms_count,
  max(overdue_terms_max)                                 over(partition by due_bill_no order by s_d_date)    as overdue_terms_max,
  nvl(sum(distinct overdue_principal)                    over(partition by due_bill_no order by s_d_date),0) as overdue_principal_accumulate,
  nvl(max(overdue_principal)                             over(partition by due_bill_no order by s_d_date),0) as overdue_principal_max,
  s_d_date,
  e_d_date,
  effective_time,
  expire_time,
  product_id


  `(biz_date|product_id)?+.+`,
  biz_date as s_d_date,
  lead(biz_date) over(partition by product_id,due_bill_no,loan_term order by biz_date) as e_d_date,
  product_id
from ods${db_suffix}.repay_schedule_inter;
