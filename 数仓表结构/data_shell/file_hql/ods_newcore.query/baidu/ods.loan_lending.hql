set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
insert overwrite table ods${db_suffix}.loan_lending partition(product_id)
select
  a.apply_no,
  b.contract_no          as contract_no,
  a.total_term           as contract_term,
  a.due_bill_no,
  b.guarantee_type       as guarantee_type,
  a.loan_usage,
  a.contract_start_date  as loan_issue_date,
  a.contract_end_date    as loan_expiry_date,
  a.actual_loan_date     as loan_active_date,
  a.contract_end_date    as loan_expire_date,
  a.repay_day            as cycle_day,
  case a.int_repay_type
  when '等额本息' then 'MCEI'
  when '等额本金' then 'MCEP'
  when '等本等费' then 'WFSF'
  else a.int_repay_type
  end                    as loan_type,
  a.int_repay_type       as loan_type_cn,
  0                      as contract_daily_interest_rate_basis,
  a.interest_rate_type   as interest_rate_type,
  a.service_fee_annualized_rate        as loan_init_interest_rate,
  0                      as loan_init_term_fee_rate,
  0                      as loan_init_svc_fee_rate,
  0                      as loan_init_penalty_rate,
  0                      as tail_amount,
  0                      as tail_amount_rate,
  ''                     as bus_product_id,
  ''                     as bus_product_name,
  0                      as mortgage_rate,
  a.actual_loan_date     as biz_date,
  a.init_contract_amount as loan_original_principal,
  0                      as shoufu_amount,
  a.product_no           as product_id
from
(select * from
(select
*,
row_number() over(partition by due_bill_no order by batch_date desc) rn from stage.loan_contract where d_date<='${ST9}' and p_type='WS0012200001') a
where a.rn=1
) a
left join
(select * from
(select
*,
row_number() over(partition by due_bill_no order by batch_date desc) rn from stage.loan_contract_ex where d_date<='${ST9}' and p_type='WS0012200001') a
where a.rn=1
 ) b
on a.due_bill_no=b.due_bill_no and a.project_no=b.project_no;
