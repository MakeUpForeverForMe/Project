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




insert overwrite table dm_eagle${db_suffix}.eagle_repayment_record_day partition(biz_date = '${ST9}',product_id)
select
  biz_conf.capital_id                                            as capital_id,
  biz_conf.channel_id                                            as channel_id,
  biz_conf.project_id                                            as project_id,
  repaid_loan.loan_init_term                                     as loan_terms,
  repaid_loan.due_bill_no                                        as due_bill_no,
  repaid_loan.repay_term                                         as repay_term,
  '${ST9}'                                                       as txn_date,
  repaid_loan.paid_amount                                        as paid_amount,
  repaid_loan.paid_principal                                     as paid_principal,
  repaid_loan.paid_interest                                      as paid_interest,
  repaid_loan.paid_svc_fee                                       as paid_svc_fee,
  repaid_loan.paid_penalty                                       as paid_penalty,
  repay_schedule.loan_init_principal - repaid_sum.paid_principal as remain_principal,
  repay_schedule.schedule_status                                 as paid_out_type,
  -- '${ST9}'                                                       as biz_date,
  repaid_loan.product_id                                         as product_id
from (
    select distinct
           capital_id,
           channel_id,
           project_id,
           product_id_vt ,
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
) as biz_conf
join (
  select
    product_id,
    loan_init_term,
    due_bill_no,
    repay_term,
    sum(repay_amount)                                        as paid_amount,    -- 金额
    sum(if(bnp_type = 'Pricinpal',          repay_amount,0)) as paid_principal, -- 本金
    sum(if(bnp_type = 'Interest',           repay_amount,0)) as paid_interest,  -- 利息
    sum(if(bnp_type in ('TERMFee','SVCFee'),repay_amount,0)) as paid_svc_fee,   -- 费用
    sum(if(bnp_type = 'Penalty',            repay_amount,0)) as paid_penalty    -- 罚息
  from ods${db_suffix}.repay_detail
  where 1 > 0
    and biz_date = '${ST9}'
    and bnp_type in ('Pricinpal','Interest','TERMFee','SVCFee','Penalty')  ${hive_param_str}
  group by due_bill_no,loan_init_term,product_id,repay_term
) as repaid_loan
on biz_conf.product_id = repaid_loan.product_id
left join (
  select
    due_bill_no,
    repay_term,
    sum(repay_amount) over(partition by product_id,due_bill_no order by repay_term rows between UNBOUNDED PRECEDING AND CURRENT ROW) as paid_principal,
    product_id
  from ods${db_suffix}.repay_detail
  where 1 > 0
    and biz_date <= '${ST9}'
    and bnp_type = 'Pricinpal' ${hive_param_str}
  -- group by due_bill_no,product_id,repay_term
) as repaid_sum
on  repaid_loan.product_id  = repaid_sum.product_id
and repaid_loan.due_bill_no = repaid_sum.due_bill_no
and repaid_loan.repay_term  = repaid_sum.repay_term
left join (
  select
    product_id,
    due_bill_no,
    loan_term,
    loan_init_principal,
    schedule_status
  from ods${db_suffix}.repay_schedule
  where 1 > 0
    and '${ST9}' between s_d_date and date_sub(e_d_date,1) ${hive_param_str}
) as repay_schedule
on  repaid_loan.product_id  = repay_schedule.product_id
and repaid_loan.due_bill_no = repay_schedule.due_bill_no
and repaid_loan.repay_term  = repay_schedule.loan_term
;
