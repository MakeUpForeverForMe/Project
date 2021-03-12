set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.groupby.orderby.position.alias=true;


insert overwrite table dm_eagle.eagle_credit_loan_approval_amount_rate_day partition (biz_date='${ST9}',product_id)
select
  capital_id,
  channel_id,
  project_id,
  -- sum(nvl(credit_approval_amount,0)) as credit_approval_amount,
  max(credit_approval_amount)                                              as credit_approval_amount,
  -- sum(nvl(credit_loan_approval_num_amount_t0,0)) + sum(nvl(credit_loan_approval_num_amount_t1,0)) + sum(nvl(credit_loan_approval_num_amount_t2,0)) + sum(nvl(credit_loan_approval_num_amount_t3,0)) as loan_approval_amount,
  sum(credit_loan_approval_num_amount)                                     as loan_approval_amount,
  -- (sum(nvl(credit_loan_approval_num_amount_t0,0)) + sum(nvl(credit_loan_approval_num_amount_t1,0)) + sum(nvl(credit_loan_approval_num_amount_t2,0)) + sum(nvl(credit_loan_approval_num_amount_t3,0))) / sum(nvl(credit_approval_amount,0)) * 100  as credit_loan_approval_amount_rate,
  sum(credit_loan_approval_num_amount) / max(credit_approval_amount) * 100 as credit_loan_approval_amount_rate,
  biz_conf.product_id${vt} as product_id
from dw_new.dw_credit_approval_stat_day
join (
  select *
  from
  dim_new.biz_conf
  where (case when product_id = 'pl00282' and '${ST9}' > '2019-02-22' then 0 else 1 end) = 1
) as biz_conf
on  dw_credit_approval_stat_day.product_id = biz_conf.product_id
and dw_credit_approval_stat_day.biz_date = '${ST9}'
and biz_conf.product_id${vt} is not null
group by capital_id,channel_id,project_id,biz_conf.product_id${vt}
;
