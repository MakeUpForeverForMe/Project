set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
 

insert overwrite table dw.dw_credit_apply_stat_day partition(biz_date = '${ST9}',product_id)
-- insert overwrite table dw.dw_credit_apply_stat_day partition(biz_date,product_id)
select
  credit_apply_date                            as credit_apply_date,
  count(apply_id)                              as credit_apply_num,
  count(distinct user_hash_no)                 as credit_apply_num_person,
  sum(apply_amount)                            as credit_apply_amount,
  credit_approval_date                         as credit_approval_date,
  count(if(resp_code = '1',apply_id,null))     as credit_approval_num,
  count(if(resp_code = '1',user_hash_no,null)) as credit_approval_num_person,
  sum(if(resp_code = '1',credit_amount,0))     as credit_approval_amount,
  -- credit_apply_date                            as biz_date,
  product_id
from (
  select distinct
    user_hash_no,
    apply_id,
    to_date(credit_apply_time) as credit_apply_date,
    apply_amount,
    resp_code,
    if(resp_code = '1',to_date(credit_approval_time),null) as credit_approval_date,
    credit_amount,
    product_id
  from ods.credit_apply
  where 1 > 0
    and biz_date = '${ST9}'  ${hive_param_str}
) as credit_apply
group by
  credit_approval_date,
  credit_apply_date,
  product_id
-- limit 10
;
