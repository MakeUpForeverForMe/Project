set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
 

insert overwrite table dw.dw_credit_ret_msg_day partition(biz_date = '${ST9}',product_id)
-- insert overwrite table dw.dw_credit_ret_msg_day partition(biz_date,product_id)
select distinct
  resp_msg,
  count(apply_id) over(partition by biz_date,product_id,resp_msg) as ret_msg_num,
  nvl(count(apply_id) over(partition by biz_date,product_id,resp_msg) / count(apply_id) over(partition by biz_date,product_id) * 100,0) as ret_msg_rate,
  -- biz_date,
  product_id
from (
  select
    apply_id,
    resp_msg,
    biz_date,
    product_id
  from ods.credit_apply
  where 1 > 0
    and resp_code != 1
    and biz_date = '${ST9}' ${hive_param_str}
) as credit_apply
;
