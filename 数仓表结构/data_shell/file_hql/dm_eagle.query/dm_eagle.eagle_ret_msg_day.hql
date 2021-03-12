set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.groupby.orderby.position.alias=true;

insert overwrite table dm_eagle.eagle_ret_msg_day partition (biz_date = '${ST9}',product_id)
-- insert overwrite table dm_eagle.eagle_ret_msg_day partition (biz_date,product_id)
select
  capital_id,
  channel_id,
  project_id,
  0 as loan_terms,
  "01" as ret_msg_stage,
  ret_msg,
  sum(nvl(ret_msg_num,0))  as ret_msg_num,
  sum(nvl(ret_msg_rate,0)) as ret_msg_rate,
  -- biz_date,
  biz_conf.product_id${vt} as product_id
from dw_new.dw_credit_ret_msg_day
join dim_new.biz_conf
on  dw_credit_ret_msg_day.product_id = biz_conf.product_id
and dw_credit_ret_msg_day.biz_date = '${ST9}'
and biz_conf.product_id${vt} is not null
group by 1,2,3,4,5,6,9
-- ,10
union all
select
  capital_id,
  channel_id,
  project_id,
  loan_terms,
  "02" as ret_msg_stage,
  ret_msg,
  sum(nvl(ret_msg_num,0))  as ret_msg_num,
  sum(nvl(ret_msg_rate,0)) as ret_msg_rate,
  -- biz_date,
  biz_conf.product_id${vt} as product_id
from dw_new.dw_loan_ret_msg_day
join dim_new.biz_conf
on  dw_loan_ret_msg_day.product_id = biz_conf.product_id
and dw_loan_ret_msg_day.biz_date = '${ST9}'
and biz_conf.product_id${vt} is not null
group by 1,2,3,4,5,6,9
-- ,10
;
