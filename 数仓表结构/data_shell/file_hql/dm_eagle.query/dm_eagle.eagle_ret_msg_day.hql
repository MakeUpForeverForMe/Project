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





insert overwrite table dm_eagle.eagle_ret_msg_day partition (biz_date,product_id)
select
  capital_id,
  channel_id,
  project_id,
  0 as loan_terms,
  "01" as ret_msg_stage,
  ret_msg,
  sum(nvl(ret_msg_num,0))  as ret_msg_num,
  sum(nvl(ret_msg_rate,0)) as ret_msg_rate,
  biz_date,
  biz_conf.product_id${vt} as product_id
from dw.dw_credit_ret_msg_day
join (
  select distinct
    capital_id,
    channel_id,
    project_id,
    product_id_vt,
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
  ) as tmp
) as biz_conf
on  dw_credit_ret_msg_day.product_id = biz_conf.product_id
and dw_credit_ret_msg_day.biz_date = '${ST9}'
and biz_conf.product_id${vt} is not null
group by
  capital_id,
  channel_id,
  project_id,
  ret_msg,
  biz_date,
  biz_conf.product_id${vt}
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
  biz_date,
  biz_conf.product_id${vt} as product_id
from dw.dw_loan_ret_msg_day
join (
  select distinct
    capital_id,
    channel_id,
    project_id,
    product_id_vt,
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
  ) as tmp
) as biz_conf
on  dw_loan_ret_msg_day.product_id = biz_conf.product_id
and dw_loan_ret_msg_day.biz_date = '${ST9}'
and biz_conf.product_id${vt} is not null
group by
  capital_id,
  channel_id,
  project_id,
  loan_terms,
  ret_msg,
  biz_date,
  biz_conf.product_id${vt}
-- limit 10
;
