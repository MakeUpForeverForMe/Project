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


insert overwrite table dm_eagle.eagle_credit_loan_approval_amount_rate_day partition (biz_date,product_id)
select
  capital_id                                                               as capital_id,
  channel_id                                                               as channel_id,
  project_id                                                               as project_id,
  max(credit_approval_amount)                                              as credit_approval_amount,
  sum(credit_loan_approval_num_amount)                                     as loan_approval_amount,
  sum(credit_loan_approval_num_amount) / max(credit_approval_amount) * 100 as credit_loan_approval_amount_rate,
  credit_approval.biz_date                                                 as biz_date,
  biz_conf.product_id${vt}                                                 as product_id
from (
  select
    credit_approval_amount,
    credit_loan_approval_num_amount,
    biz_date,
    product_id
  from dw.dw_credit_approval_stat_day
  where 1 > 0
    and biz_date = '${ST9}'
    and (
      case
        when product_id = 'pl00282' and biz_date > '2019-02-22' then false
        else true
      end
    )
) as credit_approval
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
  where product_id${vt} is not null
) as biz_conf
on credit_approval.product_id = biz_conf.product_id
group by
  capital_id,
  channel_id,
  project_id,
  credit_approval.biz_date,
  biz_conf.product_id${vt}
-- limit 10
;
