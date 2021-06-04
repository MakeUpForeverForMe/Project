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


insert overwrite table dm_eagle${db_suffix}.eagle_deferred_overdue_rate_full_month_product_day partition (biz_date = '${ST9}',product_id)
-- insert overwrite table dm_eagle${db_suffix}.eagle_deferred_overdue_rate_full_month_product_day partition (biz_date,product_id)
select
  biz_conf.capital_id                              as capital_id,
  biz_conf.channel_id                              as channel_id,
  biz_conf.project_id                              as project_id,
  overdue_num.dpd                                  as dpd,
  sum(nvl(should_repay.loan_principal,0))          as loan_principal,
  sum(nvl(overdue_num.overdue_principal,0))        as overdue_principal,
  sum(nvl(overdue_num.overdue_remain_principal,0)) as overdue_remain_principal,
  if(sum(nvl(overdue_num.overdue_principal,0)) = 0,0,sum(nvl(overdue_num.overdue_principal,0)) / sum(nvl(should_repay.loan_principal,0))) * 100 as overdue_rate_overdue_principal,
  if(sum(nvl(overdue_num.overdue_principal,0)) = 0,0,sum(nvl(overdue_num.overdue_remain_principal,0)) / sum(nvl(should_repay.loan_principal,0))) * 100 as overdue_rate_remain_principal,
  -- overdue_num.biz_date                             as biz_date,
  biz_conf.product_id${vt}                         as product_id
from (
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
       )tmp
        where 1 > 0
        and tmp.product_id${vt} is not null
) as biz_conf
join (
  select
    loan_active_date,
    should_repay_date,
    sum(overdue_principal)        as overdue_principal,
    sum(overdue_remain_principal) as overdue_remain_principal,
    biz_date,
    product_id,
    dpd
  from dw${db_suffix}.dw_loan_base_stat_overdue_num_day
  lateral view explode(
    split(
      concat_ws(',',
        if(overdue_days = 0,  '0',   null),
        if(overdue_days >= 1, '1+',  null),
        if(overdue_days > 3,  '3+',  null),
        if(overdue_days > 7,  '7+',  null),
        if(overdue_days > 14, '14+', null),
        if(overdue_days > 30, '30+', null),
        if(overdue_days > 60, '60+', null),
        if(overdue_days > 90, '90+', null),
        if(overdue_days > 120,'120+',null),
        if(overdue_days > 150,'150+',null),
        if(overdue_days > 180,'180+',null)
      ),','
    )
  ) dpd_x as dpd
  where 1 > 0
    and biz_date = '${ST9}'
  group by product_id,biz_date,loan_active_date,should_repay_date,dpd
  -- order by product_id,biz_date,loan_active_date,should_repay_date
) as overdue_num
on overdue_num.product_id = biz_conf.product_id
left join (
  select
    loan_active_date,
    sum(loan_principal) as loan_principal,
    biz_date,
    product_id
  from dw${db_suffix}.dw_loan_base_stat_should_repay_day
  where 1 > 0
    and biz_date = should_repay_date
  group by loan_active_date,biz_date,product_id
) as should_repay
on  overdue_num.product_id        = should_repay.product_id
and overdue_num.loan_active_date  = should_repay.loan_active_date
and overdue_num.should_repay_date = should_repay.biz_date
group by
biz_conf.capital_id,
biz_conf.channel_id,
biz_conf.project_id,
overdue_num.dpd,
-- overdue_num.biz_date,
biz_conf.product_id${vt}
-- order by product_id,dpd
;
