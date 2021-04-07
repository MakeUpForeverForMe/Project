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


insert overwrite table dm_eagle.eagle_asset_change_comp_t1 partition(biz_date)
select distinct
  b.capital_id,
  b.channel_id,
  a.project_id,
  a.yesterday_remain_sum,
  a.loan_sum_daily,
  a.repay_sum_daily,
  a.repay_other_sum_daily,
  a.today_remain_sum,
  a.d_date
from dm.dm_watch_asset_change_comp_t1 as a
left join (
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
) as b
on a.project_id = b.project_id
;


insert overwrite table dm_eagle.eagle_asset_change_t1 partition(biz_date)
select distinct
  b.capital_id,
  b.channel_id,
  a.project_id,
  a.yesterday_remain_sum,
  a.loan_sum_daily,
  a.repay_sum_daily,
  a.repay_other_sum_daily,
  a.today_remain_sum,
  a.d_date
from dm.dm_watch_asset_change_t1 as a
left join (
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
) as b
on a.project_id = b.project_id
;


insert overwrite table dm_eagle.eagle_asset_change_comp partition(biz_date)
select distinct
  b.capital_id,
  b.channel_id,
  a.project_id,
  a.yesterday_remain_sum,
  a.loan_sum_daily,
  a.repay_sum_daily,
  a.repay_other_sum_daily,
  a.today_remain_sum,
  a.d_date
from dm.dm_watch_asset_change_comp as a
left join (
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
) as b
on a.project_id = b.project_id
;


insert overwrite table dm_eagle.eagle_asset_change partition(biz_date)
select distinct
  b.capital_id,b.channel_id,
  a.project_id,
  a.yesterday_remain_sum,
  a.loan_sum_daily,
  a.repay_sum_daily,
  a.repay_other_sum_daily,
  a.today_remain_sum,
  a.d_date
from dm.dm_watch_asset_change as a
left join (
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
) as b
on a.project_id = b.project_id
;


insert overwrite table dm_eagle.eagle_unreach_funds partition(biz_date)
select distinct
  b.capital_id,
  b.channel_id,
  a.project_id,
  a.trade_yesterday_bal,
  a.repay_today_bal,
  a.repay_sum_daily,
  a.trade_today_bal,
  a.d_date
from dm.dm_watch_unreach_funds as a
left join (
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
) as b
on a.project_id = b.project_id
;


insert overwrite table dm_eagle.eagle_asset_comp_info partition(biz_date)
select distinct
  b.capital_id,
  b.channel_id,
  a.project_id,
  a.repay_sum_daily,
  a.repay_way,
  a.amount_type,
  a.amount,
  a.d_date
from dm.dm_watch_asset_comp_info as a
left join (
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
) as b
on a.project_id = b.project_id
;


insert overwrite table dm_eagle.eagle_acct_cost partition(biz_date)
select distinct
  b.capital_id,
  b.channel_id,
  a.project_id,
  a.acct_total_fee,
  a.fee_type,
  a.amount,
  a.d_date
from dm.dm_watch_acct_cost as a
left join (
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
) as b
on a.project_id = b.project_id
;


insert overwrite table dm_eagle.eagle_funds partition(biz_date)
select distinct
  b.capital_id,
  b.channel_id,
  a.project_id,
  a.trade_yesterday_bal,
  a.loan_today_bal,
  a.repay_today_bal,
  a.acct_int,
  a.acct_total_fee,
  a.invest_cash,
  a.trade_today_bal,
  a.d_date
from dm.dm_watch_funds as a
left join (
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
) as b
on a.project_id = b.project_id
;


insert overwrite table dm_eagle.eagle_repayment_detail partition(biz_date)
select distinct
  b.capital_id,
  b.channel_id,
  a.project_id,
  a.repay_today_bal,
  a.repay_type,
  a.amount,
  a.d_date
from dm.dm_watch_repayment_detail as a
left join (
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
) as b
on a.project_id = b.project_id
;


