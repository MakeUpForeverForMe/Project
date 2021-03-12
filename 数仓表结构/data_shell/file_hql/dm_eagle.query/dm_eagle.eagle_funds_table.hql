set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;         -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;             -- 关闭自动 MapJoin
set hive.mapjoin.optimized.hashtable=false;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;



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
left join dim_new.biz_conf as b
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
left join dim_new.biz_conf as b
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
left join dim_new.biz_conf as b
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
left join dim_new.biz_conf as b
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
left join dim_new.biz_conf as b
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
left join dim_new.biz_conf as b
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
left join dim_new.biz_conf as b
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
left join dim_new.biz_conf as b
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
left join dim_new.biz_conf as b
on a.project_id = b.project_id
;


