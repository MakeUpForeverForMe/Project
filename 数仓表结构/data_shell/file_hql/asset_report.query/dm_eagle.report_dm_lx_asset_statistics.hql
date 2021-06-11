set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

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
-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;


insert overwrite table dm_eagle.report_dm_lx_asset_statistics partition(snapshot_date,project_id)
select

  cast(m2plus_recover_amount_accum as decimal(15,4)) as m2plus_recover_amount_accum,
  cast(m2plus_recover_prin_accum as decimal(15,4)) as m2plus_recover_prin_accum,
  cast(m2plus_recover_inter_accum as decimal(15,4))as m2plus_recover_inter_accum,
  cast(m4plus_recover_amount_accum as decimal(15,4))as m4plus_recover_amount_accum,
  cast(m4plus_recover_prin_accum as decimal(15,4))as m4plus_recover_prin_accum,
  cast(m4plus_recover_inter_accum as decimal(15,4))as m4plus_recover_inter_accum,
  cast(static_30_amount as decimal(15,4)) as static_30_amount,
  cast(static_30_prin as decimal(15,4)) as static_30_prin,
  cast(static_30_iner as decimal(15,4))as static_30_iner,
  cast(static_90_amount as decimal(15,4))as static_90_amount,
  cast(static_90_prin as decimal(15,4))as static_90_prin,
  cast(static_90_iner as decimal(15,4))as static_90_iner,
  cast(overdue_remain_principal as decimal(15,4))as overdue_remain_principal,
  cast(acc_buy_back_amount as decimal(15,4))as acc_buy_back_amount,
  cast(from_unixtime(UNIX_TIMESTAMP(date_add('${ST9}',1)),'yyyy-MM-dd') as string) as snapshot_date,
  project_id
from
(
  select
  project_id ,
  sum(m2plus_recover_amount_acc) as m2plus_recover_amount_accum,
  sum(m2plus_recover_prin_acc)as m2plus_recover_prin_accum,
  sum(m2plus_recover_inter_acc) as m2plus_recover_inter_accum,
  sum(m4plus_recover_amount_acc) as m4plus_recover_amount_accum,
  sum(m4plus_recover_prin_acc) as m4plus_recover_prin_accum,
  sum(m4plus_recover_inter_acc) as m4plus_recover_inter_accum,
  sum(static_30_acc_amount) as static_30_amount,
  sum(static_acc_30_prin) as static_30_prin,
  sum(static_acc_30_iner) as static_30_iner,
  sum(static_acc_90_amount) as static_90_amount,
  sum(static_acc_90_prin) as static_90_prin,
  sum(static_acc_90_iner) as static_90_iner,
  sum(overdue_remain_principal) as overdue_remain_principal,
  sum(acc_buy_back_amount) as acc_buy_back_amount
  from
  dw.dw_report_cal_day where biz_date='${ST9}'
  group by  project_id
)temp;


