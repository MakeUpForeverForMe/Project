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


insert overwrite table dm_eagle.report_dm_lx_asset_summary_statistics partition(snapshot_date,project_id)
  select

  cast(m2plus_recover_amount as DECIMAL(15,4)) as m2plus_recover_amount ,
  cast(m2plus_recover_prin as DECIMAL(15,4)) as m2plus_recover_prin ,
  cast(m2plus_recover_inter as DECIMAL(15,4))as m2plus_recover_inter ,
  cast(m4plus_recover_amount as DECIMAL(15,4))as m4plus_recover_amount ,
  cast(m4plus_recover_prin as DECIMAL(15,4)) as m4plus_recover_prin,
  cast(m4plus_recover_inter as DECIMAL(15,4))as  m4plus_recover_inter,
  cast(m2plus_num as Int) as m2plus_num,
  cast( m2plus_amount as DECIMAL(15,4)) as m2plus_amount,
  cast( m2plus_prin as DECIMAL(15,4))as  m2plus_prin,
  cast(m2plus_inter as DECIMAL(15,4)) as m2plus_inter ,
  cast(m4plus_num as Int) as m4plus_num,
  cast( m4plus_amount as DECIMAL(15,4))  as m4plus_amount,
  cast( m4plus_prin as DECIMAL(15,4)) as m4plus_prin,
  cast( m4plus_inter as DECIMAL(15,4)) as  m4plus_inter,
  cast( static_30_new_amount as DECIMAL(15,4))  as static_30_new_amount,
  cast(static_30_new_prin as DECIMAL(15,4)) as  static_30_new_prin,
  cast(static_30_new_inter as DECIMAL(15,4))as static_30_new_inter ,
  cast( static_90_new_amount as DECIMAL(15,4))as  static_90_new_amount,
  cast(static_90_new_prin as DECIMAL(15,4)) as static_90_new_prin,
  cast( static_90_new_inter as DECIMAL(15,4))as static_90_new_inter ,
  cast(from_unixtime(UNIX_TIMESTAMP(date_add('${ST9}',1)),'yyyy-MM-dd') as string) as snapshot_date,
  project_id
  from
  (
  select
   project_id,
  sum(m2plus_recover_amount) as m2plus_recover_amount,
  sum(m2plus_recover_prin) as m2plus_recover_prin,
  sum(m2plus_recover_inter) as m2plus_recover_inter,
  sum(m4plus_recover_amount) as m4plus_recover_amount,
  sum(m4plus_recover_prin) as m4plus_recover_prin,
  sum(m4plus_recover_inter) as m4plus_recover_inter,
  sum(m2plus_num) as m2plus_num,
  sum(m2plus_amount) as m2plus_amount,
  sum(m2plus_prin) as m2plus_prin,
  sum(m2plus_inter) as m2plus_inter,
  sum(m4plus_num) as m4plus_num,
  sum(m4plus_amount) as m4plus_amount,
  sum(m4plus_prin) as m4plus_prin,
  sum(m4plus_inter) as m4plus_inter,
  sum(static_30_new_amount) as static_30_new_amount,
  sum(static_30_new_prin) as static_30_new_prin,
  sum(static_30_new_inter) as static_30_new_inter,
  sum(static_90_new_amount) as static_90_new_amount,
  sum(static_90_new_prin) as static_90_new_prin,
  sum(static_90_new_inter) as static_90_new_inter
  from  dw.dw_report_cal_day where biz_date='${ST9}'
  group by project_id
  )temp ;
