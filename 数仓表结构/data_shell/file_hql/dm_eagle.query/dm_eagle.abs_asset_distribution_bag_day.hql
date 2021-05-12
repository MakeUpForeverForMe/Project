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




-- set hivevar:ST9=2021-05-10;
-- set hivevar:bag_id=
--   select distinct bag_id
--   from dim.bag_info
--   where 1 > 0
-- ;




-- 封包时 只统计封包当天
with bill_info as (
  select
    abs_due.biz_date,
    abs_due.project_id,
    abs_due.due_bill_no,
    abs_due.remain_principal,
    abs_due.distribution_array,
    bag_due.bag_date,
    bag_due.bag_id
  from dw.abs_due_info_day_abs as abs_due
  inner join (
    select
      bag_info.project_id,
      bag_info.bag_id,
      bag_info.bag_date,
      bag_due.due_bill_no
    from (
      select
        project_id,
        bag_id,
        bag_date
      from dim.bag_info
      where 1 > 0
        and bag_id in (${bag_id})
    ) as bag_info
    inner join (
      select
        project_id,
        bag_id,
        due_bill_no
      from dim.bag_due_bill_no
    ) as bag_due
    on  bag_info.project_id  = bag_due.project_id
    and bag_info.bag_id      = bag_due.bag_id
  ) as bag_due
  on  abs_due.project_id  = bag_due.project_id
  and abs_due.due_bill_no = bag_due.due_bill_no
  and abs_due.biz_date    >= bag_due.bag_date
  and abs_due.biz_date    = '${ST9}'
  and abs_due.loan_status <> 'F'
  -- and abs_due.project_id  = 'CL202011090089'
)

-- 插入数据
insert overwrite table dm_eagle.abs_asset_distribution_bag_snapshot_day partition(biz_date,bag_id)
select
  asset_project_id                                       as project_id,
  asset_tab_name                                         as asset_tab_name,
  asset_name                                             as asset_name,
  asset_name_order                                       as asset_name_order,
  nvl(asset_remain_principal,0)                          as remain_principal,
  nvl(asset_remain_principal / total_remain_principal,0) as remain_principal_ratio,
  nvl(asset_loan_num,0)                                  as loan_num,
  nvl(asset_loan_num / total_loan_num,0)                 as loan_numratio,
  nvl(asset_remain_principal / asset_loan_num,0)         as remain_principal_loan_num_avg,
  asset_biz_date                                         as biz_date,
  asset_bag_id                                           as bag_id
from (
  select
    project_id                   as asset_project_id,
    bag_id                       as asset_bag_id,
    biz_date                     as asset_biz_date,
    split(part_info,'\\|')[0]    as asset_tab_name,
    split(part_info,'\\|')[2]    as asset_name_order,
    split(part_info,'\\|')[1]    as asset_name,
    count(distinct due_bill_no)  as asset_loan_num,
    sum(nvl(remain_principal,0)) as asset_remain_principal
  from bill_info
  lateral view explode(distribution_array) mode_info as part_info
  where 1 > 0
    and split(part_info,'\\|')[1] != 'NONE'
  group by project_id,bag_id,biz_date,split(part_info,'\\|')[0],split(part_info,'\\|')[1],split(part_info,'\\|')[2]
) as asset_total
inner join (
  select
    project_id                   as total_project_id,
    bag_id                       as total_bag_id,
    count(distinct due_bill_no)  as total_loan_num,
    sum(nvl(remain_principal,0)) as total_remain_principal
  from bill_info
  group by project_id,bag_id
) as project_total
on  asset_total.asset_project_id = project_total.total_project_id
and asset_total.asset_bag_id     = project_total.total_bag_id
-- limit 10
;
