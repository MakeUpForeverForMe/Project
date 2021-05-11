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


set hivevar:ST9=2021-05-01;

-- 封包时 只统计封包当天
with
bill_info as (
  select
    biz_date,
    project_id,
    due_bill_no,
    remain_principal,
    distribution_array
  from dw.abs_due_info_day_abs
  where 1 > 0
    and biz_date = '${ST9}'
    and loan_status <> 'F'
),

--汇总数据--项目下所有有包的借据
project_total_bag_bill as (
  select
    bill_info.project_id,
    bag_info.bag_id,
    sum(remain_principal) as total_remain_principal,
    count(bill_info.due_bill_no)    as total_bill
  from bill_info
  join (
    select
      bag.bag_id,
      bag.project_id,
      bag_bill.due_bill_no
    from (
      select
        bag_id,
        project_id
      from dim.bag_info
      where bag_date = '${ST9}'
    ) as bag
    join dim.bag_due_bill_no as bag_bill
    on bag.bag_id = bag_bill.bag_id
  ) as bag_info
  on bill_info.due_bill_no=bag_info.due_bill_no
  and bill_info.project_id=bag_info.project_id
  group by bill_info.project_id,bag_id
)


-- 插入数据
insert overwrite table dm_eagle.abs_asset_distribution_bag_snapshot_day partition(biz_date,bag_id)
select
  tmp.project_id                                                    as project_id,
  asset_tab_name                                                    as asset_tab_name,
  asset_name                                                        as asset_name,
  asset_name_order                                                  as asset_name_order,
  sum(remain_principal)                                             as remain_principal,
  sum(remain_principal) / max(project_total.total_remain_principal) as remain_principal_ratio,
  count(tmp.due_bill_no)                                            as loan_num,
  count(tmp.due_bill_no) / max(project_total.total_bill)            as loan_numratio,
  sum(remain_principal) / count(tmp.due_bill_no)                    as remain_principal_loan_num_avg,
  '${ST9}'                                                          as biz_date,
 bag_info.bag_id                                                    as bag_id
from (
  select
    project_id,
    due_bill_no,
    remain_principal,
    split(part_info,'\\|')[0] as asset_tab_name,
    split(part_info,'\\|')[1] as asset_name,
    split(part_info,'\\|')[2] as asset_name_order
  from dw.abs_due_info_day_abs
  lateral view explode(distribution_array) mode_info as part_info
  where 1 > 0
    and biz_date = '${ST9}'
    and project_id = '001601'
    and loan_status <> 'F'
) as tmp
join (
  select
    bag.bag_id,
    bag.project_id,
    bag_bill.due_bill_no
  from (
    select
      bag_id,
      project_id
    from dim.bag_info
    where bag_date = '${ST9}'
  ) as bag
  join dim.bag_due_bill_no as bag_bill
  on bag.bag_id = bag_bill.bag_id
) as bag_info
on tmp.due_bill_no=bag_info.due_bill_no and tmp.project_id=bag_info.project_id
inner join project_total_bag_bill as project_total
on  tmp.project_id = project_total.project_id
and bag_info.bag_id     = project_total.bag_id
where asset_name != 'NONE'
group by asset_tab_name,asset_name,asset_name_order,tmp.project_id,bag_info.bag_id
-- limit 10
;
