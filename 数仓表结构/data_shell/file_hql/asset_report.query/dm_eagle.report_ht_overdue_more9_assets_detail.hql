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

set hivevar:pack_date=2020-10-17;
set hivevar:product_id='001601','001602','001603';


insert overwrite table dm_eagle.report_ht_overdue_more9_assets_detail  partition (snapshot_date,project_id)
select
 a.due_bill_no                                                                                                                as due_bill_no,
 a.name                                                                                                                       as borrower_name,
 b.overdue_date_start                                                                                                         as overdue_start_date,
 cast((nvl(overdue_principal,0)+nvl(overdue_interest,0)) as decimal(15,4))                                                    as overdue_amount,
 '${ST9}'                                                                                                                 as snapshot_date,
  a.project_id                                                                                                                 as project_id
from
  (select
   project_id,
   biz_date,
   due_bill_no,
   name
  from dim.dim_ht_bag_asset where biz_date='${pack_date}') a
  join
  (select
    due_bill_no,
    overdue_date_start,
    overdue_principal,
    overdue_interest
   from ods.loan_info where '${ST9}' between s_d_date and date_sub(e_d_date,1) and product_id in (${product_id}) and overdue_days>9) b
  on a.due_bill_no=b.due_bill_no
