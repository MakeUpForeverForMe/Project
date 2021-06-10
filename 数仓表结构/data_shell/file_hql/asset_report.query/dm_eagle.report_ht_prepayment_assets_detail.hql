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

--set hivevar:ST9=2020-10-17;

insert overwrite table dm_eagle.report_ht_prepayment_assets_detail partition (project_id)
select
  a.due_bill_no                                                                                          as due_bill_no,
  a.name                                                                                                 as borrower_name,
  min(should_repay_date)                                                                                 as should_pay_date,
  cast(sum(should_repay_amount) as decimal(15,4))                                                        as should_pay_amount,
  a.paid_out_date                                                                                        as advance_paid_out_date,
  cast(sum(paid_amount) as decimal(15,4))                                                                as repayment_amount,
  a.project_id                                                                                           as project_id
from
(select
  a.project_id,
  a.biz_date,
  a.due_bill_no,
  a.name,
  b.paid_out_date
 from
  (select
    project_id,
    biz_date,
    due_bill_no,
    name
   from dim.dim_ht_bag_asset where biz_date='${pack_date}' ) a
   join
   (select
     a.due_bill_no,
     a.paid_out_date
    from
  (select
    due_bill_no,
    max(paid_out_date) as paid_out_date
   from ods.repay_schedule
   where '${ST9}' between s_d_date and date_sub(e_d_date,1) and product_id in (${product_id})
   and paid_out_date is not null and paid_out_type not in ('REDEMPTION') group by due_bill_no) a
   left join
   (select
     due_bill_no,
     max(should_repay_date) should_repay_date
    from ods.repay_schedule where '${ST9}' between s_d_date and date_sub(e_d_date,1) and product_id in (${product_id}) and
    paid_out_date is not null and paid_out_type not in ('REDEMPTION')
    group by due_bill_no
    ) b
    on a.due_bill_no=b.due_bill_no
    where a.paid_out_date<b.should_repay_date) b
 on a.due_bill_no=b.due_bill_no) a
left join
(select
  due_bill_no,
  paid_out_date,
  should_repay_date,
  should_repay_amount,
  paid_amount
from ods.repay_schedule where '${ST9}' between s_d_date and date_sub(e_d_date,1) and product_id in (${product_id}) and loan_term!=0
and paid_out_type not in ('REDEMPTION') ) b
on a.due_bill_no=b.due_bill_no and a.paid_out_date=b.paid_out_date
group by a.project_id,
  a.due_bill_no,
  a.name,
  a.paid_out_date,
  a.biz_date