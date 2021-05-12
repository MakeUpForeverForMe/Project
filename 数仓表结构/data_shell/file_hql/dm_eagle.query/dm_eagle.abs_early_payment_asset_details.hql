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



-- set hivevar:ST9=2018-01-01;

-- set hivevar:bag_id=
--   select distinct bag_id
--   from dim.bag_info
--   where 1 > 0
-- ;



insert overwrite table dm_eagle.abs_early_payment_asset_details partition(biz_date,project_id,bag_id)
select
  loan_settle.due_bill_no                as serial_number,
  loan_settle.contract_no                as contract_no,
  loan_settle.remain_principal_yesterday as remain_principal_before_payment,
  loan_settle.paid_out_date              as early_payment_date,
  repay_detail.early_payment_amount      as early_payment_amount,
  repay_detail.early_payment_principal   as early_payment_principal,
  repay_detail.early_payment_interest    as early_payment_interest,
  repay_detail.early_payment_fee         as early_payment_fee,
  loan_settle.biz_date                   as biz_date,
  bag_due.project_id                     as project_id,
  bag_due.bag_id                         as bag_id
from (
  select
    *
  from dim.bag_due_bill_no
  where 1 > 0
    and bag_id in (${bag_id})
) as bag_due
inner join (
  select
    biz_date,
    project_id,
    due_bill_no,
    contract_no,
    paid_out_date,
    remain_principal_yesterday
  from dw.abs_due_info_day_abs
  where 1 > 0
    and biz_date = '${ST9}'
    and loan_status = 'F'
    and paid_out_type = 'PRE_SETTLE'
) as loan_settle
on  bag_due.project_id  = loan_settle.project_id
and bag_due.due_bill_no = loan_settle.due_bill_no
inner join (
  select
    project_id,
    due_bill_no,
    sum(repay_amount)                              as early_payment_amount,
    sum(if(bnp_type = 'Pricinpal',repay_amount,0)) as early_payment_principal,
    sum(if(bnp_type = 'Interest', repay_amount,0)) as early_payment_interest,
    sum(if(bnp_type = 'TXNFee',   repay_amount,0)) as early_payment_fee
  from ods.repay_detail_abs
  where 1 > 0
    and biz_date = '${ST9}'
    and bnp_type in ('Pricinpal','Interest','TXNFee')
  group by project_id,due_bill_no
) as repay_detail
on  loan_settle.project_id  = repay_detail.project_id
and loan_settle.due_bill_no = repay_detail.due_bill_no
-- limit 10
;
