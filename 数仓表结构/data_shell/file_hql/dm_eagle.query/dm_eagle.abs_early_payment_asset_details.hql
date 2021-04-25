set hive.execution.engine=spark;
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
set hive.vectorized.execution.enabled=false;




set hivevar:ST9=2021-04-01;




with loan_settle_info as (
  select
    loan_tmp.project_id,
    loan_tmp.due_bill_no,
    lending_tmp.contract_no,
    repay_tmp.pre_settle_date
  from (
    select *
    from ods.loan_info_abs
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_status = 'F'
      and paid_out_type = 'PRE_SETTLE'
  ) as loan_tmp
  left join ods.loan_lending_abs as lending_tmp
  on  loan_tmp.project_id  = lending_tmp.project_id
  and loan_tmp.due_bill_no = lending_tmp.due_bill_no
  inner join (
    select
      project_id,
      due_bill_no,
      max(biz_date) as pre_settle_date
    from ods.repay_detail_abs
    where biz_date <= '${ST9}'
    group by project_id,due_bill_no
  ) as repay_tmp
  on  loan_tmp.project_id  = repay_tmp.project_id
  and loan_tmp.due_bill_no = repay_tmp.due_bill_no
)

-- insert overwrite table dm_eagle.abs_early_payment_asset_details partition(biz_date,project_id,bag_id)
select
  'Y'                                                                      as is_allBag,
  loan_settle.due_bill_no                                                  as serial_number,
  loan_settle.contract_no                                                  as contract_no,
  loan_before_settle.remain_principal                                      as remain_principal_before_payment,
  loan_settle.pre_settle_date                                              as early_payment_date,
  sum(repay_detail.repay_amount)                                           as early_payment_amount,
  sum(if(repay_detail.bnp_type = 'Pricinpal',repay_detail.repay_amount,0)) as early_payment_principal,
  sum(if(repay_detail.bnp_type = 'Interest',repay_detail.repay_amount,0))  as early_payment_interest,
  sum(if(repay_detail.bnp_type = 'TXNFee',repay_detail.repay_amount,0))    as early_payment_fee,
  '${ST9}'                                                                 as biz_date,
  bag_due.project_id                                                       as project_id,
  'default_all_bag'                                                        as bag_id
from dim.bag_due_bill_no as bag_due
inner join loan_settle_info as loan_settle
on  bag_due.project_id  = loan_settle.project_id
and bag_due.due_bill_no = loan_settle.due_bill_no
inner join (
  select
    loan_info.project_id,
    loan_info.due_bill_no,
    loan_info.remain_principal
  from ods.loan_info_abs as loan_info
  -- 跑历史数据可以用这行
  -- from (select due_bill_no,remain_principal from ods.loan_info where s_d_date <= '${ST9}') loan_info
  inner join loan_settle_info as settle_info
  on  loan_info.project_id  = settle_info.project_id
  and loan_info.due_bill_no = settle_info.due_bill_no
  where date_sub(settle_info.pre_settle_date,1) between s_d_date and date_sub(e_d_date,1) -- 取提前结清的前一天的剩余本金，要不然提前结清后，当天的剩余本金就为0
) as loan_before_settle
on  loan_settle.project_id  = loan_before_settle.project_id
and loan_settle.due_bill_no = loan_before_settle.due_bill_no
inner join (
  select project_id,due_bill_no,bnp_type,repay_amount,biz_date
  from ods.repay_detail_abs
  where bnp_type in ('Pricinpal','Interest','TXNFee')
    and biz_date = '${ST9}'
) as repay_detail
on  loan_settle.project_id      = repay_detail.project_id
and loan_settle.due_bill_no     = repay_detail.due_bill_no
and loan_settle.pre_settle_date = repay_detail.biz_date
group by
  bag_due.project_id,
  loan_settle.due_bill_no,
  loan_settle.contract_no,
  loan_before_settle.remain_principal,
  loan_settle.pre_settle_date
union all
select
  'N'                                                                      as is_allBag,
  loan_settle.due_bill_no                                                  as serial_number,
  loan_settle.contract_no                                                  as contract_no,
  loan_before_settle.remain_principal                                      as remain_principal_before_payment,
  loan_settle.pre_settle_date                                              as early_payment_date,
  sum(repay_detail.repay_amount)                                           as early_payment_amount,
  sum(if(repay_detail.bnp_type = 'Pricinpal',repay_detail.repay_amount,0)) as early_payment_principal,
  sum(if(repay_detail.bnp_type = 'Interest',repay_detail.repay_amount,0))  as early_payment_interest,
  sum(if(repay_detail.bnp_type = 'TXNFee',repay_detail.repay_amount,0))    as early_payment_fee,
  '${ST9}'                                                                 as biz_date,
  bag_due.project_id                                                       as project_id,
  bag_due.bag_id                                                           as bag_id
from dim.bag_due_bill_no as bag_due
inner join loan_settle_info as loan_settle
on  bag_due.project_id = loan_settle.project_id
and bag_due.due_bill_no = loan_settle.due_bill_no
inner join (
  select
    loan_info.project_id,
    loan_info.due_bill_no,
    loan_info.remain_principal
  from ods.loan_info_abs as loan_info
  -- 跑历史数据可以用这行
  -- from (select due_bill_no,remain_principal from ods.loan_info where s_d_date <= '${ST9}') loan_info
  inner join loan_settle_info as settle_info
  on  loan_info.project_id  = settle_info.project_id
  and loan_info.due_bill_no = settle_info.due_bill_no
  where date_sub(settle_info.pre_settle_date,1) between s_d_date and date_sub(e_d_date,1)
) as loan_before_settle
on  loan_settle.project_id  = loan_before_settle.project_id
and loan_settle.due_bill_no = loan_before_settle.due_bill_no
inner join (
  select project_id,due_bill_no,bnp_type,repay_amount,biz_date
  from ods.repay_detail_abs
  where bnp_type in ('Pricinpal','Interest','TXNFee')
    and biz_date = '${ST9}'
) as repay_detail
on  loan_settle.project_id      = repay_detail.project_id
and loan_settle.due_bill_no     = repay_detail.due_bill_no
and loan_settle.pre_settle_date = repay_detail.biz_date
group by
  bag_due.project_id,
  bag_due.bag_id,
  loan_settle.due_bill_no,
  loan_settle.contract_no,
  loan_before_settle.remain_principal,
  loan_settle.pre_settle_date
limit 10
;
