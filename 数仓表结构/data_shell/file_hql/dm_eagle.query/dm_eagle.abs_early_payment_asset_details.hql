-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;




with loan_settle_info as (
  select
    loan_tmp.due_bill_no,
    lending_tmp.contract_no,
    repay_tmp.pre_settle_date
  from (
    select *
    from ods.loan_info
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_status = 'F' and paid_out_type = 'PRE_SETTLE'
  ) as loan_tmp
  left join ods.loan_lending as lending_tmp
  on loan_tmp.due_bill_no = lending_tmp.due_bill_no
  inner join (
    select
      due_bill_no,
      max(biz_date) as pre_settle_date
    from ods.repay_detail
    where biz_date <= '${ST9}'
    group by due_bill_no
  ) as repay_tmp
  on loan_tmp.due_bill_no = repay_tmp.due_bill_no
)
-- insert overwrite table dm_eagle.abs_early_payment_asset_details partition(biz_date='${ST9}',project_id,bag_id)
select
    'Y'                                                                                         as is_allBag,
    loan_settle.due_bill_no                                                                     as serial_number,
    loan_settle.contract_no                                                                     as contract_no,
    loan_before_settle.remain_principal                                                         as remain_principal_before_payment,
    loan_settle.pre_settle_date                                                                 as early_payment_date,
    sum(repay_detail.repay_amount)                                                              as early_payment_amount,
    sum(if(repay_detail.bnp_type = 'Pricinpal',repay_detail.repay_amount,0))                    as early_payment_principal,
    sum(if(repay_detail.bnp_type = 'Interest',repay_detail.repay_amount,0))                     as early_payment_interest,
    sum(if(repay_detail.bnp_type = 'TXNFee',repay_detail.repay_amount,0))                       as early_payment_fee,
    pro_due.project_id                                                                          as project_id,
    'default_all_bag'                                                                           as bag_id
from dim.project_due_bill_no as pro_due
inner join dim.bag_due_bill_no as bag_due
on pro_due.due_bill_no = bag_due.due_bill_no
inner join dim.bag_info as bag_info
on bag_due.bag_id = bag_info.bag_id
inner join loan_settle_info as loan_settle
on pro_due.due_bill_no = loan_settle.due_bill_no
inner join
(
    select
        loan_info.due_bill_no,
        loan_info.remain_principal
    from ods.loan_info loan_info
-- 跑历史数据可以用这行
--    from (select due_bill_no,remain_principal from ods.loan_info where s_d_date <= '${ST9}') loan_info
    inner join loan_settle_info settle_info
    on loan_info.due_bill_no = settle_info.due_bill_no
    where date_sub(settle_info.pre_settle_date,1) between s_d_date and date_sub(e_d_date,1)
) loan_before_settle
on loan_settle.due_bill_no = loan_before_settle.due_bill_no
inner join
(
    select due_bill_no,bnp_type,repay_amount,biz_date
    from ods.repay_detail
    where bnp_type in ('Pricinpal','Interest','TXNFee')
    and biz_date = '${ST9}'
) repay_detail
on loan_settle.due_bill_no = repay_detail.due_bill_no
and loan_settle.pre_settle_date = repay_detail.biz_date
group by
pro_due.project_id,
loan_settle.due_bill_no,
loan_settle.contract_no,
loan_before_settle.remain_principal,
loan_settle.pre_settle_date
union all
select
    'N'                                                                                         as is_allBag,
    loan_settle.due_bill_no                                                                     as serial_number,
    loan_settle.contract_no                                                                     as contract_no,
    loan_before_settle.remain_principal                                                         as remain_principal_before_payment,
    loan_settle.pre_settle_date                                                                 as early_payment_date,
    sum(repay_detail.repay_amount)                                                              as early_payment_amount,
    sum(if(repay_detail.bnp_type = 'Pricinpal',repay_detail.repay_amount,0))                    as early_payment_principal,
    sum(if(repay_detail.bnp_type = 'Interest',repay_detail.repay_amount,0))                     as early_payment_interest,
    sum(if(repay_detail.bnp_type = 'TXNFee',repay_detail.repay_amount,0))                       as early_payment_fee,
    pro_due.project_id                                                                          as project_id,
    bag_due.bag_id                                                                              as bag_id
from dim.project_due_bill_no as pro_due
inner join dim.bag_due_bill_no as bag_due
on pro_due.due_bill_no = bag_due.due_bill_no
inner join dim.bag_info as bag_info
on bag_due.bag_id = bag_info.bag_id
inner join loan_settle_info loan_settle
on pro_due.due_bill_no = loan_settle.due_bill_no
inner join
(
    select
        loan_info.due_bill_no,
        loan_info.remain_principal
    from ods.loan_info loan_info
-- 跑历史数据可以用这行
--    from (select due_bill_no,remain_principal from ods.loan_info where s_d_date <= '${ST9}') loan_info
    inner join loan_settle_info settle_info
    on loan_info.due_bill_no = settle_info.due_bill_no
    where date_sub(settle_info.pre_settle_date,1) between s_d_date and date_sub(e_d_date,1)
) loan_before_settle
on loan_settle.due_bill_no = loan_before_settle.due_bill_no
inner join
(
    select due_bill_no,bnp_type,repay_amount,biz_date
    from ods.repay_detail
    where bnp_type in ('Pricinpal','Interest','TXNFee')
    and biz_date = '${ST9}'
) repay_detail
on loan_settle.due_bill_no = repay_detail.due_bill_no
and loan_settle.pre_settle_date = repay_detail.biz_date
group by
pro_due.project_id,
bag_due.bag_id,
loan_settle.due_bill_no,
loan_settle.contract_no,
loan_before_settle.remain_principal,
loan_settle.pre_settle_date;
