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



-- set hivevar:ST9=2018-11-10;

-- set hivevar:bag_id=
--   select distinct bag_id
--   from dim.bag_info
--   where 1 > 0
-- ;



insert overwrite table dm_eagle.abs_early_payment_asset_statistic partition(biz_date,project_id,bag_id)
select
  'Y'                                                                                                                                                as is_allBag,
  sum(if(bag_total.pre_settle_due_bill_no            is not null and repay_detail.due_bill_no      is not null,repay_detail.repay_amount,     0))    as early_payment_principal,
  count(if(bag_total.pre_settle_due_bill_no          is not null and repay_detail.due_bill_no      is not null,bag_total.due_bill_no,         null)) as early_payment_asset_count,
  count(distinct if(bag_total.pre_settle_due_bill_no is not null and repay_detail.due_bill_no      is not null,bag_total.user_hash_no,        null)) as early_payment_cust_count,
  sum(if(bag_total.pre_settle_due_bill_no            is not null and repay_detail_accu.due_bill_no is not null,repay_detail_accu.repay_amount,0))    as early_payment_principal_accu,
  count(if(bag_total.pre_settle_due_bill_no          is not null and repay_detail_accu.due_bill_no is not null,bag_total.due_bill_no,         null)) as early_payment_asset_count_accu,
  count(distinct if(bag_total.pre_settle_due_bill_no is not null and repay_detail_accu.due_bill_no is not null,bag_total.user_hash_no,        null)) as early_payment_cust_count_accu,
  sum(bag_total.unsettled_remain_principal)                                                                                                          as remain_principal,
  count(bag_total.unsettled_due_bill_no)                                                                                                             as asset_count,
  count(distinct bag_total.unsettled_user_hash_no)                                                                                                   as cust_count,
  min(bag_total.bag_date)                                                                                                                            as bag_date,
  sum(bag_total.package_remain_principal)                                                                                                            as package_remain_principal,
  count(bag_total.due_bill_no)                                                                                                                       as package_asset_count,
  count(distinct bag_total.user_hash_no)                                                                                                             as package_cust_count,
  bag_total.biz_date                                                                                                                                 as biz_date,
  bag_total.project_id                                                                                                                               as project_id,
  'default_all_bag'                                                                                                                                  as bag_id
from (
  select
    loan.biz_date                                                                               as biz_date,
    loan.bag_date                                                                               as bag_date,
    loan.project_id                                                                             as project_id,
    loan.bag_id                                                                                 as bag_id,
    loan.due_bill_no                                                                            as due_bill_no,
    loan.package_remain_principal                                                               as package_remain_principal,
    customer.user_hash_no                                                                       as user_hash_no,

    if(loan.loan_status <> 'F',loan.due_bill_no,     null)                                      as unsettled_due_bill_no,
    if(loan.loan_status <> 'F',customer.user_hash_no,null)                                      as unsettled_user_hash_no,
    if(loan.loan_status <> 'F',loan.remain_principal,0)                                         as unsettled_remain_principal,

    if(loan.loan_status = 'F' and loan.paid_out_type = 'PRE_SETTLE',loan.due_bill_no,     null) as pre_settle_due_bill_no,
    if(loan.loan_status = 'F' and loan.paid_out_type = 'PRE_SETTLE',customer.user_hash_no,null) as pre_settle_user_hash_no
  from (
    select
      '${ST9}' as biz_date,
      bag_info.bag_date as bag_date,
      bag_info.bag_id,
      bag_due_bill_no.package_remain_principal,
      loan_info_abs.project_id,
      loan_info_abs.due_bill_no,
      loan_info_abs.loan_status,
      loan_info_abs.paid_out_type,
      loan_info_abs.remain_principal
    from (
      select
        *
      from dim.bag_info
      where 1 > 0
        and bag_id in (${bag_id})
    ) as bag_info
    inner join dim.bag_due_bill_no
    on  bag_info.project_id = bag_due_bill_no.project_id
    and bag_info.bag_id     = bag_due_bill_no.bag_id
    inner join ods.loan_info_abs
    on  bag_due_bill_no.project_id  = loan_info_abs.project_id
    and bag_due_bill_no.due_bill_no = loan_info_abs.due_bill_no
    and '${ST9}' between loan_info_abs.s_d_date and date_sub(loan_info_abs.e_d_date,1)
  ) as loan
  inner join (
    select
      project_id,
      due_bill_no,
      user_hash_no
    from ods.customer_info_abs
  ) as customer
  on  loan.project_id  = customer.project_id
  and loan.due_bill_no = customer.due_bill_no
) as bag_total
left join (
  select
    project_id,
    due_bill_no,
    sum(repay_amount) as repay_amount
  from ods.repay_detail_abs
  where 1 > 0
    and biz_date = '${ST9}'
    and bnp_type in ('Pricinpal')
  group by project_id,due_bill_no
) as repay_detail
on  bag_total.project_id  = repay_detail.project_id
and bag_total.due_bill_no = repay_detail.due_bill_no
left join (
  select
    project_id,
    due_bill_no,
    sum(repay_amount) as repay_amount
  from ods.repay_detail_abs
  where 1 > 0
    and biz_date <= '${ST9}'
    and bnp_type in ('Pricinpal')
  group by project_id,due_bill_no
) as repay_detail_accu
on  bag_total.project_id  = repay_detail_accu.project_id
and bag_total.due_bill_no = repay_detail_accu.due_bill_no
group by bag_total.biz_date,bag_total.project_id
-- limit 10
;




insert overwrite table dm_eagle.abs_early_payment_asset_statistic partition(biz_date,project_id,bag_id)
select
  'N'                                                                                                                                                as is_allBag,
  sum(if(bag_total.pre_settle_due_bill_no            is not null and repay_detail.due_bill_no      is not null,repay_detail.repay_amount,     0))    as early_payment_principal,
  count(if(bag_total.pre_settle_due_bill_no          is not null and repay_detail.due_bill_no      is not null,bag_total.due_bill_no,         null)) as early_payment_asset_count,
  count(distinct if(bag_total.pre_settle_due_bill_no is not null and repay_detail.due_bill_no      is not null,bag_total.user_hash_no,        null)) as early_payment_cust_count,
  sum(if(bag_total.pre_settle_due_bill_no            is not null and repay_detail_accu.due_bill_no is not null,repay_detail_accu.repay_amount,0))    as early_payment_principal_accu,
  count(if(bag_total.pre_settle_due_bill_no          is not null and repay_detail_accu.due_bill_no is not null,bag_total.due_bill_no,         null)) as early_payment_asset_count_accu,
  count(distinct if(bag_total.pre_settle_due_bill_no is not null and repay_detail_accu.due_bill_no is not null,bag_total.user_hash_no,        null)) as early_payment_cust_count_accu,
  sum(bag_total.unsettled_remain_principal)                                                                                                          as remain_principal,
  count(bag_total.unsettled_due_bill_no)                                                                                                             as asset_count,
  count(distinct bag_total.unsettled_user_hash_no)                                                                                                   as cust_count,
  min(bag_total.bag_date)                                                                                                                            as bag_date,
  sum(bag_total.package_remain_principal)                                                                                                            as package_remain_principal,
  count(bag_total.due_bill_no)                                                                                                                       as package_asset_count,
  count(distinct bag_total.user_hash_no)                                                                                                             as package_cust_count,
  bag_total.biz_date                                                                                                                                 as biz_date,
  bag_total.project_id                                                                                                                               as project_id,
  bag_total.bag_id                                                                                                                                   as bag_id
from (
  select
    loan.biz_date                                                                               as biz_date,
    loan.bag_date                                                                               as bag_date,
    loan.project_id                                                                             as project_id,
    loan.bag_id                                                                                 as bag_id,
    loan.due_bill_no                                                                            as due_bill_no,
    loan.package_remain_principal                                                               as package_remain_principal,
    customer.user_hash_no                                                                       as user_hash_no,

    if(loan.loan_status <> 'F',loan.due_bill_no,     null)                                      as unsettled_due_bill_no,
    if(loan.loan_status <> 'F',customer.user_hash_no,null)                                      as unsettled_user_hash_no,
    if(loan.loan_status <> 'F',loan.remain_principal,0)                                         as unsettled_remain_principal,

    if(loan.loan_status = 'F' and loan.paid_out_type = 'PRE_SETTLE',loan.due_bill_no,     null) as pre_settle_due_bill_no,
    if(loan.loan_status = 'F' and loan.paid_out_type = 'PRE_SETTLE',customer.user_hash_no,null) as pre_settle_user_hash_no
  from (
    select
      '${ST9}' as biz_date,
      bag_info.bag_date as bag_date,
      bag_info.bag_id,
      bag_due_bill_no.package_remain_principal,
      loan_info_abs.project_id,
      loan_info_abs.due_bill_no,
      loan_info_abs.loan_status,
      loan_info_abs.paid_out_type,
      loan_info_abs.remain_principal
    from (
      select
        *
      from dim.bag_info
      where 1 > 0
        and bag_id in (${bag_id})
    ) as bag_info
    inner join dim.bag_due_bill_no
    on  bag_info.project_id = bag_due_bill_no.project_id
    and bag_info.bag_id     = bag_due_bill_no.bag_id
    inner join ods.loan_info_abs
    on  bag_due_bill_no.project_id  = loan_info_abs.project_id
    and bag_due_bill_no.due_bill_no = loan_info_abs.due_bill_no
    and '${ST9}' between loan_info_abs.s_d_date and date_sub(loan_info_abs.e_d_date,1)
  ) as loan
  inner join (
    select
      project_id,
      due_bill_no,
      user_hash_no
    from ods.customer_info_abs
  ) as customer
  on  loan.project_id  = customer.project_id
  and loan.due_bill_no = customer.due_bill_no
) as bag_total
left join (
  select
    project_id,
    due_bill_no,
    sum(repay_amount) as repay_amount
  from ods.repay_detail_abs
  where 1 > 0
    and biz_date = '${ST9}'
    and bnp_type in ('Pricinpal')
  group by project_id,due_bill_no
) as repay_detail
on  bag_total.project_id  = repay_detail.project_id
and bag_total.due_bill_no = repay_detail.due_bill_no
left join (
  select
    project_id,
    due_bill_no,
    sum(repay_amount) as repay_amount
  from ods.repay_detail_abs
  where 1 > 0
    and biz_date <= '${ST9}'
    and bnp_type in ('Pricinpal')
  group by project_id,due_bill_no
) as repay_detail_accu
on  bag_total.project_id  = repay_detail_accu.project_id
and bag_total.due_bill_no = repay_detail_accu.due_bill_no
group by bag_total.biz_date,bag_total.project_id,bag_total.bag_id
-- limit 10
;
