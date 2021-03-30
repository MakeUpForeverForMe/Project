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

set hive.auto.convert.join=false;                    -- 关闭自动 MapJoin
set hive.auto.convert.join.noconditionaltask=false;  -- 关闭自动 MapJoin


set hivevar:ST9=2020-10-15;


-- insert overwrite table dm_eagle.abs_early_payment_asset_statistic partition(biz_date,project_id,bag_id)
select
  'Y'                                              as is_allBag,
  sum(nvl(cur_pre_settle.repay_amount,0))          as early_payment_principal,
  count(distinct cur_pre_settle.due_bill_no)       as early_payment_asset_count,
  count(distinct cur_pre_settle.user_hash_no)      as early_payment_cust_count,
  sum(nvl(accu_pre_settle.repay_amount,0))         as early_payment_principal_accu,
  count(distinct accu_pre_settle.due_bill_no)      as early_payment_asset_count_accu,
  count(distinct accu_pre_settle.user_hash_no)     as early_payment_cust_count_accu,
  sum(bag_total.unsettled_remain_principal)        as remain_principal,
  count(bag_total.unsettled_due_bill_no)           as asset_count,
  count(distinct bag_total.unsettled_user_hash_no) as cust_count,
  min(bag_info.bag_date)                           as bag_date,
  sum(bag_info.bag_remain_principal)               as package_remain_principal,
  count(bag_total.due_bill_no)                     as package_asset_count,
  count(distinct bag_total.user_hash_no)           as package_cust_count,
  '${ST9}'                                         as biz_date,
  bag_info.project_id                              as project_id,
  'default_all_bag'                                as bag_id
from dim.bag_due_bill_no as bag_due
inner join dim.bag_info as bag_info
on  bag_due.bag_id     = bag_info.bag_id
and bag_due.project_id = bag_info.project_id
inner join (
  select
    total.project_id,
    total.due_bill_no,
    cust.user_hash_no,

    if(total.loan_status <> 'F',total.due_bill_no,null)   as unsettled_due_bill_no,
    if(total.loan_status <> 'F',cust.user_hash_no,null)   as unsettled_user_hash_no,
    if(total.loan_status <> 'F',total.remain_principal,0) as unsettled_remain_principal
  from (
    select
      project_id,
      due_bill_no,
      remain_principal,
      loan_status
    from ods.loan_info_abs
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
  ) as total
  left join ods.customer_info_abs as cust
  on  total.project_id  = cust.project_id
  and total.due_bill_no = cust.due_bill_no
) as bag_total
on  bag_info.project_id = bag_total.project_id
and bag_due.due_bill_no = bag_total.due_bill_no
left join (
  select
    repay_detail.project_id,
    repay_detail.due_bill_no,
    repay_detail.repay_amount,
    cust.user_hash_no
  from (
    select
      project_id,
      due_bill_no,
      repay_amount
    from ods.repay_detail_abs
    where 1 > 0
      and biz_date = '${ST9}'
      and bnp_type = 'Pricinpal'
  ) as repay_detail
  inner join (
    select
      project_id,
      due_bill_no
    from ods.loan_info_abs
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_status = 'F'
      and paid_out_type = 'PRE_SETTLE'
  ) as loan_pre_settle
  on  repay_detail.project_id  = loan_pre_settle.project_id
  and repay_detail.due_bill_no = loan_pre_settle.due_bill_no
  left join ods.customer_info_abs as cust
  on  repay_detail.project_id  = cust.project_id
  and repay_detail.due_bill_no = cust.due_bill_no
) as cur_pre_settle
on  bag_info.project_id = cur_pre_settle.project_id
and bag_due.due_bill_no = cur_pre_settle.due_bill_no
left join (
  select
    repay_detail.project_id,
    repay_detail.due_bill_no,
    repay_detail.repay_amount,
    cust.user_hash_no
  from (
    select
      project_id,
      due_bill_no,
      repay_amount
    from ods.repay_detail_abs
    where 1 > 0
      and biz_date <= '${ST9}'
      and bnp_type = 'Pricinpal'
  ) as repay_detail
  inner join (
    select
      project_id,
      due_bill_no
    from ods.loan_info_abs
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_status = 'F'
      and paid_out_type = 'PRE_SETTLE'
  ) as loan_pre_settle
  on  repay_detail.project_id  = loan_pre_settle.project_id
  and repay_detail.due_bill_no = loan_pre_settle.due_bill_no
  left join ods.customer_info_abs cust
  on  repay_detail.project_id  = cust.project_id
  and repay_detail.due_bill_no = cust.due_bill_no
) as accu_pre_settle
on  bag_info.project_id = accu_pre_settle.project_id
and bag_due.due_bill_no = accu_pre_settle.due_bill_no
group by bag_info.project_id
-- union all
-- select
--   'N'                                                                            as is_allBag,
--   sum(if(cur_pre_settle.bnp_type = 'Pricinpal',cur_pre_settle.repay_amount,0))   as early_payment_principal,
--   count(distinct cur_pre_settle.due_bill_no)                                     as early_payment_asset_count,
--   count(distinct cur_pre_settle.user_hash_no)                                    as early_payment_cust_count,
--   sum(if(accu_pre_settle.bnp_type = 'Pricinpal',accu_pre_settle.repay_amount,0)) as early_payment_principal_accu,
--   count(distinct accu_pre_settle.due_bill_no)                                    as early_payment_asset_count_accu,
--   count(distinct accu_pre_settle.user_hash_no)                                   as early_payment_cust_count_accu,
--   sum(unsettle.unsettled_remain_principal)                                       as remain_principal,
--   count(unsettle.due_bill_no)                                                    as asset_count,
--   count(distinct unsettle.user_hash_no)                                          as cust_count,
--   bag_info.bag_date                                                              as bag_date,
--   cast(bag_info.bag_remain_principal as decimal(15,4))                           as package_remain_principal,
--   count(bag_total.due_bill_no)                                                   as package_asset_count,
--   count(distinct bag_total.user_hash_no)                                         as package_cust_count,
--   '${ST9}'                                                                       as biz_date,
--   bag_info.project_id                                                            as project_id,
--   bag_due.bag_id                                                                 as bag_id
-- from dim.bag_due_bill_no as bag_due
-- inner join dim.bag_info as bag_info
-- on  bag_due.bag_id     = bag_info.bag_id
-- and bag_due.project_id = bag_info.project_id
-- inner join (
--   select
--     total.project_id,
--     total.due_bill_no,
--     cust.user_hash_no
--   from (
--     select
--       project_id,
--       due_bill_no
--     from ods.loan_info_abs
--     where '${ST9}' between s_d_date and date_sub(e_d_date,1)
--   ) as total
--   left join ods.customer_info_abs as cust
--   on  total.project_id  = cust.project_id
--   and total.due_bill_no = cust.due_bill_no
-- ) as bag_total
-- on bag_due.due_bill_no = bag_total.due_bill_no
-- left join (
--   select
--     unsettled_loan.project_id,
--     unsettled_loan.due_bill_no,
--     unsettled_loan.unsettled_remain_principal,
--     cust.user_hash_no
--   from (
--     select
--       project_id,
--       due_bill_no,
--       sum(remain_principal) as unsettled_remain_principal
--     from ods.loan_info_abs
--     where '${ST9}' between s_d_date and date_sub(e_d_date,1)
--       and loan_status <> 'F'
--     group by project_id,due_bill_no
--   ) as unsettled_loan
--   left join ods.customer_info_abs cust
--   on  unsettled_loan.project_id  = cust.project_id
--   and unsettled_loan.due_bill_no = cust.due_bill_no
-- ) as unsettle
-- on  bag_info.project_id = unsettle.project_id
-- and bag_due.due_bill_no = unsettle.due_bill_no
-- left join (
--   select
--     repay_detail.project_id,
--     repay_detail.due_bill_no,
--     repay_detail.biz_date,
--     repay_detail.bnp_type,
--     repay_detail.repay_amount,
--     settle_date.pre_settle_date,
--     cust.user_hash_no
--   from (
--     select
--       project_id,
--       due_bill_no,
--       bnp_type,
--       repay_amount,
--       biz_date
--     from ods.repay_detail_abs
--     where biz_date = '${ST9}'
--   ) as repay_detail
--   inner join (
--     select
--       loan_tmp.project_id,
--       loan_tmp.due_bill_no,
--       repay_tmp.pre_settle_date
--     from (
--       select
--         project_id,
--         due_bill_no
--       from ods.loan_info_abs
--       where '${ST9}' between s_d_date and date_sub(e_d_date,1)
--         and loan_status = 'F'
--         and paid_out_type = 'PRE_SETTLE'
--     ) as loan_tmp
--     inner join (
--       select
--         project_id,
--         due_bill_no,
--         max(biz_date) as pre_settle_date
--       from ods.repay_detail_abs
--       where biz_date <= '${ST9}'
--       group by project_id,due_bill_no
--     ) as repay_tmp
--     on  loan_tmp.project_id  = repay_tmp.project_id
--     and loan_tmp.due_bill_no = repay_tmp.due_bill_no
--   ) as settle_date
--   on  repay_detail.project_id  = settle_date.project_id
--   and repay_detail.due_bill_no = settle_date.due_bill_no
--   left join ods.customer_info_abs cust
--   on  repay_detail.project_id  = cust.project_id
--   and repay_detail.due_bill_no = cust.due_bill_no
-- ) as cur_pre_settle
-- on bag_due.due_bill_no = cur_pre_settle.due_bill_no
-- left join (
--   select
--     repay_detail.project_id,
--     repay_detail.due_bill_no,
--     repay_detail.biz_date,
--     repay_detail.bnp_type,
--     repay_detail.repay_amount,
--     settle_date.pre_settle_date,
--     cust.user_hash_no
--   from (
--     select
--       project_id,
--       due_bill_no,
--       bnp_type,
--       repay_amount,
--       biz_date
--     from ods.repay_detail_abs
--     where biz_date <= '${ST9}'
--   ) as repay_detail
--   inner join (
--     select
--       loan_tmp.project_id,
--       loan_tmp.due_bill_no,
--       repay_tmp.pre_settle_date
--     from (
--       select
--         project_id,
--         due_bill_no
--       from ods.loan_info_abs
--       where '${ST9}' between s_d_date and date_sub(e_d_date,1)
--         and loan_status = 'F'
--         and paid_out_type = 'PRE_SETTLE'
--     ) as loan_tmp
--     inner join (
--       select
--         project_id,
--         due_bill_no,
--         max(biz_date) as pre_settle_date
--       from ods.repay_detail_abs
--       where biz_date <= '${ST9}'
--       group by project_id,due_bill_no
--     ) as repay_tmp
--     on  loan_tmp.project_id  = repay_tmp.project_id
--     and loan_tmp.due_bill_no = repay_tmp.due_bill_no
--   ) as settle_date
--   on  repay_detail.project_id  = settle_date.project_id
--   and repay_detail.due_bill_no = settle_date.due_bill_no
--   left join ods.customer_info_abs as cust
--   on  repay_detail.project_id  = cust.project_id
--   and repay_detail.due_bill_no = cust.due_bill_no
-- ) as accu_pre_settle
-- on  bag_info.project_id = accu_pre_settle.project_id
-- and bag_due.due_bill_no = accu_pre_settle.due_bill_no
-- group by bag_info.project_id,bag_due.bag_id,bag_info.bag_date,bag_info.bag_remain_principal
limit 10
;
