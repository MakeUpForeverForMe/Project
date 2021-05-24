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



-- set hivevar:ST9=2021-05-12;
-- set hivevar:bag_id=
--   select distinct bag_id
--   from dim.bag_info
--   where 1 > 0
-- ;



insert overwrite table dm_eagle.abs_asset_information_bag partition(biz_date,bag_id)
select
  dw_abs.project_id                                                                                                                  as project_id,
  count(dw_abs.due_bill_no)                                                                                                          as asset_count,
  count(distinct dw_abs.user_hash_no)                                                                                                as customer_count,
  sum(dw_abs.remain_principal)                                                                                                       as remain_principal,
  sum(dw_abs.remain_principal) + sum(dw_abs.remain_interest)                                                                         as remain_interest,
  sum(dw_abs.remain_principal) / count(distinct dw_abs.user_hash_no)                                                                 as customer_remain_principal_avg,
  max(dw_abs.remain_principal)                                                                                                       as remain_principal_max,
  min(dw_abs.remain_principal)                                                                                                       as remain_principal_min,
  sum(dw_abs.remain_principal) / count(dw_abs.due_bill_no)                                                                           as remain_principal_avg,
  max(dw_abs.loan_init_principal)                                                                                                    as loan_principal_max,
  min(dw_abs.loan_init_principal)                                                                                                    as loan_principal_min,
  sum(dw_abs.loan_init_principal) / count(dw_abs.due_bill_no)                                                                        as loan_principal_avg,
  max(dw_abs.loan_init_term)                                                                                                         as loan_terms_max,
  min(dw_abs.loan_init_term)                                                                                                         as loan_terms_min,
  sum(dw_abs.loan_init_term) / count(dw_abs.due_bill_no)                                                                             as loan_terms_avg,
  sum(dw_abs.remain_principal * dw_abs.loan_init_term) / sum(dw_abs.remain_principal)                                                as loan_terms_avg_weighted,
  max(dw_abs.loan_term_remain)                                                                                                       as remain_term_max,
  min(dw_abs.loan_term_remain)                                                                                                       as remain_term_min,
  sum(dw_abs.loan_term_remain) / count(dw_abs.due_bill_no)                                                                           as remain_term_avg,
  sum(dw_abs.remain_principal * dw_abs.loan_term_remain) / sum(dw_abs.remain_principal)                                              as remain_term_avg_weighted,
  max(dw_abs.loan_term_repaid)                                                                                                       as repaid_term_max,
  min(dw_abs.loan_term_repaid)                                                                                                       as repaid_term_min,
  sum(dw_abs.loan_term_repaid) / count(dw_abs.due_bill_no)                                                                           as repaid_term_avg,
  sum(dw_abs.remain_principal * dw_abs.loan_term_repaid) / sum(dw_abs.remain_principal)                                              as repaid_term_avg_weighted,
  max(dw_abs.account_age)                                                                                                            as aging_max,
  min(dw_abs.account_age)                                                                                                            as aging_min,
  sum(dw_abs.account_age) / count(dw_abs.due_bill_no)                                                                                as aging_avg,
  nvl(sum(bag_due.package_remain_principal * (dw_abs.account_age)) / sum(bag_due.package_remain_principal),0)                        as aging_avg_weighted,
  max(dw_abs.contract_term - dw_abs.account_age)                                                                                     as remain_period_max,
  min(dw_abs.contract_term - dw_abs.account_age)                                                                                     as remain_period_min,
  sum(dw_abs.contract_term - dw_abs.account_age) / count(dw_abs.due_bill_no)                                                         as remain_period_avg,
  nvl(sum(bag_due.package_remain_principal * (dw_abs.contract_term - dw_abs.account_age)) / sum(bag_due.package_remain_principal),0) as remain_period_avg_weighted,
  max(dw_abs.loan_init_interest_rate)                                                                                                as interest_rate_max,
  min(dw_abs.loan_init_interest_rate)                                                                                                as interest_rate_min,
  sum(dw_abs.loan_init_interest_rate) / count(dw_abs.due_bill_no)                                                                    as interest_rate_avg,
  sum(dw_abs.remain_principal * dw_abs.loan_init_interest_rate) / sum(dw_abs.remain_principal)                                       as interest_rate_avg_weighted,
  max(dw_abs.age)                                                                                                                    as age_max,
  min(dw_abs.age)                                                                                                                    as age_min,
  sum(dw_abs.age) / count(dw_abs.due_bill_no)                                                                                        as age_avg,
  sum(dw_abs.age * dw_abs.remain_principal) / sum(dw_abs.remain_principal)                                                           as age_avg_weighted,
  nvl(max(greatest(dw_abs.income_year,dw_abs.income_year_max)),0)                                                                    as income_year_max,
  nvl(min(least(dw_abs.income_year,dw_abs.income_year_min)),0)                                                                       as income_year_min,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year)) / count(dw_abs.due_bill_no),0) as income_year_avg,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0) as income_year_avg_weighted,
  max(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal) as income_debt_ratio_max,
  min(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal) as income_debt_ratio_min,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal) / count(dw_abs.due_bill_no),0) as income_debt_ratio_avg,
  nvl(sum(dw_abs.remain_principal * (if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / bag_due.package_remain_principal)) / sum(dw_abs.remain_principal),0) as income_debt_ratio_avg_weighted,
  nvl(sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)),0)                                                       as pledged_asset_balance,
  count(if(dw_abs.guarantee_type = '抵押担保',dw_abs.due_bill_no,null))                                                              as pledged_asset_count,
  sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)) / sum(dw_abs.remain_principal)                               as pledged_asset_balance_ratio,
  count(if(dw_abs.guarantee_type = '抵押担保',dw_abs.due_bill_no,null)) / count(dw_abs.due_bill_no)                                  as pledged_asset_count_ratio,
  nvl(sum(dw_abs.pawn_value),0)                                                                                                      as pawn_value,
  nvl(
    sum(
      if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)
      * (nvl(if(dw_abs.guarantee_type  = '抵押担保',dw_abs.loan_init_principal,0) / if(dw_abs.guarantee_type = '抵押担保',dw_abs.pawn_value,0),0))
    ) / sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0))
  ,0)                                                                                                                                as pledged_asset_rate_avg_weighted,
  nvl(sum(dw_abs.loan_init_principal / (dw_abs.loan_init_principal + dw_abs.shoufu_amount) * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0) as car_financing_rate_avg_weighted,
  dw_abs.biz_date                                                                                                                    as biz_date,
  bag_due.bag_id                                                                                                                     as bag_id
from (
  select
    *
  from dw.abs_due_info_day_abs
  where 1 > 0
    and biz_date = '${ST9}'
    and project_id in (${project_id})
    and loan_status <> 'F'
) as dw_abs
inner join (
  select
    bag_info.project_id,
    bag_info.bag_date,
    bag_info.bag_id,
    bag_due.due_bill_no,
    bag_due.package_remain_principal
  from (
    select
      project_id,
      bag_id,
      bag_date
    from dim.bag_info
    where 1 > 0
      and '${ST9}' >= bag_date
      and bag_id in (${bag_id})
  ) as bag_info
  join (
    select
      project_id,
      bag_id,
      due_bill_no,
      package_remain_principal
    from dim.bag_due_bill_no
  ) as bag_due
  on  bag_info.project_id = bag_due.project_id
  and bag_info.bag_id     = bag_due.bag_id
) as bag_due
on  dw_abs.project_id  = bag_due.project_id
and dw_abs.due_bill_no = bag_due.due_bill_no
group by dw_abs.biz_date,dw_abs.project_id,bag_due.bag_id
-- limit 10
;
