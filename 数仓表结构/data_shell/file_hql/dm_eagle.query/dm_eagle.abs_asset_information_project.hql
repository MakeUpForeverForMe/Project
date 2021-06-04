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


-- set hivevar:ST9=2021-03-28;
-- set hivevar:project_id='CL202012160091';

-- set hivevar:project_id=
--   select distinct project_id
--   from dim.project_info
--   where 1 > 0
-- ;


insert overwrite table dm_eagle.abs_asset_information_project partition(biz_date,project_id)
select
  count(dw_abs.due_bill_no)                                                                                                          as asset_count,
  count(distinct dw_abs.user_hash_no)                                                                                                as customer_count,
  nvl(sum(dw_abs.remain_principal),0)                                                                                                as remain_principal,
  nvl(sum(dw_abs.remain_principal) + sum(dw_abs.remain_interest),0)                                                                  as remain_interest,
  nvl(sum(dw_abs.remain_principal) / count(distinct dw_abs.user_hash_no),0)                                                          as customer_remain_principal_avg,
  max(dw_abs.remain_principal)                                                                                                       as remain_principal_max,
  min(dw_abs.remain_principal)                                                                                                       as remain_principal_min,
  nvl(sum(dw_abs.remain_principal) / count(dw_abs.due_bill_no),0)                                                                    as remain_principal_avg,
  max(dw_abs.loan_init_principal)                                                                                                    as loan_principal_max,
  min(dw_abs.loan_init_principal)                                                                                                    as loan_principal_min,
  nvl(sum(dw_abs.loan_init_principal) / count(dw_abs.due_bill_no),0)                                                                 as loan_principal_avg,
  max(dw_abs.loan_init_term)                                                                                                         as loan_terms_max,
  min(dw_abs.loan_init_term)                                                                                                         as loan_terms_min,
  nvl(sum(dw_abs.loan_init_term) / count(dw_abs.due_bill_no),0)                                                                      as loan_terms_avg,
  nvl(sum(dw_abs.remain_principal * dw_abs.loan_init_term) / sum(dw_abs.remain_principal),0)                                         as loan_terms_avg_weighted,
  max(dw_abs.loan_term_remain)                                                                                                       as remain_term_max,
  min(dw_abs.loan_term_remain)                                                                                                       as remain_term_min,
  nvl(sum(dw_abs.loan_term_remain) / count(dw_abs.due_bill_no),0)                                                                    as remain_term_avg,
  nvl(sum(dw_abs.remain_principal * dw_abs.loan_term_remain) / sum(dw_abs.remain_principal),0)                                       as remain_term_avg_weighted,
  max(dw_abs.loan_term_repaid)                                                                                                       as repaid_term_max,
  min(dw_abs.loan_term_repaid)                                                                                                       as repaid_term_min,
  nvl(sum(dw_abs.loan_term_repaid) / count(dw_abs.due_bill_no),0)                                                                    as repaid_term_avg,
  nvl(sum(dw_abs.remain_principal * dw_abs.loan_term_repaid) / sum(dw_abs.remain_principal),0)                                       as repaid_term_avg_weighted,
  max(dw_abs.account_age)                                                                                                            as aging_max,
  min(dw_abs.account_age)                                                                                                            as aging_min,
  nvl(sum(dw_abs.account_age) / count(dw_abs.due_bill_no),0)                                                                         as aging_avg,
  nvl(sum(bag_due.package_remain_principal * (dw_abs.account_age)) / sum(bag_due.package_remain_principal),0)                        as aging_avg_weighted,
  max(dw_abs.contract_term - dw_abs.account_age)                                                                                     as remain_period_max,
  min(dw_abs.contract_term - dw_abs.account_age)                                                                                     as remain_period_min,
  nvl(sum(dw_abs.contract_term - dw_abs.account_age) / count(dw_abs.due_bill_no),0)                                                  as remain_period_avg,
  nvl(sum(bag_due.package_remain_principal * (dw_abs.contract_term - dw_abs.account_age)) / sum(bag_due.package_remain_principal),0) as remain_period_avg_weighted,
  max(dw_abs.loan_init_interest_rate)                                                                                                as interest_rate_max,
  min(dw_abs.loan_init_interest_rate)                                                                                                as interest_rate_min,
  nvl(sum(dw_abs.loan_init_interest_rate) / count(dw_abs.due_bill_no),0)                                                             as interest_rate_avg,
  nvl(sum(dw_abs.remain_principal * dw_abs.loan_init_interest_rate) / sum(dw_abs.remain_principal),0)                                as interest_rate_avg_weighted,
  max(dw_abs.age)                                                                                                                    as age_max,
  min(dw_abs.age)                                                                                                                    as age_min,
  nvl(sum(dw_abs.age) / count(dw_abs.due_bill_no),0)                                                                                 as age_avg,
  nvl(sum(dw_abs.age * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0)                                                    as age_avg_weighted,
  nvl(max(greatest(dw_abs.income_year,dw_abs.income_year_max)),0)                                                                    as income_year_max,
  nvl(min(least(dw_abs.income_year,dw_abs.income_year_min)),0)                                                                       as income_year_min,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year)) / count(dw_abs.due_bill_no),0) as income_year_avg,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0) as income_year_avg_weighted,
  nvl(max(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal),0) as income_debt_ratio_max,
  nvl(min(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal),0) as income_debt_ratio_min,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal) / count(dw_abs.due_bill_no),0) as income_debt_ratio_avg,
  nvl(sum(dw_abs.remain_principal * (if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / bag_due.package_remain_principal)) / sum(dw_abs.remain_principal),0) as income_debt_ratio_avg_weighted,
  nvl(sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)),0)                                                       as pledged_asset_balance,
  count(if(dw_abs.guarantee_type = '抵押担保',dw_abs.due_bill_no,null))                                                              as pledged_asset_count,
  nvl(sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)) / sum(dw_abs.remain_principal),0)                        as pledged_asset_balance_ratio,
  nvl(count(if(dw_abs.guarantee_type = '抵押担保',dw_abs.due_bill_no,null)) / count(dw_abs.due_bill_no),0)                           as pledged_asset_count_ratio,
  nvl(sum(dw_abs.pawn_value),0)                                                                                                      as pawn_value,
  nvl(
    sum(
      if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)
      * (nvl(if(dw_abs.guarantee_type  = '抵押担保',dw_abs.loan_init_principal,0) / if(dw_abs.guarantee_type = '抵押担保',dw_abs.pawn_value,0),0))
    ) / sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0))
  ,0)                                                                                                                                as pledged_asset_rate_avg_weighted,
  nvl(sum(dw_abs.loan_init_principal / (dw_abs.loan_init_principal + dw_abs.shoufu_amount) * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0) as car_financing_rate_avg_weighted,
  'n'                                                                                                                                as is_allBag,
  dw_abs.biz_date                                                                                                                    as biz_date,
  dw_abs.project_id                                                                                                                  as project_id
from (
  select
    biz_date                                            as biz_date,
    project_id                                          as project_id,
    due_bill_no                                         as due_bill_no_join,

    if(loan_status != 'F',due_bill_no,            null) as due_bill_no,
    if(loan_status != 'F',user_hash_no,           null) as user_hash_no,
    if(loan_status != 'F',guarantee_type,         null) as guarantee_type,
    if(loan_status != 'F',account_age,            0)    as account_age,
    if(loan_status != 'F',age,                    0)    as age,
    if(loan_status != 'F',contract_term,          0)    as contract_term,
    if(loan_status != 'F',income_year,            0)    as income_year,
    if(loan_status != 'F',income_year_max,        0)    as income_year_max,
    if(loan_status != 'F',income_year_min,        0)    as income_year_min,
    if(loan_status != 'F',loan_init_interest_rate,0)    as loan_init_interest_rate,
    if(loan_status != 'F',loan_init_principal,    0)    as loan_init_principal,
    if(loan_status != 'F',loan_init_term,         0)    as loan_init_term,
    if(loan_status != 'F',loan_term_remain,       0)    as loan_term_remain,
    if(loan_status != 'F',loan_term_repaid,       0)    as loan_term_repaid,
    if(loan_status != 'F',pawn_value,             0)    as pawn_value,
    if(loan_status != 'F',remain_interest,        0)    as remain_interest,
    if(loan_status != 'F',remain_principal,       0)    as remain_principal,
    if(loan_status != 'F',shoufu_amount,          0)    as shoufu_amount
  from dw.abs_due_info_day_abs
  where 1 > 0
    and biz_date = '${ST9}'
    and project_id in (${project_id})
) as dw_abs
left join (
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
on  dw_abs.project_id       = bag_due.project_id
and dw_abs.due_bill_no_join = bag_due.due_bill_no
group by dw_abs.biz_date,dw_abs.project_id
union all
select
  count(dw_abs.due_bill_no)                                                                                                          as asset_count,
  count(distinct dw_abs.user_hash_no)                                                                                                as customer_count,
  nvl(sum(dw_abs.remain_principal),0)                                                                                                as remain_principal,
  nvl(sum(dw_abs.remain_principal) + sum(dw_abs.remain_interest),0)                                                                  as remain_interest,
  nvl(sum(dw_abs.remain_principal) / count(distinct dw_abs.user_hash_no),0)                                                          as customer_remain_principal_avg,
  max(dw_abs.remain_principal)                                                                                                       as remain_principal_max,
  min(dw_abs.remain_principal)                                                                                                       as remain_principal_min,
  nvl(sum(dw_abs.remain_principal) / count(dw_abs.due_bill_no),0)                                                                    as remain_principal_avg,
  max(dw_abs.loan_init_principal)                                                                                                    as loan_principal_max,
  min(dw_abs.loan_init_principal)                                                                                                    as loan_principal_min,
  nvl(sum(dw_abs.loan_init_principal) / count(dw_abs.due_bill_no),0)                                                                 as loan_principal_avg,
  max(dw_abs.loan_init_term)                                                                                                         as loan_terms_max,
  min(dw_abs.loan_init_term)                                                                                                         as loan_terms_min,
  nvl(sum(dw_abs.loan_init_term) / count(dw_abs.due_bill_no),0)                                                                      as loan_terms_avg,
  nvl(sum(dw_abs.remain_principal * dw_abs.loan_init_term) / sum(dw_abs.remain_principal),0)                                         as loan_terms_avg_weighted,
  max(dw_abs.loan_term_remain)                                                                                                       as remain_term_max,
  min(dw_abs.loan_term_remain)                                                                                                       as remain_term_min,
  nvl(sum(dw_abs.loan_term_remain) / count(dw_abs.due_bill_no),0)                                                                    as remain_term_avg,
  nvl(sum(dw_abs.remain_principal * dw_abs.loan_term_remain) / sum(dw_abs.remain_principal),0)                                       as remain_term_avg_weighted,
  max(dw_abs.loan_term_repaid)                                                                                                       as repaid_term_max,
  min(dw_abs.loan_term_repaid)                                                                                                       as repaid_term_min,
  nvl(sum(dw_abs.loan_term_repaid) / count(dw_abs.due_bill_no),0)                                                                    as repaid_term_avg,
  nvl(sum(dw_abs.remain_principal * dw_abs.loan_term_repaid) / sum(dw_abs.remain_principal),0)                                       as repaid_term_avg_weighted,
  max(dw_abs.account_age)                                                                                                            as aging_max,
  min(dw_abs.account_age)                                                                                                            as aging_min,
  nvl(sum(dw_abs.account_age) / count(dw_abs.due_bill_no),0)                                                                         as aging_avg,
  nvl(sum(bag_due.package_remain_principal * (dw_abs.account_age)) / sum(bag_due.package_remain_principal),0)                        as aging_avg_weighted,
  max(dw_abs.contract_term - dw_abs.account_age)                                                                                     as remain_period_max,
  min(dw_abs.contract_term - dw_abs.account_age)                                                                                     as remain_period_min,
  nvl(sum(dw_abs.contract_term - dw_abs.account_age) / count(dw_abs.due_bill_no),0)                                                  as remain_period_avg,
  nvl(sum(bag_due.package_remain_principal * (dw_abs.contract_term - dw_abs.account_age)) / sum(bag_due.package_remain_principal),0) as remain_period_avg_weighted,
  max(dw_abs.loan_init_interest_rate)                                                                                                as interest_rate_max,
  min(dw_abs.loan_init_interest_rate)                                                                                                as interest_rate_min,
  nvl(sum(dw_abs.loan_init_interest_rate) / count(dw_abs.due_bill_no),0)                                                             as interest_rate_avg,
  nvl(sum(dw_abs.remain_principal * dw_abs.loan_init_interest_rate) / sum(dw_abs.remain_principal),0)                                as interest_rate_avg_weighted,
  max(dw_abs.age)                                                                                                                    as age_max,
  min(dw_abs.age)                                                                                                                    as age_min,
  nvl(sum(dw_abs.age) / count(dw_abs.due_bill_no),0)                                                                                 as age_avg,
  nvl(sum(dw_abs.age * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0)                                                    as age_avg_weighted,
  nvl(max(greatest(dw_abs.income_year,dw_abs.income_year_max)),0)                                                                    as income_year_max,
  nvl(min(least(dw_abs.income_year,dw_abs.income_year_min)),0)                                                                       as income_year_min,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year)) / count(dw_abs.due_bill_no),0) as income_year_avg,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0) as income_year_avg_weighted,
  nvl(max(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal),0) as income_debt_ratio_max,
  nvl(min(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal),0) as income_debt_ratio_min,
  nvl(sum(if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / dw_abs.remain_principal) / count(dw_abs.due_bill_no),0) as income_debt_ratio_avg,
  nvl(sum(dw_abs.remain_principal * (if(dw_abs.income_year = 0,(dw_abs.income_year_max + dw_abs.income_year_min) / 2,dw_abs.income_year) / bag_due.package_remain_principal)) / sum(dw_abs.remain_principal),0) as income_debt_ratio_avg_weighted,
  nvl(sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)),0)                                                       as pledged_asset_balance,
  count(if(dw_abs.guarantee_type = '抵押担保',dw_abs.due_bill_no,null))                                                              as pledged_asset_count,
  nvl(sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)) / sum(dw_abs.remain_principal),0)                        as pledged_asset_balance_ratio,
  nvl(count(if(dw_abs.guarantee_type = '抵押担保',dw_abs.due_bill_no,null)) / count(dw_abs.due_bill_no),0)                           as pledged_asset_count_ratio,
  nvl(sum(dw_abs.pawn_value),0)                                                                                                      as pawn_value,
  nvl(
    sum(
      if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)
      * (nvl(if(dw_abs.guarantee_type  = '抵押担保',dw_abs.loan_init_principal,0) / if(dw_abs.guarantee_type = '抵押担保',dw_abs.pawn_value,0),0))
    ) / sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0))
  ,0)                                                                                                                                as pledged_asset_rate_avg_weighted,
  nvl(sum(dw_abs.loan_init_principal / (dw_abs.loan_init_principal + dw_abs.shoufu_amount) * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0) as car_financing_rate_avg_weighted,
  'y'                                                                                                                                as is_allBag,
  dw_abs.biz_date                                                                                                                    as biz_date,
  dw_abs.project_id                                                                                                                  as project_id
from (
  select
    biz_date                                            as biz_date,
    project_id                                          as project_id,
    due_bill_no                                         as due_bill_no_join,

    if(loan_status != 'F',due_bill_no,            null) as due_bill_no,
    if(loan_status != 'F',user_hash_no,           null) as user_hash_no,
    if(loan_status != 'F',guarantee_type,         null) as guarantee_type,
    if(loan_status != 'F',account_age,            0)    as account_age,
    if(loan_status != 'F',age,                    0)    as age,
    if(loan_status != 'F',contract_term,          0)    as contract_term,
    if(loan_status != 'F',income_year,            0)    as income_year,
    if(loan_status != 'F',income_year_max,        0)    as income_year_max,
    if(loan_status != 'F',income_year_min,        0)    as income_year_min,
    if(loan_status != 'F',loan_init_interest_rate,0)    as loan_init_interest_rate,
    if(loan_status != 'F',loan_init_principal,    0)    as loan_init_principal,
    if(loan_status != 'F',loan_init_term,         0)    as loan_init_term,
    if(loan_status != 'F',loan_term_remain,       0)    as loan_term_remain,
    if(loan_status != 'F',loan_term_repaid,       0)    as loan_term_repaid,
    if(loan_status != 'F',pawn_value,             0)    as pawn_value,
    if(loan_status != 'F',remain_interest,        0)    as remain_interest,
    if(loan_status != 'F',remain_principal,       0)    as remain_principal,
    if(loan_status != 'F',shoufu_amount,          0)    as shoufu_amount
  from dw.abs_due_info_day_abs
  where 1 > 0
    and biz_date = '${ST9}'
    and project_id in (${project_id})
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
on  dw_abs.project_id       = bag_due.project_id
and dw_abs.due_bill_no_join = bag_due.due_bill_no
group by dw_abs.biz_date,dw_abs.project_id
-- limit 10
;
