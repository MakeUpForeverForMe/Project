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
  nvl(sum(bag_info.bag_remain_principal * (dw_abs.account_age)) / sum(bag_info.bag_remain_principal),0)                              as aging_avg_weighted,
  max(dw_abs.contract_term - dw_abs.account_age)                                                                                     as remain_period_max,
  min(dw_abs.contract_term - dw_abs.account_age)                                                                                     as remain_period_min,
  sum(dw_abs.contract_term - dw_abs.account_age) / count(dw_abs.due_bill_no)                                                         as remain_period_avg,
  nvl(sum(bag_info.bag_remain_principal * (dw_abs.contract_term - dw_abs.account_age)) / sum(bag_info.bag_remain_principal),0)       as remain_period_avg_weighted,
  max(dw_abs.loan_init_interest_rate)                                                                                                as interest_rate_max,
  min(dw_abs.loan_init_interest_rate)                                                                                                as interest_rate_min,
  sum(dw_abs.loan_init_interest_rate) / count(dw_abs.due_bill_no)                                                                    as interest_rate_avg,
  sum(dw_abs.remain_principal * dw_abs.loan_init_interest_rate) / sum(dw_abs.remain_principal)                                       as interest_rate_avg_weighted,
  max(dw_abs.age)                                                                                                                    as age_max,
  min(dw_abs.age)                                                                                                                    as age_min,
  sum(dw_abs.age) / count(dw_abs.due_bill_no)                                                                                        as age_avg,
  sum(dw_abs.age * dw_abs.remain_principal) / sum(dw_abs.remain_principal)                                                           as age_avg_weighted,
  nvl(max(dw_abs.income_year),0)                                                                                                     as income_year_max,
  nvl(min(dw_abs.income_year),0)                                                                                                     as income_year_min,
  nvl(sum(dw_abs.income_year) / count(dw_abs.due_bill_no),0)                                                                         as income_year_avg,
  nvl(sum(dw_abs.income_year * dw_abs.remain_principal) / sum(dw_abs.remain_principal),0)                                            as income_year_avg_weighted,
  0                                                                                                                                  as income_debt_ratio_max,
  0                                                                                                                                  as income_debt_ratio_min,
  0                                                                                                                                  as income_debt_ratio_avg,
  0                                                                                                                                  as income_debt_ratio_avg_weighted,
  nvl(sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)),0)                                                       as pledged_asset_balance,
  count(if(dw_abs.guarantee_type = '抵押担保',dw_abs.due_bill_no,null))                                                              as pledged_asset_count,
  sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0)) / sum(dw_abs.remain_principal)                               as pledged_asset_balance_ratio,
  count(if(dw_abs.guarantee_type = '抵押担保',dw_abs.due_bill_no,null)) / count(dw_abs.due_bill_no)                                  as pledged_asset_count_ratio,
  nvl(sum(dw_abs.pawn_value),0)                                                                                                      as pawn_value,
  sum(
    if(
      dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0
    ) * (
      if(dw_abs.guarantee_type  = '抵押担保',dw_abs.loan_init_principal,0) / if(dw_abs.guarantee_type = '抵押担保',dw_abs.pawn_value,0)
    )
  ) / sum(if(dw_abs.guarantee_type = '抵押担保',dw_abs.remain_principal,0))                                                          as pledged_asset_rate_avg_weighted,
  dw_abs.biz_date                                                                                                                    as biz_date,
  dw_abs.bag_id                                                                                                                      as bag_id
from (
  select
    project_id,
    bag_id,
    bag_remain_principal
  from dim.bag_info
  where 1 > 0
    and bag_id in (${bag_id})
    and '${ST9}' >= bag_date
) as bag_info
inner join (
  select
    -- 借据级
    loan.biz_date,
    loan.project_id,
    loan.due_bill_no,
    loan.account_age,
    loan.loan_init_principal,
    loan.loan_init_term,
    loan.loan_term_remain,
    loan.loan_term_repaid,
    loan.remain_interest,
    loan.remain_principal,

    -- 合同级
    lending.contract_term,
    lending.loan_init_interest_rate,

    -- 客户级
    cust.user_hash_no,
    cust.income_year,
    cust.age,

    -- 抵押物级
    guaranty.pawn_value,
    guaranty.guarantee_type,

    -- 包级
    bag_due.bag_id
  from (
    select
      project_id,
      bag_id,
      due_bill_no
    from dim.bag_due_bill_no
    where 1 > 0
      and bag_id in (${bag_id})
  ) as bag_due
  inner join (
    select
      project_id,
      due_bill_no,
      account_age,
      loan_init_principal,
      loan_init_term,
      loan_term_remain,
      loan_term_repaid,
      remain_interest,
      remain_principal,
      '${ST9}' as biz_date
    from ods.loan_info_abs
    where 1 > 0
      and '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and project_id in (${project_id})
      and loan_status <> 'F'
  ) as loan
  on  bag_due.project_id  = loan.project_id
  and bag_due.due_bill_no = loan.due_bill_no
  inner join (
    select
      project_id,
      due_bill_no,
      contract_term,
      loan_init_interest_rate
    from ods.loan_lending_abs
  ) as lending
  on  loan.project_id  = lending.project_id
  and loan.due_bill_no = lending.due_bill_no
  inner join (
    select
      project_id,
      due_bill_no,
      user_hash_no,
      income_year,
      age
    from ods.customer_info_abs
  ) as cust
  on  loan.project_id  = cust.project_id
  and loan.due_bill_no = cust.due_bill_no
  left join (
    select
      project_id,
      due_bill_no,
      pawn_value,
      guarantee_type
    from ods.guaranty_info_abs
  ) as guaranty
  on  loan.project_id  = guaranty.project_id
  and loan.due_bill_no = guaranty.due_bill_no
) as dw_abs
on  bag_info.project_id = dw_abs.project_id
and bag_info.bag_id     = dw_abs.bag_id
group by dw_abs.biz_date,dw_abs.project_id,dw_abs.bag_id
-- limit 10
;
