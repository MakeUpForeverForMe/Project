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


-- set hivevar:ST9=2020-10-01;

insert overwrite table dm_eagle.abs_asset_information_project partition(biz_date,project_id)
select
  count(loan.due_bill_no)                                                                                                    as asset_count,
  count(distinct cust.user_hash_no)                                                                                          as customer_count,
  sum(loan.remain_principal)                                                                                                 as remain_principal,
  sum(loan.remain_interest)                                                                                                  as remain_interest,
  sum(loan.remain_principal) / count(distinct cust.user_hash_no)                                                             as customer_remain_principal_avg,
  max(loan.remain_principal)                                                                                                 as remain_principal_max,
  min(loan.remain_principal)                                                                                                 as remain_principal_min,
  sum(loan.remain_principal) / count(loan.due_bill_no)                                                                       as remain_principal_avg,
  max(loan.loan_init_principal)                                                                                              as loan_principal_max,
  min(loan.loan_init_principal)                                                                                              as loan_principal_min,
  sum(loan.loan_init_principal) / count(loan.due_bill_no)                                                                    as loan_principal_avg,
  max(loan.loan_init_term)                                                                                                   as loan_terms_max,
  min(loan.loan_init_term)                                                                                                   as loan_terms_min,
  sum(loan.loan_init_term) / count(loan.due_bill_no)                                                                         as loan_terms_avg,
  sum(loan.remain_principal * loan.loan_init_term) / sum(loan.remain_principal)                                              as loan_terms_avg_weighted,
  max(loan.loan_term_remain)                                                                                                 as remain_term_max,
  min(loan.loan_term_remain)                                                                                                 as remain_term_min,
  sum(loan.loan_term_remain) / count(loan.due_bill_no)                                                                       as remain_term_avg,
  sum(loan.remain_principal * loan.loan_term_remain) / sum(loan.remain_principal)                                            as remain_term_avg_weighted,
  max(loan.loan_term_repaid)                                                                                                 as repaid_term_max,
  min(loan.loan_term_repaid)                                                                                                 as repaid_term_min,
  sum(loan.loan_term_repaid) / count(loan.due_bill_no)                                                                       as repaid_term_avg,
  sum(loan.remain_principal * loan.loan_term_repaid) / sum(loan.remain_principal)                                            as repaid_term_avg_weighted,
  max(loan.account_age)                                                                                                      as aging_max,
  min(loan.account_age)                                                                                                      as aging_min,
  sum(loan.account_age) / count(loan.due_bill_no)                                                                            as aging_avg,
  sum(bag_due.package_remain_principal * (loan.account_age)) / sum(bag_due.package_remain_principal)                         as aging_avg_weighted,
  max(lending.contract_term - loan.account_age)                                                                              as remain_period_max,
  min(lending.contract_term - loan.account_age)                                                                              as remain_period_min,
  sum(lending.contract_term - loan.account_age) / count(loan.due_bill_no)                                  as remain_period_avg,
  sum(bag_due.package_remain_principal * (lending.contract_term - loan.account_age)) / sum(bag_due.package_remain_principal) as remain_period_avg_weighted,
  max(lending.loan_init_interest_rate)                                                                                       as interest_rate_max,
  min(lending.loan_init_interest_rate)                                                                                       as interest_rate_min,
  sum(lending.loan_init_interest_rate) / count(loan.due_bill_no)                                                             as interest_rate_avg,
  sum(loan.remain_principal * lending.loan_init_interest_rate) / sum(loan.remain_principal)                                  as interest_rate_avg_weighted,
  max(cust.age)                                                                                                              as age_max,
  min(cust.age)                                                                                                              as age_min,
  sum(cust.age) / count(loan.due_bill_no)                                                                                    as age_avg,
  sum(cust.age * loan.remain_principal) / sum(loan.remain_principal)                                                         as age_avg_weighted,
  max(cust.income_year)                                                                                                      as income_year_max,
  min(cust.income_year)                                                                                                      as income_year_min,
  sum(cust.income_year) / count(loan.due_bill_no)                                                                            as income_year_avg,
  sum(cust.income_year * loan.remain_principal) / sum(loan.remain_principal)                                                 as income_year_avg_weighted,
  0                                                                                                                          as income_debt_ratio_max,
  0                                                                                                                          as income_debt_ratio_min,
  0                                                                                                                          as income_debt_ratio_avg,
  0                                                                                                                          as income_debt_ratio_avg_weighted,
  sum(if(guaranty.guarantee_type = '抵押担保',loan.remain_principal,0))                                                      as pledged_asset_balance,
  count(if(guaranty.guarantee_type = '抵押担保',loan.due_bill_no,null))                                                      as pledged_asset_count,
  sum(if(guaranty.guarantee_type = '抵押担保',loan.remain_principal,0)) / sum(loan.remain_principal)                         as pledged_asset_balance_ratio,
  count(if(guaranty.guarantee_type = '抵押担保',loan.due_bill_no,null)) / count(loan.due_bill_no)                            as pledged_asset_count_ratio,
  sum(guaranty.pawn_value)                                                                                                   as pawn_value,
  sum(
    if(guaranty.guarantee_type = '抵押担保',loan.remain_principal,0) * (
      if(guaranty.guarantee_type = '抵押担保',loan.loan_init_principal,0) /
      if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0)
    )
  ) / sum(if(guaranty.guarantee_type = '抵押担保',loan.remain_principal,0))                                                  as pledged_asset_rate_avg_weighted,
  'y'                                                                                                                        as is_allBag,
  '${ST9}'                                                                                                                   as biz_date,
  loan.project_id                                                                                                            as project_id
from (
select * from ods.loan_info_abs
where '${ST9}' between s_d_date and date_sub(e_d_date,1)
  and loan_status <> 'F'
) as loan
left join ods.loan_lending_abs as lending
on loan.due_bill_no = lending.due_bill_no
left join ods.customer_info_abs as cust
on loan.due_bill_no = cust.due_bill_no
left join (
select
  due_bill_no,
  sum(pawn_value) as pawn_value,
  max(guarantee_type) as guarantee_type
from ods.guaranty_info_abs
group by due_bill_no
) as guaranty
on loan.due_bill_no = guaranty.due_bill_no
inner join dim.bag_due_bill_no as bag_due
on loan.due_bill_no = bag_due.due_bill_no
inner join dim.bag_info as bag_info
on bag_due.bag_id = bag_info.bag_id
group by loan.project_id
union all
select
  count(loan.due_bill_no)                                                                                                    as asset_count,
  count(distinct cust.user_hash_no)                                                                                          as customer_count,
  sum(loan.remain_principal)                                                                                                 as remain_principal,
  sum(loan.remain_interest)                                                                                                  as remain_interest,
  sum(loan.remain_principal) / count(distinct cust.user_hash_no)                                                             as customer_remain_principal_avg,
  max(loan.remain_principal)                                                                                                 as remain_principal_max,
  min(loan.remain_principal)                                                                                                 as remain_principal_min,
  sum(loan.remain_principal) / count(loan.due_bill_no)                                                                       as remain_principal_avg,
  max(loan.loan_init_principal)                                                                                              as loan_principal_max,
  min(loan.loan_init_principal)                                                                                              as loan_principal_min,
  sum(loan.loan_init_principal) / count(loan.due_bill_no)                                                                    as loan_principal_avg,
  max(loan.loan_init_term)                                                                                                   as loan_terms_max,
  min(loan.loan_init_term)                                                                                                   as loan_terms_min,
  sum(loan.loan_init_term) / count(loan.due_bill_no)                                                                         as loan_terms_avg,
  sum(loan.remain_principal * loan.loan_init_term) / sum(loan.remain_principal)                                              as loan_terms_avg_weighted,
  max(loan.loan_term_remain)                                                                                                 as remain_term_max,
  min(loan.loan_term_remain)                                                                                                 as remain_term_min,
  sum(loan.loan_term_remain) / count(loan.due_bill_no)                                                                       as remain_term_avg,
  sum(loan.remain_principal * loan.loan_term_remain) / sum(loan.remain_principal)                                            as remain_term_avg_weighted,
  max(loan.loan_term_repaid)                                                                                                 as repaid_term_max,
  min(loan.loan_term_repaid)                                                                                                 as repaid_term_min,
  sum(loan.loan_term_repaid) / count(loan.due_bill_no)                                                                       as repaid_term_avg,
  sum(loan.remain_principal * loan.loan_term_repaid) / sum(loan.remain_principal)                                            as repaid_term_avg_weighted,
  max(loan.account_age)                                                                                                      as aging_max,
  min(loan.account_age)                                                                                                      as aging_min,
  sum(loan.account_age) / count(loan.due_bill_no)                                                                            as aging_avg,
  sum(bag_due.package_remain_principal * (loan.account_age)) / sum(bag_due.package_remain_principal)                         as aging_avg_weighted,
  max(lending.contract_term - loan.account_age)                                                                              as remain_period_max,
  min(lending.contract_term - loan.account_age)                                                                              as remain_period_min,
  sum(lending.contract_term - loan.account_age) / count(loan.due_bill_no)                                                    as remain_period_avg,
  sum(bag_due.package_remain_principal * (lending.contract_term - loan.account_age)) / sum(bag_due.package_remain_principal) as remain_period_avg_weighted,
  max(lending.loan_init_interest_rate)                                                                                       as interest_rate_max,
  min(lending.loan_init_interest_rate)                                                                                       as interest_rate_min,
  sum(lending.loan_init_interest_rate) / count(loan.due_bill_no)                                                             as interest_rate_avg,
  sum(loan.remain_principal * lending.loan_init_interest_rate) / sum(loan.remain_principal)                                  as interest_rate_avg_weighted,
  max(cust.age)                                                                                                              as age_max,
  min(cust.age)                                                                                                              as age_min,
  sum(cust.age) / count(loan.due_bill_no)                                                                                    as age_avg,
  sum(cust.age * loan.remain_principal) / sum(loan.remain_principal)                                                         as age_avg_weighted,
  max(cust.income_year)                                                                                                      as income_year_max,
  min(cust.income_year)                                                                                                      as income_year_min,
  sum(cust.income_year) / count(loan.due_bill_no)                                                                            as income_year_avg,
  sum(cust.income_year * loan.remain_principal) / sum(loan.remain_principal)                                                 as income_year_avg_weighted,
  0                                                                                                                          as income_debt_ratio_max,
  0                                                                                                                          as income_debt_ratio_min,
  0                                                                                                                          as income_debt_ratio_avg,
  0                                                                                                                          as income_debt_ratio_avg_weighted,
  sum(if(guaranty.guarantee_type = '抵押担保',loan.remain_principal,0))                                                      as pledged_asset_balance,
  count(if(guaranty.guarantee_type = '抵押担保',loan.due_bill_no,null))                                                      as pledged_asset_count,
  100 * sum(if(guaranty.guarantee_type = '抵押担保',loan.remain_principal,0)) / sum(loan.remain_principal)                   as pledged_asset_balance_ratio,
  100 * count(if(guaranty.guarantee_type = '抵押担保',loan.due_bill_no,null)) / count(loan.due_bill_no)                      as pledged_asset_count_ratio,
  sum(guaranty.pawn_value)                                                                                                   as pawn_value,
  100 * (
    sum(
      if(guaranty.guarantee_type = '抵押担保',loan.remain_principal,0) * (
        if(guaranty.guarantee_type = '抵押担保',loan.loan_init_principal,0) /
        if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0)
      )
    ) / sum(if(guaranty.guarantee_type = '抵押担保',loan.remain_principal,0))
  )                                                                                                                          as pledged_asset_rate_avg_weighted,
  'n'                                                                                                                        as is_allBag,
  '${ST9}'                                                                                                                   as biz_date,
  loan.project_id                                                                                                            as project_id
from (
  select * from ods.loan_info_abs
  where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    and loan_status <> 'F'
) as loan
left join ods.loan_lending_abs as lending
on loan.due_bill_no = lending.due_bill_no
left join ods.customer_info_abs as cust
on loan.due_bill_no = cust.due_bill_no
left join (
select
  due_bill_no,
  sum(pawn_value) as pawn_value,
  max(guarantee_type) as guarantee_type
from ods.guaranty_info_abs
group by due_bill_no
) as guaranty
on loan.due_bill_no = guaranty.due_bill_no
left join dim.bag_due_bill_no as bag_due
on loan.due_bill_no = bag_due.due_bill_no
left join dim.bag_info as bag_info
on bag_due.bag_id = bag_info.bag_id
group by loan.project_id
-- limit 10
;
