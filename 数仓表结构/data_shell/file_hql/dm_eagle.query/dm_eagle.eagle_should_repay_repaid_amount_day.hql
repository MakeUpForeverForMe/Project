set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;



insert overwrite table dm_eagle${db_suffix}.eagle_should_repay_repaid_amount_day partition(biz_date,project_id)
select
  capital_id                               as capital_id,
  channel_id                               as channel_id,
  nvl(should_loan_terms,repaid_loan_terms) as loan_terms,
  nvl(should_repay_amount_plan,0)          as should_repay_amount_plan,
  nvl(should_repay_amount_actual,0)        as should_repay_amount_actual,
  nvl(repaid_amount,0)                     as repaid_amount,
  nvl(should_biz_date,repaid_biz_date)     as biz_date,
  nvl(should_project_id,repaid_project_id) as project_id
from (
  select
    loan_terms                      as should_loan_terms,
    sum(should_repay_amount)        as should_repay_amount_plan,
    sum(should_repay_amount_actual) as should_repay_amount_actual,
    biz_date                        as should_biz_date,
    project_id                      as should_project_id
  from dw_new${db_suffix}.dw_loan_base_stat_should_repay_day as should_repay
  join dim_new.biz_conf
  on  should_repay.product_id = biz_conf.product_id
  and should_repay.biz_date = '${ST9}'
  -- and should_repay.biz_date like '2019%'
  group by loan_terms,biz_date,project_id
) as should_repay
full join(
  select
    loan_terms         as repaid_loan_terms,
    sum(repaid_amount) as repaid_amount,
    biz_date           as repaid_biz_date,
    project_id         as repaid_project_id
  from dw_new${db_suffix}.dw_loan_base_stat_repay_detail_day as repay_detail
  join dim_new.biz_conf
  on  repay_detail.product_id = biz_conf.product_id
  and repay_detail.biz_date = '${ST9}'
  -- and repay_detail.biz_date like '2019%'
  group by loan_terms,biz_date,project_id
) as repay_detail
on  should_biz_date   = repaid_biz_date
and should_project_id = repaid_project_id
and should_loan_terms = repaid_loan_terms
left join (select distinct capital_id,channel_id,project_id from dim_new.biz_conf) as biz_conf
on nvl(should_project_id,repaid_project_id) = project_id
-- order by biz_date,product_id,loan_terms
-- limit 10
;
