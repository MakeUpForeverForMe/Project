set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set spark.shuffle.memoryFraction=0.6;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.groupby.orderby.position.alias=true;




insert overwrite table dm_eagle${db_suffix}.eagle_asset_scale_repaid_day partition (biz_date = '${ST9}',product_id)
-- insert overwrite table dm_eagle${db_suffix}.eagle_asset_scale_repaid_day partition (biz_date,product_id)
select
  capital_id                                                                             as capital_id,
  channel_id                                                                             as channel_id,
  project_id                                                                             as project_id,
  loan_terms                                                                             as loan_terms,
  sum(repaid_amount_count)                                                               as paid_amount,
  sum(repaid_principal_count)                                                            as paid_principal,
  sum(repaid_interest_penalty_svc_fee_count)                                             as paid_interest_penalty_svc_fee,
  sum(repaid_interest_count)                                                             as paid_interest,
  sum(repaid_repay_svc_term_count)                                                       as paid_svc_fee,
  sum(repaid_penalty_count)                                                              as paid_penalty,
  nvl(sum(repaid_principal_count) / sum(repaid_amount_count) * 100.0,0.0)                as paid_principal_rate,
  nvl(sum(repaid_interest_penalty_svc_fee_count) / sum(repaid_amount_count) * 100.0,0.0) as paid_interest_svc_fee_rate,
  sum(repaid_amount)                                                                     as repay_amount,
  sum(repaid_principal)                                                                  as repay_principal,
  sum(repaid_interest_penalty_svc_fee)                                                   as repay_interest_penalty_svc_fee,
  sum(repaid_interest)                                                                   as repay_interest,
  sum(repaid_repay_svc_term)                                                             as repay_svc_fee,
  sum(repaid_penalty)                                                                    as repay_penalty,
  nvl(sum(repaid_principal) / sum(repaid_amount) * 100.0,0.0)                            as repay_principal_rate,
  nvl(sum(repaid_interest_penalty_svc_fee) / sum(repaid_amount) * 100.0,0.0)             as repay_interest_svc_fee_rate,
  -- biz_date                                                                               as biz_date,
  biz_conf.product_id${vt}                                                               as product_id
from dw_new${db_suffix}.dw_loan_base_stat_repay_detail_day as repay_detail
join dim_new.biz_conf
on  repay_detail.product_id = biz_conf.product_id
and repay_detail.biz_date = '${ST9}'
and biz_conf.product_id${vt} is not null
group by 1,2,3,4,21
-- ,22
-- limit 10
;
