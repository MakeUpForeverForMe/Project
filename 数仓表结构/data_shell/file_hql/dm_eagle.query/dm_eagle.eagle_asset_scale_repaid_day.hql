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
set hive.groupby.orderby.position.alias=true;



 insert overwrite table dm_eagle${db_suffix}.eagle_asset_scale_repaid_day partition (biz_date,product_id)
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
  biz_date                                                                               as biz_date,
  biz_conf.product_id${vt}                                                               as product_id
from dw${db_suffix}.dw_loan_base_stat_repay_detail_day as repay_detail
join (
  select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt,
       product_id
       from (
         select
           max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
           max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
         )tmp
  where 1 > 0
    and product_id_vt is not null
) as biz_conf
on  repay_detail.product_id = biz_conf.product_id
and repay_detail.biz_date = '${ST9}'
and biz_conf.product_id${vt} is not null
group by capital_id,channel_id,project_id,loan_terms,biz_date,biz_conf.product_id${vt}
--limit 10
;
