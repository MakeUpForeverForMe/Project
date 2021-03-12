set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.groupby.orderby.position.alias=true;


insert overwrite table dm_eagle${db_suffix}.eagle_loan_amount_day partition(biz_date = '${ST9}',product_id)
-- insert overwrite table dm_eagle${db_suffix}.eagle_loan_amount_day partition(biz_date,product_id)
select
  capital_id,
  channel_id,
  project_id,
  loan_terms,
  sum(loan_principal) as loan_amount,
  sum(loan_principal_count) as loan_principal_count,
  -- biz_date,
  biz_conf.product_id${vt} as product_id
from dw_new${db_suffix}.dw_loan_base_stat_loan_num_day as loan_num
join dim_new.biz_conf
on  loan_num.product_id = biz_conf.product_id
and loan_num.biz_date = '${ST9}'
and biz_conf.product_id${vt} is not null
group by 1,2,3,4,7
         -- ,8
;
