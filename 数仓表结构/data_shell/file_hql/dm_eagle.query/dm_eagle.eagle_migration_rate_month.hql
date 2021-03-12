set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.groupby.orderby.position.alias=true;



insert overwrite table dm_eagle${db_suffix}.eagle_migration_rate_month partition(biz_month,product_id)
select
  capital_id,
  channel_id,
  project_id,
  loan_terms,
  overdue_stage,
  overdue_mob,
  loan_month,
  loan_principal,
  remain_principal,
  biz_month,
  product_id
from (
  select
    capital_id,
    channel_id,
    project_id,
    loan_terms,
    if(overdue_stage = 0,'C',concat('M',overdue_stage)) as overdue_stage,
    overdue_mob                                         as overdue_mob,
    date_format(loan_active_date,'yyyy-MM')             as loan_month,
    sum(remain_principal)                               as remain_principal,
    date_format(biz_date,'yyyy-MM')                     as biz_month,
    biz_conf.product_id${vt}                            as product_id
  from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day as overdue_num
  join dim_new.biz_conf
  on  overdue_num.product_id = biz_conf.product_id
  and overdue_num.biz_date = '${ST9}'
  and overdue_num.overdue_mob <= 12
  and overdue_num.overdue_stage <= 6
  and biz_conf.product_id${vt} is not null
  group by 1,2,3,4,5,6,7,9,10
  -- order by biz_month,loan_month,product_id,loan_terms
) as overdue_num
left join (
  select
    loan_terms                      as loan_terms_loan_num,
    date_format(biz_date,'yyyy-MM') as loan_month_loan_num,
    sum(loan_principal)             as loan_principal,
    biz_conf.product_id${vt}        as product_id_loan_num
  from dw_new${db_suffix}.dw_loan_base_stat_loan_num_day as loan_num
  join dim_new.biz_conf
  on  loan_num.product_id = biz_conf.product_id
  and loan_num.biz_date <= '${ST9}'
  and biz_conf.product_id${vt} is not null
  group by 1,2,4
  -- order by loan_month_loan_num,product_id_loan_num,loan_terms_loan_num
) as loan_num
on  product_id = product_id_loan_num
and loan_terms = loan_terms_loan_num
and loan_month = loan_month_loan_num
-- order by biz_month,loan_month,product_id,loan_terms,overdue_stage
-- limit 10
;
