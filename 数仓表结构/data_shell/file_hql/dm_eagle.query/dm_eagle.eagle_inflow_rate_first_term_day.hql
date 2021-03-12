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


insert overwrite table dm_eagle${db_suffix}.eagle_inflow_rate_first_term_day partition(biz_date = '${ST9}',product_id)
-- insert overwrite table dm_eagle${db_suffix}.eagle_inflow_rate_first_term_day partition(biz_date,product_id)
select
  capital_id,channel_id,project_id,
  loan_terms                                                    as loan_terms,
  nvl(overdue_dob_overdue_num,0)                                as overdue_dob,
  loan_num                                                      as loan_num,
  nvl(overdue_loan_num,0)                                       as overdue_loan_num,
  nvl(nvl(overdue_loan_num,0) / loan_num * 100,0)               as overdue_loan_inflow_rate,
  loan_active_date                                              as loan_active_date,
  loan_principal                                                as loan_principal,
  nvl(overdue_principal,0)                                      as overdue_principal,
  nvl(nvl(overdue_principal,0) / loan_principal * 100,0)        as overdue_principal_inflow_rate,
  nvl(overdue_remain_principal,0)                               as overdue_remain_principal,
  nvl(nvl(overdue_remain_principal,0) / loan_principal * 100,0) as remain_principal_inflow_rate,
  -- biz_date                                                      as biz_date,
  product_id                                                    as product_id
from (
  select distinct
    loan_terms               as loan_terms,
    biz_date                 as biz_date,
    loan_active_date         as loan_active_date,
    biz_conf.product_id${vt} as product_id
  from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day as overdue_num
  join dim_new.biz_conf
  on  overdue_num.product_id = biz_conf.product_id
  and biz_conf.product_id${vt} is not null
  and overdue_num.biz_date = '${ST9}'
  -- order by biz_date,product_id,loan_terms
) as loan_terms
left join (
  select
    loan_terms                           as loan_terms_overdue_num,
    overdue_dob                          as overdue_dob_overdue_num,
    loan_active_date                     as loan_active_date_overdue_num,
    sum(nvl(overdue_loan_num,0))         as overdue_loan_num,
    sum(nvl(overdue_principal,0))        as overdue_principal,
    sum(nvl(overdue_remain_principal,0)) as overdue_remain_principal,
    biz_date                             as biz_date_overdue_num,
    biz_conf.product_id${vt}             as product_id_overdue_num
    -- ,overdue_days,should_repay_date
  from dw_new${db_suffix}.dw_loan_base_stat_overdue_num_day as overdue_num
  join dim_new.biz_conf
  on  overdue_num.product_id = biz_conf.product_id
  and biz_conf.product_id${vt} is not null
  and overdue_num.is_first_term_overdue = 1
  and overdue_num.overdue_days between 1 and 30
  and overdue_num.overdue_mob <= 12
  group by 1,2,3,7,8
  -- ,9,10
) as overdue_num
on  biz_date         = biz_date_overdue_num
and product_id       = product_id_overdue_num
and loan_terms       = loan_terms_overdue_num
and loan_active_date = loan_active_date_overdue_num
left join (
  select
    loan_terms               as loan_terms_loan_num,
    sum(loan_num)            as loan_num,
    sum(loan_principal)      as loan_principal,
    biz_date                 as loan_active_date_loan_num,
    biz_conf.product_id${vt} as product_id_loan_num
  from dw_new${db_suffix}.dw_loan_base_stat_loan_num_day as loan_num
  join dim_new.biz_conf
  on  loan_num.product_id = biz_conf.product_id
  and biz_conf.product_id${vt} is not null
  group by 1,4,5
  -- order by 4,5,1
) as loan_num
on  product_id       = product_id_loan_num
and loan_terms       = loan_terms_loan_num
and loan_active_date = loan_active_date_loan_num
 join (
  select
    capital_id,channel_id,project_id,
    product_id${vt} as dim_product_id
  from dim_new.biz_conf
 where (case when product_id = 'pl00282' and '${ST9}' > '2020-02-22' then 0 else 1 end) = 1
) as biz_conf
on product_id = dim_product_id
-- order by biz_date,loan_active_date,product_id,loan_terms,overdue_dob
-- limit 10
;
