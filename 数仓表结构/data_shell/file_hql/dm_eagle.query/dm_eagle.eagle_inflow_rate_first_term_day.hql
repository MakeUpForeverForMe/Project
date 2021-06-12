set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;
set hive.groupby.orderby.position.alias=true;
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

set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;

--set hivevar:ST9=2021-06-07;
--SET hivevar:hive_param_str=;
--set hivevar:db_suffix=;
--set hivevar:vt=_vt; 

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
  from dw${db_suffix}.dw_loan_base_stat_overdue_num_day as overdue_num
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
  )biz_conf
  on  overdue_num.product_id = biz_conf.product_id
  and biz_conf.product_id${vt} is not null
  where is_buy_back <> 1 and overdue_num.biz_date = '${ST9}'
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
  from dw${db_suffix}.dw_loan_base_stat_overdue_num_day as overdue_num
  join (select distinct
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
    )biz_conf
  on  overdue_num.product_id = biz_conf.product_id
  and biz_conf.product_id${vt} is not null
  and overdue_num.is_first_term_overdue = 1
  and overdue_num.overdue_days between 1 and 30
  and overdue_num.overdue_mob <= 12
  where is_buy_back <> 1
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
  from dw${db_suffix}.dw_loan_base_stat_loan_num_day as loan_num
  join (
  select distinct
       capital_id,
       channel_id,
       project_id,
       product_id_vt ,
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
    )biz_conf
  on  loan_num.product_id = biz_conf.product_id
  and biz_conf.product_id${vt} is not null
  group by 1,4,5
  -- order by 4,5,1
) as loan_num
on  product_id       = product_id_loan_num
and loan_terms       = loan_terms_loan_num
and loan_active_date = loan_active_date_loan_num
 join (
    select distinct
       capital_id,
       channel_id,
       project_id,
       product_id${vt} as  dim_product_id
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
 where (case when product_id = 'pl00282' and '${ST9}' > '2020-02-22' then 0 else 1 end) = 1
) as biz_conf
on product_id = dim_product_id
-- order by biz_date,loan_active_date,product_id,loan_terms,overdue_dob
-- limit 10
;
