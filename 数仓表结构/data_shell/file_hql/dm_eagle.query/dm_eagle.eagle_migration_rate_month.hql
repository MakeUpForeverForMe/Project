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
  from dw${db_suffix}.dw_loan_base_stat_overdue_num_day as overdue_num
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
