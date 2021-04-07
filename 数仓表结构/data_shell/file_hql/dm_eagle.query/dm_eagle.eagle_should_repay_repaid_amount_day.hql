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
  from dw${db_suffix}.dw_loan_base_stat_should_repay_day as should_repay
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
  from dw${db_suffix}.dw_loan_base_stat_repay_detail_day as repay_detail
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
  on  repay_detail.product_id = biz_conf.product_id
  and repay_detail.biz_date = '${ST9}'
  -- and repay_detail.biz_date like '2019%'
  group by loan_terms,biz_date,project_id
) as repay_detail
on  should_biz_date   = repaid_biz_date
and should_project_id = repaid_project_id
and should_loan_terms = repaid_loan_terms
left join (
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
) as biz_conf
on nvl(should_project_id,repaid_project_id) = project_id
-- order by biz_date,product_id,loan_terms
-- limit 10
;
