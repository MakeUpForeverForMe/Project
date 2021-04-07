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


insert overwrite table dm_eagle${db_suffix}.eagle_asset_scale_principal_day partition(biz_date,product_id)
select
  capital_id,channel_id,project_id,
  loan_terms               as loan_terms,
  sum(remain_principal)    as remain_principal,
  sum(overdue_principal)   as overdue_principal,
  sum(unposted_principal)  as unposted_principal,
  if(sum(remain_principal) = 0,0,sum(overdue_principal) / sum(remain_principal) * 100) as overdue_principal_rate,
  if(sum(remain_principal) = 0,0,sum(unposted_principal) / sum(remain_principal) * 100) as unposted_principal_rate,
  biz_date                 as biz_date,
  biz_conf.product_id${vt} as product_id
from (
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
join (
  select *
  from dw${db_suffix}.dw_loan_base_stat_overdue_num_day
  where 1 > 0
    and biz_date = '${ST9}'
    -- and biz_date like '2020%'
    and if(
      product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402'
      ),
      overdue_days < 180,
      true
    )
) as overdue_num
on overdue_num.product_id = biz_conf.product_id
group by capital_id,channel_id,project_id,loan_terms,biz_date,biz_conf.product_id${vt}
-- limit 10
;
