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

insert overwrite table dm_eagle.eagle_asset_change_t1 partition (biz_date='${ST9}')
select
    distinct
    b.capital_id                                                                               as capital_id,
    b.channel_id                                                                               as channel_id,
    nvl(today_dw.project_id,last_dw.project_id)                                                as project_id,
    nvl(last_dm.today_remain_sum,0)                                                            as yesterday_remain_sum,
    nvl(today_dw.loan_sum_daily,0)                                                             as loan_sum_daily,
    nvl(last_dw.repay_principal,0)                                                             as repay_sum_daily,
    nvl(last_dw.repay_other_sum_daily,0)                                                       as repay_other_sum_daily,
    nvl(last_dm.today_remain_sum,0) + nvl(today_dw.loan_sum_daily,0) - nvl(last_dw.repay_principal,0) as today_remain_sum
from
(
    select
        project_id,
        remain_principal,
        loan_sum_daily,
        repay_principal,
        repay_interest + repay_penalty + repay_fee as repay_other_sum_daily
    from dw${db_suffix}.dw_asset_info_day
    where biz_date='${ST9}'
) today_dw
left join
(
    select
        project_id,
        remain_principal,
        loan_sum_daily,
        repay_principal,
        repay_interest + repay_penalty + repay_fee as repay_other_sum_daily
    from dw${db_suffix}.dw_asset_info_day
    where biz_date=date_sub('${ST9}',1)
) last_dw
on today_dw.project_id = last_dw.project_id
left join
(
    select
        project_id,
        today_remain_sum
    from dm_eagle.eagle_asset_change
    where biz_date=date_sub('${ST9}',1)
) last_dm
on last_dw.project_id = last_dm.project_id
left join (
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
) b
on today_dw.project_id = b.project_id;

insert overwrite table dm_eagle.eagle_asset_change partition (biz_date='${ST9}')
select
    distinct
    b.capital_id                                                                               as capital_id,
    b.channel_id                                                                               as channel_id,
    nvl(today_dw.project_id,last_dw.project_id)                                                as project_id,
    nvl(last_dw.remain_principal,0)                                                            as yesterday_remain_sum,
    nvl(today_dw.loan_sum_daily,0)                                                             as loan_sum_daily,
    nvl(today_dw.repay_principal,0)                                                            as repay_sum_daily,
    nvl(today_dw.repay_other_sum_daily,0)                                                      as repay_other_sum_daily,
    nvl(today_dw.remain_principal,0)                                                           as today_remain_sum
from
(
    select
        project_id,
        remain_principal,
        loan_sum_daily,
        repay_principal,
        repay_interest + repay_penalty + repay_fee as repay_other_sum_daily
    from dw${db_suffix}.dw_asset_info_day
    where biz_date='${ST9}'
) today_dw
left join
(
    select
        project_id,
        remain_principal,
        loan_sum_daily,
        repay_principal,
        repay_interest + repay_penalty + repay_fee as repay_other_sum_daily
    from dw${db_suffix}.dw_asset_info_day
    where biz_date=date_sub('${ST9}',1)
) last_dw
on today_dw.project_id = last_dw.project_id
left join (
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
) b
on today_dw.project_id = b.project_id;


