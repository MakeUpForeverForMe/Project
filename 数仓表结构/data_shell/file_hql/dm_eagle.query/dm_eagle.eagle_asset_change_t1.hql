set spark.executor.memory=2g;
set spark.executor.memoryOverhead=1g;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;             -- 关闭自动 MapJoin
set hive.mapjoin.optimized.hashtable=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;

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
    from dw_new${db_suffix}.dw_asset_info_day
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
    from dw_new${db_suffix}.dw_asset_info_day
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
left join dim_new.biz_conf b
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
    from dw_new${db_suffix}.dw_asset_info_day
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
    from dw_new${db_suffix}.dw_asset_info_day
    where biz_date=date_sub('${ST9}',1)
) last_dw
on today_dw.project_id = last_dw.project_id
left join dim_new.biz_conf b
on today_dw.project_id = b.project_id;


