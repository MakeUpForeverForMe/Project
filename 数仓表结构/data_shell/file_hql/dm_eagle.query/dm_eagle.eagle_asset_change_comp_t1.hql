set spark.executor.memory=2g;
set spark.executor.memoryOverhead=1g;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;             -- 关闭自动 MapJoin
set hive.mapjoin.optimized.hashtable=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;

insert overwrite table dm_eagle.eagle_asset_change_comp_t1 partition (biz_date='${ST9}')
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
    from dm_eagle.eagle_asset_change_comp_t1
    where biz_date=date_sub('${ST9}',1)
) last_dm
on last_dw.project_id = last_dm.project_id
left join dim_new.biz_conf b
on today_dw.project_id = b.project_id;

insert overwrite table dm_eagle.eagle_asset_change_comp partition (biz_date='${ST9}')
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

insert overwrite table dm_eagle.eagle_asset_comp_info partition (biz_date='${ST9}')
select
    distinct
    b.capital_id                                                                as capital_id,
    b.channel_id                                                                as channel_id,
    b.project_id                                                                as project_id,
    t.repay_sum_daily                                                           as repay_sum_daily,
    t.no1                                                                       as repay_way,
    t.no2                                                                       as amount_type,
    nvl(case
        when t.no1='1' and t.no2='1' then cust_repay_principal
        when t.no1='1' and t.no2='2' then cust_repay_interest
        when t.no1='1' and t.no2='3' then cust_repay_penalty
        when t.no1='1' and t.no2='4' then cust_repay_fee
        when t.no1='2' and t.no2='1' then comp_repay_principal
        when t.no1='2' and t.no2='2' then comp_repay_interest
        when t.no1='2' and t.no2='3' then comp_repay_penalty
        when t.no1='2' and t.no2='4' then comp_repay_fee
        when t.no1='3' and t.no2='1' then repo_repay_principal
        when t.no1='3' and t.no2='2' then repo_repay_interest
        when t.no1='3' and t.no2='3' then repo_repay_penalty
        when t.no1='3' and t.no2='4' then repo_repay_fee
        when t.no1='4' and t.no2='1' then 0
        when t.no1='4' and t.no2='2' then 0
        when t.no1='4' and t.no2='3' then 0
        when t.no1='4' and t.no2='4' then 0
        when t.no1='5' and t.no2='1' then return_repay_principal
        when t.no1='5' and t.no2='2' then return_repay_interest
        when t.no1='5' and t.no2='3' then return_repay_penalty
        when t.no1='5' and t.no2='4' then return_repay_fee
    else 0 end,0)                                                                         as amount
from
(
    select *
    from
    (
        select *
        from dw_new${db_suffix}.dw_asset_info_day
        lateral view explode(
            split(
                concat_ws(',','1','2','3','4','5'),','
            )
        ) t1 as no1
    ) tmp
    lateral view explode(
        split(
            concat_ws(',','1','2','3','4'),','
        )
    ) t2 as no2
) t
left join dim_new.biz_conf b
on t.project_id=b.project_id
where t.biz_date='${ST9}';
