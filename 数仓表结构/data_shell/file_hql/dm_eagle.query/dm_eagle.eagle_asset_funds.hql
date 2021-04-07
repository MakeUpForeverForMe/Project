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

insert overwrite table dm_eagle.eagle_funds partition (biz_date)
select
    b.capital_id                                                                as capital_id,
    b.channel_id                                                                as channel_id,
    b.project_id                                                                as project_id,
    t.trade_yesterday_bal                                                       as trade_yesterday_bal,
    t.loan_amt                                                                  as loan_today_bal,
    t.cust_repay_amt + t.comp_bak_amt + t.buy_bak_amt + t.return_ticket_bak_amt as repay_today_bal,
    t.acct_int + t.invest_earning + t.ci_earning                                as acct_int,
    t.acct_fee + t.deduct_sve_fee + t.other_amt + t.tax_amt                     as acct_total_fee,
    t.invest_cash                                                               as invest_cash,
    t.trade_day_bal                                                             as trade_today_bal,
    t.biz_date                                                                  as biz_date
from dw.dw_transaction_blend_record t
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
on t.product_code = b.product_id;

insert overwrite table dm_eagle.eagle_acct_cost partition (biz_date)
select
    b.capital_id                                                                as capital_id,
    b.channel_id                                                                as channel_id,
    b.project_id                                                                as project_id,
    acct_fee + deduct_sve_fee + other_amt + tax_amt                             as acct_total_fee,
    no                                                                          as fee_type,
    case no
        when '1' then acct_fee
        when '2' then deduct_sve_fee
        when '3' then other_amt
        when '4' then tax_amt  else null end                                    as amount,
    biz_date                                                                    as biz_date
from
(
    select * from dw.dw_transaction_blend_record
    lateral view explode(
        split(
            concat_ws(',','1','2','3','4'),','
        )
    ) t as no
) tmp
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
on tmp.product_code = b.product_id;

insert overwrite table dm_eagle.eagle_unreach_funds partition (biz_date='${ST9}')
select
    coalesce(today.project_id,yest.project_id)                                        as project_id,
    nvl(yest.trade_today_bal,0)                                                       as trade_yesterday_bal,
    nvl(today.repay_today_bal,0)                                                      as repay_today_bal,
    nvl(today.repay_sum_daily,0)                                                      as repay_sum_daily,
    nvl(yest.trade_today_bal,0) - nvl(today.repay_today_bal,0) + nvl(today.repay_sum_daily,0) as trade_today_bal
from
(
    select
        b.project_id                                                                  as project_id,
        (t.cust_repay_amt + t.comp_bak_amt + t.buy_bak_amt + t.return_ticket_bak_amt) as repay_today_bal,
        nvl(a.repay_sum_daily,0)                                                      as repay_sum_daily
    from
    (
        select product_code,cust_repay_amt,comp_bak_amt,buy_bak_amt,return_ticket_bak_amt
        from dw.dw_transaction_blend_record
        where biz_date = '${ST9}'
    ) t
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
    on t.product_code = b.product_id
    left join
    (
        select project_id,repay_sum_daily
        from dw${db_suffix}.dw_asset_info_day
        where biz_date = '${ST9}'
    ) a
    on b.project_id = a.project_id
) today
left join
(
    select * from dm_eagle.eagle_unreach_funds
    where biz_date = date_sub('${ST9}',1)
) yest
on today.project_id = yest.project_id;

insert overwrite table dm_eagle.eagle_repayment_detail partition (biz_date)
select
    b.capital_id                                                                as capital_id,
    b.channel_id                                                                as channel_id,
    b.project_id                                                                as project_id,
    cust_repay_amt + comp_bak_amt + buy_bak_amt + return_ticket_bak_amt         as repay_today_bal,
    no                                                                          as repay_type,
    case no
        when '1' then cust_repay_amt
        when '2' then comp_bak_amt
        when '3' then buy_bak_amt
        when '4' then return_ticket_bak_amt  else null end                      as amount,
    biz_date                                                                    as biz_date
from
(
    select * from dw.dw_transaction_blend_record
    lateral view explode (
        split(
            concat_ws(',','1','2','3','4'),','
        )
    ) t as no
) tmp
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
on tmp.product_code = b.product_id;
