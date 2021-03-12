set spark.executor.memory=2g;
set spark.executor.memoryOverhead=1g;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;             -- 关闭自动 MapJoin
set hive.mapjoin.optimized.hashtable=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;

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
from dw_new.dw_transaction_blend_record t
left join dim_new.biz_conf b
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
    select * from dw_new.dw_transaction_blend_record
    lateral view explode(
        split(
            concat_ws(',','1','2','3','4'),','
        )
    ) t as no
) tmp
left join dim_new.biz_conf b
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
        from dw_new.dw_transaction_blend_record
        where biz_date = '${ST9}'
    ) t
    left join dim_new.biz_conf b
    on t.product_code = b.product_id
    left join
    (
        select project_id,repay_sum_daily
        from dw_new${db_suffix}.dw_asset_info_day
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
    select * from dw_new.dw_transaction_blend_record
    lateral view explode (
        split(
            concat_ws(',','1','2','3','4'),','
        )
    ) t as no
) tmp
left join dim_new.biz_conf b
on tmp.product_code = b.product_id;
