#!/bin/bash
ST9=`date +%Y-%m-%d -d "-1 days"`
##测试
##beeline -n hadoop -u "jdbc:hive2://10.83.1.157:7001"  
beeline -n hadoop -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e "

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

insert overwrite table dw.dw_transaction_blend_record partition (biz_date)
select
    blend_serial_no       ,
    record_type           ,
    loan_amt              ,
    cust_repay_amt        ,
    comp_bak_amt          ,
    buy_bak_amt           ,
    deduct_sve_fee        ,
    invest_amt            ,
    invest_redeem_amt     ,
    invest_earning        ,
    acct_int              ,
    acct_fee              ,
    tax_amt               ,
    invest_cash           ,
    ci_fund               ,
    ci_redeem_amt         ,
    ci_earning            ,
    other_amt             ,
    trade_day_bal         ,
    trade_yesterday_bal   ,
    trade_day__bal_diff   ,
    remark                ,
    create_date           ,
    update_date           ,
    calc_date             ,
    product_code          ,
    product_name          ,
    return_ticket_bak_amt ,
    ch_diff_explain       ,
    en_diff_explain       ,
    calc_date   
from stage.t_transaction_blend_record
where d_date='${ST9}' and record_type='G';
";