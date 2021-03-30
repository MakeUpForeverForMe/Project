set spark.executor.memory=1g;
set spark.executor.memoryOverhead=512M;
set hive.auto.convert.join=false;            -- 关闭自动 MapJoin
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;

insert overwrite table dw.dw_transaction_blend_record partition (biz_date)
select
  `blend_serial_no`       ,
  `record_type`           ,
  `loan_amt`              ,
  `cust_repay_amt`        ,
  `comp_bak_amt`          ,
  `buy_bak_amt`           ,
  `deduct_sve_fee`        ,
  `invest_amt`            ,
  `invest_redeem_amt`     ,
  `invest_earning`        ,
  `acct_int`              ,
  `acct_fee`              ,
  `tax_amt`               ,
  `invest_cash`           ,
  `ci_fund`               ,
  `ci_redeem_amt`         ,
  `ci_earning`            ,
  `other_amt`             ,
  `trade_day_bal`         ,
  `trade_yesterday_bal`   ,
  `trade_day__bal_diff`   ,
  `remark`                ,
  `create_date`           ,
  `update_date`           ,
  `calc_date`             ,
  `product_code`          ,
  `product_name`          ,
  `return_ticket_bak_amt` ,
  `ch_diff_explain`       ,
  `en_diff_explain`       ,
  `calc_date`
from stage.t_transaction_blend_record
where d_date='${ST9}' and record_type='G'
;
