set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

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



-- #---------------------------------------------- 手动跑批参数 ----------------------------------------------# --

-- 乐信代偿前
-- set hivevar:db_suffix=;set hivevar:tb_suffix=_asset;

-- 乐信代偿后
-- set hivevar:db_suffix=_cps;set hivevar:tb_suffix=;

-- 滴滴汇通瓜子
-- set hivevar:db_suffix=;set hivevar:tb_suffix=;


-- 乐信产品编号
-- set hivevar:product_id=
--   select distinct product_id
--   from (
--     select
--       max(if(col_name = 'channel_id',col_val,null)) as channel_id,
--       max(if(col_name = 'product_id',col_val,null)) as product_id
--     from dim.data_conf
--     where col_type = 'ac'
--     group by col_id
--   ) as tmp
--   where channel_id = '0006'
-- ;


-- 滴滴汇通瓜子产品编号
-- set hivevar:product_id='DIDI201908161538','001601','001602','001603','001701','001702';


-- 手动跑批日志
-- set hivevar:ST9=2021-03-23;


-- 跑全量的时候用的参数
-- set hivevar:join_str=;

-- #---------------------------------------------- 手动跑批参数 ----------------------------------------------# --



-- #---------------------------------------------- 自动跑批参数 ----------------------------------------------# --

-- 修数重跑天数
set hivevar:days=20;

-- 各表的 where 条件
set hivevar:where_date=and d_date = '${ST9}';

-- 跑增量的时候用的参数
set hivevar:join_str=where if(schedule_repay_order_info_ddht.order_id is not null, schedule_repay_order_info_ddht.paid_out_date,nvl(txn_date,datefmt(cast(txn_time as string),'ms','yyyy-MM-dd'))) between date_sub('${ST9}',${days}) and '${ST9}';

-- #---------------------------------------------- 自动跑批参数 ----------------------------------------------# --


insert overwrite table ods${db_suffix}.order_info partition(biz_date,product_id)
select
  ecas_order_res.order_id,
  ecas_order_res.apply_no,
  ecas_order_res.due_bill_no,
  ecas_order_res.term,
  ecas_order_res.pay_channel,
  ecas_order_res.command_type,
  ecas_order_res.order_status,
  ecas_order_res.repay_way,
  ecas_order_res.txn_amt,
  ecas_order_res.success_amt,
  ecas_order_res.currency,
  ecas_order_res.business_date,
  ecas_order_res.loan_usage,
  ecas_order_res.purpose,
  ecas_order_res.bank_trade_act_no,
  ecas_order_res.bank_trade_act_name,
  ecas_order_res.bank_trade_act_phone,
  ecas_order_res.txn_time,
  ecas_order_res.txn_date,
  ecas_order_res.create_time,
  ecas_order_res.update_time,
  ecas_order_res.biz_date,
  ecas_loan.product_id
from (
  select
    ecas_order.order_id,
    ecas_order.apply_no,
    ecas_order.due_bill_no,
    ecas_order.term,
    ecas_order.pay_channel,
    ecas_order.command_type,
    ecas_order.order_status,
    ecas_order.repay_way,
    ecas_order.txn_amt,
    ecas_order.success_amt,
    ecas_order.currency,
    ecas_order.business_date,
    ecas_order.loan_usage,
    ecas_order.purpose,
    ecas_order.bank_trade_act_no,
    ecas_order.bank_trade_act_name,
    ecas_order.bank_trade_act_phone,
    ecas_order.txn_time,
    ecas_order.txn_date,
    ecas_order.create_time,
    ecas_order.update_time,
    if(schedule_repay_order_info_ddht.order_id is not null, schedule_repay_order_info_ddht.paid_out_date, ecas_order.biz_date) as biz_date
  from (
    select
      order_id,
      apply_no,
      due_bill_no,
      term,
      pay_channel,
      command_type,
      order_status,
      repay_way,
      txn_amt,
      success_amt,
      currency,
      business_date,
      loan_usage,
      purpose,
      bank_trade_act_no,
      bank_trade_act_name,
      bank_trade_act_phone,
      datefmt(cast(txn_time as string),'ms','yyyy-MM-dd HH:mm:ss') as txn_time,
      txn_date,
      nvl(datefmt(cast(create_time as string),'ms','yyyy-MM-dd HH:mm:ss'),cast(business_date as timestamp)) as create_time,
      nvl(datefmt(cast(lst_upd_time as string),'ms','yyyy-MM-dd HH:mm:ss'),cast(business_date as timestamp)) as update_time,
      nvl(txn_date,datefmt(cast(txn_time as string),'ms','yyyy-MM-dd')) as biz_date
    from stage.ecas_order_hst${tb_suffix}
    where 1 > 0
      ${where_date}
    union all
    select
      order_id,
      apply_no,
      due_bill_no,
      term,
      pay_channel,
      command_type,
      order_status,
      repay_way,
      txn_amt,
      success_amt,
      currency,
      business_date,
      loan_usage,
      purpose,
      bank_trade_act_no,
      bank_trade_act_name,
      bank_trade_act_phone,
      datefmt(cast(txn_time as string),'ms','yyyy-MM-dd HH:mm:ss') as txn_time,
      txn_date,
      nvl(datefmt(cast(create_time as string),'ms','yyyy-MM-dd HH:mm:ss'),cast(business_date as timestamp)) as create_time,
      nvl(datefmt(cast(lst_upd_time as string),'ms','yyyy-MM-dd HH:mm:ss'),cast(business_date as timestamp)) as update_time,
      nvl(txn_date,datefmt(cast(txn_time as string),'ms','yyyy-MM-dd')) as biz_date
    from stage.ecas_order${tb_suffix}
    where 1 > 0
      ${where_date}
  ) as ecas_order
  left join (
    select
      order_id,
      max(paid_out_date) as paid_out_date
    from stage.schedule_repay_order_info_ddht
    where 1 > 0
      and biz_date = date_sub(current_date,1)
    group by order_id
  ) as schedule_repay_order_info_ddht
  on ecas_order.order_id = schedule_repay_order_info_ddht.order_id
  ${join_str}
) as ecas_order_res
join (
  select
    product_code as product_id,
    due_bill_no
  from stage.ecas_loan${tb_suffix}
  where 1 > 0
    ${where_date}
    and d_date <= current_date()
    and product_code in (${product_id})
) as ecas_loan
on ecas_order_res.due_bill_no = ecas_loan.due_bill_no
-- limit 10
;
