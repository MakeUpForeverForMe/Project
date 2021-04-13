set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=2048;
set tez.am.resource.memory.mb=2048;
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



insert overwrite table ods.order_info partition(biz_date,product_id)
select distinct
  is_empty(map_from_str(extra_info)['订单号'],order_id)                                                                          as order_id,
  is_empty(map_from_str(extra_info)['借据号'],asset_id)                                                                          as apply_no,
  is_empty(map_from_str(extra_info)['借据号'],asset_id)                                                                          as due_bill_no,
  null                                                                                                                           as term,
  is_empty(map_from_str(extra_info)['交易渠道'],trade_channel)                                                                   as pay_channel,
  null                                                                                                                           as command_type,
  is_empty(map_from_str(extra_info)['交易状态'],trade_status)                                                                    as order_status,
  null                                                                                                                           as repay_way,
  is_empty(map_from_str(extra_info)['订单金额(元)'],order_amount)                                                                as txn_amt,
  null                                                                                                                           as success_amt,
  is_empty(map_from_str(extra_info)['交易币种'],trade_currency)                                                                  as currency,
  case when length(map_from_str(extra_info)['确认还款日期']) = 19 then to_date(map_from_str(extra_info)['确认还款日期'])
  else is_empty(datefmt(map_from_str(extra_info)['确认还款日期'],'','yyyy-MM-dd')) end                                           as business_date,
  is_empty(map_from_str(extra_info)['交易类型'],trade_type)                                                                      as loan_usage,
  null                                                                                                                           as purpose,
  sha256(decrypt_aes(is_empty(map_from_str(extra_info)['银行帐号'],bank_account),'tencentabs123456'),'bankCard',1)               as bank_trade_act_no,
  sha256(decrypt_aes(is_empty(map_from_str(extra_info)['姓名'],name),'tencentabs123456'),'userName',1)                           as bank_trade_act_name,
  null                                                                                                                           as bank_trade_act_phone,
  cast(is_empty(datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd'),datefmt(trade_time,'','yyyy-MM-dd')) as timestamp) as txn_time,
  is_empty(datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd'),datefmt(trade_time,'','yyyy-MM-dd'))                    as txn_date,
  create_time                                                                                                                    as create_time,
  update_time                                                                                                                    as update_time,
  is_empty(datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd'),datefmt(trade_time,'','yyyy-MM-dd'))                    as biz_date,
  case is_empty(map_from_str(extra_info)['项目编号'],project_id)
    when 'Cl00333' then 'cl00333'
    else is_empty(map_from_str(extra_info)['项目编号'],project_id)
  end                                                                                                                            as project_id
from stage.asset_06_t_asset_pay_flow
where 1 > 0
  and is_empty(map_from_str(extra_info)['项目编号'],project_id) not in (
    '001601',           -- 汇通
    'WS0005200001',     -- 瓜子
    'CL202012280092',   -- 汇通国银
    'DIDI201908161538', -- 滴滴
    ''
  )
  and is_empty(datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd'),datefmt(trade_time,'','yyyy-MM-dd')) = '${ST9}'
  -- and is_empty(datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd'),datefmt(trade_time,'','yyyy-MM-dd')) like '2017%'
-- limit 50
;
