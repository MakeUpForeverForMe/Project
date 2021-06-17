select
  -- count(distinct stage.due_bill_no) as due_bill_no_stage, count(distinct ods.due_bill_no) as due_bill_no_ods, if(count(distinct stage.due_bill_no)  != count(distinct ods.due_bill_no), 1,0) as due_bill_no,
  -- count(distinct stage.order_id)    as order_id_stage,    count(distinct ods.order_id)    as order_id_ods,    if(count(distinct stage.order_id)     != count(distinct ods.order_id),    1,0) as order_id,
  stage.project_id                  as project_id_stage,  ods.project_id                  as product_id_ods,  if(nvl(stage.project_id,         '1') != nvl(ods.project_id,         '1'),1,0) as project_id,
  stage.due_bill_no                 as due_bill_no_stage, ods.due_bill_no                 as due_bill_no_ods, if(nvl(stage.due_bill_no,        '1') != nvl(ods.due_bill_no,        '1'),1,0) as due_bill_no,
  stage.order_id                    as order_id_stage,    ods.order_id                    as order_id_ods,    if(nvl(stage.order_id,           '1') != nvl(ods.order_id,           '1'),1,0) as order_id,
  stage.pay_channel                 as pay_channel_stage, ods.pay_channel                 as pay_channel_ods, if(nvl(stage.pay_channel,        '1') != nvl(ods.pay_channel,        '1'),1,0) as pay_channel,
  -- stage.order_status                as order_status_stage,ods.order_status                as order_status_ods,if(nvl(stage.order_status,       '1') != nvl(ods.order_status,       '1'),1,0) as order_status,
  -- stage.txn_amt                     as txn_amt_stage,     ods.txn_amt                     as txn_amt_ods,     if(nvl(stage.txn_amt,            '1') != nvl(ods.txn_amt,            '1'),1,0) as txn_amt,
  -- stage.txn_date                    as txn_date_stage,    ods.txn_date                    as txn_date_ods,    if(nvl(stage.txn_date,           '1') != nvl(ods.txn_date,           '1'),1,0) as txn_date,

  -- stage.loan_usage          as loan_usage_stage,         ods.loan_usage          as loan_usage_ods,         if(nvl(stage.loan_usage,         '1') != nvl(ods.loan_usage,         '1'),1,0) as loan_usage,
  -- stage.currency            as currency_stage,           ods.currency            as currency_ods,           if(nvl(stage.currency,           '1') != nvl(ods.currency,           '1'),1,0) as currency,
  -- stage.message             as message_stage,            ods.message             as message_ods,            if(nvl(stage.message,            '1') != nvl(ods.message,            '1'),1,0) as message,
  -- stage.business_date       as business_date_stage,      ods.business_date       as business_date_ods,      if(nvl(stage.business_date,      '1') != nvl(ods.business_date,      '1'),1,0) as business_date,
  -- stage.bank_trade_act_no   as bank_trade_act_no_stage,  ods.bank_trade_act_no   as bank_trade_act_no_ods,  if(nvl(stage.bank_trade_act_no,  '1') != nvl(ods.bank_trade_act_no,  '1'),1,0) as bank_trade_act_no,
  -- stage.bank_trade_act_name as bank_trade_act_name_stage,ods.bank_trade_act_name as bank_trade_act_name_ods,if(nvl(stage.bank_trade_act_name,'1') != nvl(ods.bank_trade_act_name,'1'),1,0) as bank_trade_act_name,

  stage.create_time         as create_time_stage,        ods.create_time         as create_time_ods,        if(nvl(stage.create_time,        '1') != nvl(ods.create_time,        '1'),1,0) as create_time,
  stage.update_time         as update_time_stage,        ods.update_time         as update_time_ods,        if(nvl(stage.update_time,        '1') != nvl(ods.update_time,        '1'),1,0) as update_time,

  '123' as aa
from (
  select
    is_empty(order_number)                        as order_id,
    is_empty(serial_number)                       as due_bill_no,
    is_empty(trade_channel)                       as pay_channel,
    is_empty(trade_status)                        as order_status,
    is_empty(trade_type)                          as loan_usage,
    cast(is_empty(order_amount) as decimal(25,5)) as txn_amt,
    is_empty(trade_currency)                      as currency,
    is_empty(trade_digest)                        as message,
    is_empty(confirm_repay_time)                  as business_date,
    decrypt_aes(bank_account,'tencentabs123456')  as bank_trade_act_no,
    decrypt_aes(name,'tencentabs123456')          as bank_trade_act_name,
    is_empty(trade_time)                          as txn_date,
    is_empty(create_time)                         as create_time,
    is_empty(update_time)                         as update_time,
    is_empty(project_id)                          as project_id
  from stage.t_06_assettradeflow
  where 1 > 0
    and project_id in (select distinct is_empty(map_from_str(extra_info)['项目编号'],project_id) from ods.t_asset_pay_flow)
    -- and project_id = 'cl00187'
) as stage
join (
  select
    is_empty(map_from_str(extra_info)['订单号'],order_id)                                                       as order_id,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                                                       as due_bill_no,
    is_empty(map_from_str(extra_info)['交易渠道'],trade_channel)                                                as pay_channel,
    is_empty(map_from_str(extra_info)['交易状态'],trade_status)                                                 as order_status,
    is_empty(map_from_str(extra_info)['交易类型'],trade_type)                                                   as loan_usage,
    cast(is_empty(map_from_str(extra_info)['订单金额(元)'],order_amount) as decimal(25,5))                      as txn_amt,
    is_empty(map_from_str(extra_info)['交易币种'],trade_currency)                                               as currency,
    is_empty(map_from_str(extra_info)['交易摘要'],trad_desc)                                                    as message,
    case
    when length(map_from_str(extra_info)['确认还款日期']) = 19 then to_date(map_from_str(extra_info)['确认还款日期'])
    else is_empty(datefmt(map_from_str(extra_info)['确认还款日期'],'','yyyy-MM-dd')) end                        as business_date,
    decrypt_aes(is_empty(map_from_str(extra_info)['银行帐号'],bank_account),'tencentabs123456')                 as bank_trade_act_no,
    decrypt_aes(is_empty(map_from_str(extra_info)['姓名'],name),'tencentabs123456')                             as bank_trade_act_name,
    is_empty(datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd'),datefmt(trade_time,'','yyyy-MM-dd')) as txn_date,
    create_time                                                                                                 as create_time,
    update_time                                                                                                 as update_time,
    is_empty(map_from_str(extra_info)['项目编号'],project_id)                                                   as project_id
  from ods.t_asset_pay_flow
) as ods
on  ods.project_id  = stage.project_id
and ods.due_bill_no = stage.due_bill_no
and ods.order_id    = stage.order_id
and ods.loan_usage  = stage.loan_usage
and ods.txn_date    = stage.txn_date
where 1 > 0
  and (
    -- nvl(stage.due_bill_no,        '1') != nvl(ods.due_bill_no,        '1') or
    -- nvl(stage.order_id,           '1') != nvl(ods.order_id,           '1') or

    -- nvl(stage.txn_amt,            '1') != nvl(ods.txn_amt,            '1') or -- 350笔不一样      -- 交易金额
    nvl(stage.pay_channel,        '1') != nvl(ods.pay_channel,        '1') or -- 1个项目不同      -- 支付渠道
    -- nvl(stage.order_status,       '1') != nvl(ods.order_status,       '1') or -- 完全相同         -- 订单状态（成功、失败等）
    -- nvl(stage.txn_date,           '1') != nvl(ods.txn_date,           '1') or -- 完全相同         -- 交易日期
    -- nvl(stage.loan_usage,         '1') != nvl(ods.loan_usage,         '1') or -- 完全相同         -- 贷款用途（回购等）

    -- nvl(stage.currency,           '1') != nvl(ods.currency,           '1') or -- 完全相同         -- 币种              -- 不关心
    -- nvl(stage.message,            '1') != nvl(ods.message,            '1') or -- 5笔星云有\t问题  -- 描述              -- 不关心
    -- nvl(stage.business_date,      '1') != nvl(ods.business_date,      '1') or -- 375笔不一样      -- 确认还款日期      -- 不关心
    -- nvl(stage.bank_trade_act_no,  '1') != nvl(ods.bank_trade_act_no,  '1') or -- 375笔不一样      -- 银行付款账号      -- 不关心
    -- nvl(stage.bank_trade_act_name,'1') != nvl(ods.bank_trade_act_name,'1') or -- 完全相同         -- 银行付款账户名称  -- 不关心

    false
  )
-- group by stage.project_id,ods.project_id,
-- stage.pay_channel, ods.pay_channel,
-- -- stage.order_status,ods.order_status,
-- -- stage.txn_amt,     ods.txn_amt,
-- -- stage.txn_date,    ods.txn_date,

-- -- stage.loan_usage,         ods.loan_usage,
-- -- stage.currency,           ods.currency,
-- -- stage.message,            ods.message,
-- -- stage.business_date,      ods.business_date,
-- -- stage.bank_trade_act_no,  ods.bank_trade_act_no,
-- -- stage.bank_trade_act_name,ods.bank_trade_act_name,

-- '123'
order by project_id_stage
,due_bill_no_stage,order_id_stage
limit 50
;







select * from stage.t_06_assettradeflow
where 1 > 0
  -- and project_id = '001601'
  and serial_number = '5100791412'
  and order_number = 'HT687552'
;


select
  order_id                                                                                                    as order_id_col,
  is_empty(map_from_str(extra_info)['订单号'],order_id)                                                       as order_id,
  asset_id                                                                                                    as due_bill_no_col,
  is_empty(map_from_str(extra_info)['借据号'],asset_id)                                                       as due_bill_no,
  trade_channel                                                                                               as pay_channel_col,
  is_empty(map_from_str(extra_info)['交易渠道'],trade_channel)                                                as pay_channel,
  trade_status                                                                                                as order_status_col,
  is_empty(map_from_str(extra_info)['交易状态'],trade_status)                                                 as order_status,
  trade_type                                                                                                  as loan_usage_col,
  is_empty(map_from_str(extra_info)['交易类型'],trade_type)                                                   as loan_usage,
  cast(order_amount as decimal(25,5))                                                                         as txn_amt_col,
  cast(is_empty(map_from_str(extra_info)['订单金额(元)'],order_amount) as decimal(25,5))                      as txn_amt,
  trade_currency                                                                                              as currency_col,
  is_empty(map_from_str(extra_info)['交易币种'],trade_currency)                                               as currency,
  trad_desc                                                                                                   as message_col,
  is_empty(map_from_str(extra_info)['交易摘要'],trad_desc)                                                    as message,
  case
  when length(map_from_str(extra_info)['确认还款日期']) = 19 then to_date(map_from_str(extra_info)['确认还款日期'])
  else is_empty(datefmt(map_from_str(extra_info)['确认还款日期'],'','yyyy-MM-dd')) end                        as business_date,
  bank_account                                                                                                as bank_trade_act_no_col,
  is_empty(map_from_str(extra_info)['银行帐号'],bank_account)                                                 as bank_trade_act_no,
  name                                                                                                        as bank_trade_act_name_col,
  is_empty(map_from_str(extra_info)['姓名'],name)                                                             as bank_trade_act_name,
  datefmt(trade_time,'','yyyy-MM-dd')                                                                         as txn_date_col,
  is_empty(datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd'),datefmt(trade_time,'','yyyy-MM-dd')) as txn_date,
  create_time                                                                                                 as create_time,
  update_time                                                                                                 as update_time,
  is_empty(map_from_str(extra_info)['项目编号'],project_id)                                                   as project_id
from ods.t_asset_pay_flow
where 1 > 0
  -- and project_id = '001601'
  and is_empty(map_from_str(extra_info)['借据号'],asset_id) = '5100791412'
  and (
    is_empty(map_from_str(extra_info)['订单号'],order_id) = 'HT687552' or
    is_empty(map_from_str(extra_info)['订单号'])          = 'HT687552' or
    order_id                                              = 'HT687552'
  )
;





select order_number,count(1) as cnt from stage.t_06_assettradeflow
where 1 > 0
  -- and project_id in ('cl00185','001601','cl00199','cl00217','cl00229','cl00243')
  and serial_number = '5100791412'
  and order_number = 'HT687552'
group by order_number
having count(1) > 1
order by order_number
;
