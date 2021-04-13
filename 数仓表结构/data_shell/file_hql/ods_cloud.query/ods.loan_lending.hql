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




insert overwrite table ods.loan_lending partition(product_id)
select distinct
  apply_no,
  contract_no,
  if(dayofmonth(loan_expiry_date) - dayofmonth(loan_issue_date) > 27,
    (year(loan_expiry_date) - year(loan_issue_date)) * 12 + month(loan_expiry_date) - month(loan_issue_date) + 1,
    (year(loan_expiry_date) - year(loan_issue_date)) * 12 + month(loan_expiry_date) - month(loan_issue_date)
  ) as contract_term,
  due_bill_no,
  guarantee_type,
  loan_usage,
  loan_issue_date,
  loan_expiry_date,
  is_empty(loan_active_date,loan_issue_date) as loan_active_date,
  is_empty(loan_expire_date,loan_expiry_date) as loan_expire_date,
  cycle_day,
  loan_type,
  loan_type_cn,
  contract_daily_interest_rate_basis,
  interest_rate_type,
  loan_init_interest_rate,
  loan_init_term_fee_rate,
  loan_init_svc_fee_rate,
  loan_init_penalty_rate,
  tail_amount,
  tail_amount_rate,
  bus_product_id,
  bus_product_name,
  mortgage_rate,
  is_empty(loan_active_date,loan_issue_date) as biz_date,
  case product_id
  when 'Cl00333' then 'cl00333'
  else product_id end as product_id
from (
  select
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                                        as apply_no,
    is_empty(map_from_str(extra_info)['贷款合同编号'],contract_code)                             as contract_no,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                                        as due_bill_no,
    is_empty(map_from_str(extra_info)['担保方式'],guarantee_type)                                as guarantee_type,
    is_empty(map_from_str(extra_info)['贷款用途'],loan_use)                                      as loan_usage,
    case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
    else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end         as loan_issue_date,
    case when length(map_from_str(extra_info)['合同结束时间']) = 19 then to_date(map_from_str(extra_info)['合同结束时间'])
    else is_empty(datefmt(map_from_str(extra_info)['合同结束时间'],'','yyyy-MM-dd')) end         as loan_expiry_date,
    case
    when length(map_from_str(extra_info)['贷款还款日']) = 1 then datefmt(map_from_str(extra_info)['贷款还款日'],'d','dd')
    when length(map_from_str(extra_info)['贷款还款日']) = 2 then map_from_str(extra_info)['贷款还款日']
    else is_empty(map_from_str(extra_info)['贷款还款日']) end                                    as cycle_day,
    is_empty(
      case when length(map_from_str(extra_info)['实际放款时间']) = 19 then to_date(map_from_str(extra_info)['实际放款时间'])
      else is_empty(datefmt(map_from_str(extra_info)['实际放款时间'],'','yyyy-MM-dd')) end,
      case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
      else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end
    )                                                                                            as loan_active_date,
    is_empty(
      case when length(map_from_str(extra_info)['最后一次预计扣款时间']) = 19 then to_date(map_from_str(extra_info)['最后一次预计扣款时间'])
      else is_empty(datefmt(map_from_str(extra_info)['最后一次预计扣款时间'],'','yyyy-MM-dd')) end,
      case when length(map_from_str(extra_info)['合同结束时间']) = 19 then to_date(map_from_str(extra_info)['合同结束时间'])
      else is_empty(datefmt(map_from_str(extra_info)['合同结束时间'],'','yyyy-MM-dd')) end
    )                                                                                            as loan_expire_date,
    case is_empty(map_from_str(extra_info)['还款方式'],repay_type)
    when '等额本金'             then 'MCEP'
    when '等额本息'             then 'MCEI'
    when '消费转分期'           then 'R'
    when '现金分期'             then 'C'
    when '账单分期'             then 'B'
    when 'POS分期'              then 'P'
    when '大额分期（专项分期）' then 'M'
    when '随借随还'             then 'MCAT'
    when '阶梯还款'             then 'STAIR'
    else is_empty(map_from_str(extra_info)['还款方式'],repay_type) end                           as loan_type,
    is_empty(map_from_str(extra_info)['还款方式'],repay_type)                                    as loan_type_cn,
    is_empty(map_from_str(extra_info)['日利率计算基础'])                                         as contract_daily_interest_rate_basis,
    is_empty(map_from_str(extra_info)['利率类型'],interest_rate_type)                            as interest_rate_type,
    is_empty(loan_interest_rate,is_empty(map_from_str(extra_info)['贷款年利率(%)'],0)) / 100     as loan_init_interest_rate,
    is_empty(map_from_str(extra_info)['手续费利率'],0) / 100                                     as loan_init_term_fee_rate,
    0                                                                                            as loan_init_svc_fee_rate,
    is_empty(map_from_str(extra_info)['贷款罚息利率(%)'],is_empty(loan_penalty_rate,0)) / 100    as loan_init_penalty_rate,
    is_empty(map_from_str(extra_info)['尾付款金额'])                                             as tail_amount,
    is_empty(map_from_str(extra_info)['尾付比例'])                                               as tail_amount_rate,
    is_empty(map_from_str(extra_info)['产品编号'])                                               as bus_product_id,
    is_empty(map_from_str(extra_info)['产品方案名称'])                                           as bus_product_name,
    is_empty(map_from_str(extra_info)['抵押率(%)'])                                              as mortgage_rate,
    is_empty(map_from_str(extra_info)['项目编号'],project_id)                                    as product_id
  from stage.asset_01_t_loan_contract_info
  where 1 > 0
    and is_empty(map_from_str(extra_info)['项目编号'],project_id) not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
) as tmp
-- limit 10
;
