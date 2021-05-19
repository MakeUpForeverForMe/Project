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




insert overwrite table ods.repay_detail partition(biz_date,product_id)
select distinct
  due_bill_no                 as due_bill_no,
  repay_term                  as repay_term,
  null                        as order_id,
  case repay_type_cn
    when '正常还款'     then 'NORMAL'
    when '提前还款'     then 'PRE'
    when '代偿还款'     then 'COMP'
    when '逾期追偿还款' then 'RECOVER'
    when '用户提前结清' then 'PRE_SETTLE'
    when '退票'         then 'REFUND'
    when '回购'         then 'BUYBACK'
    when '减免'         then 'REDUCE'
    when '商户贴息'     then 'DISCOUNT'
    when '降额退息'     then 'INTEREST_REBATE'
    when '退款退息'     then 'INTERNAL_REFUND'
    when '逾期还款'     then 'OVERDUE'
    when '退款回款'     then 'RECEIVABLE'
    when '降额还本'     then 'RECOVERY_REPAYMENT'
    else repay_type_cn
  end                         as repay_type,
  repay_type_cn               as repay_type_cn,
  null                        as payment_id,
  cast(txn_date as timestamp) as txn_time,
  null                        as post_time,
  bnp_type                    as bnp_type,
  case bnp_type
    when 'Pricinpal'         then '本金'
    when 'Interest'          then '利息'
    when 'Penalty'           then '罚息'
    when 'Mulct'             then '罚金'
    when 'Compensation'      then '赔偿金'
    when 'Damages'           then '违约金'
    when 'Compound'          then '复利'
    when 'CardFee'           then '年费'
    when 'OverLimitFee'      then '超限费'
    when 'LatePaymentCharge' then '滞纳金'
    when 'NSFCharge'         then '资金不足罚金'
    when 'TXNFee'            then '交易费'
    when 'TERMFee'           then '手续费'
    when 'SVCFee'            then '服务费'
    when 'LifeInsuFee'       then '寿险计划包费'
    when 'RepayFee'          then '实还费用'
    when 'EarlyRepayFee'     then '提前还款手续费'
    when 'OtherFee'          then '其它相关费用'
    else bnp_type
  end                         as bnp_type_cn,
  repay_amount                as repay_amount,
  null                        as batch_date,
  create_time                 as create_time,
  update_time                 as update_time,
  txn_date                    as biz_date,
  project_id                  as product_id
from (
  select distinct
    is_empty(asset_id,     map_from_str(extra_info)['借据号'])         as due_bill_no,    -- string
    is_empty(period,       map_from_str(extra_info)['期次'])           as repay_term,     -- int
    is_empty(repay_type,   map_from_str(extra_info)['还款类型'])       as repay_type_cn,  -- string
    is_empty(overdue_day,  map_from_str(extra_info)['逾期天数'])       as overdue_days,   -- int
    is_empty(rel_pay_date, map_from_str(extra_info)['实际还清日期'])   as txn_date,       -- string
    is_empty(rel_principal,map_from_str(extra_info)['实还本金(元)'],0) as pricinpal,      -- decimal(16,2)
    is_empty(rel_interest, map_from_str(extra_info)['实还利息(元)'],0) as interest,       -- decimal(16,2)
    is_empty(rel_penalty,  map_from_str(extra_info)['罚息'],0)         as penalty,        -- decimal(16,2)
    is_empty(rel_fee,      map_from_str(extra_info)['实还费用(元)'],0) as repayfee,       -- decimal(16,2)
    is_empty(map_from_str(extra_info)['赔偿金'],0)                     as compensation,
    is_empty(map_from_str(extra_info)['违约金'],0)                     as damages,
    is_empty(map_from_str(extra_info)['提前还款手续费'],0)             as earlyrepayfee,
    is_empty(map_from_str(extra_info)['其它相关费用'],0)               as otherfee,
    create_time                                                        as create_time,
    update_time                                                        as update_time,
    case is_empty(project_id,map_from_str(extra_info)['项目编号'])
      when 'Cl00333' then 'cl00333'
      else is_empty(project_id,map_from_str(extra_info)['项目编号'])
    end                                                                as project_id
  from stage.asset_07_t_repayment_info
  where 1 > 0
    and is_empty(project_id,   map_from_str(extra_info)['项目编号']) not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
    and is_empty(map_from_str(extra_info)['实际还清日期'],rel_pay_date) = '${ST9}'
    -- and is_empty(map_from_str(extra_info)['实际还清日期'],rel_pay_date) like '2021%'
    -- and is_empty(map_from_str(extra_info)['借据号'],asset_id) = '1000000407'
  -- order by due_bill_no,repay_term
) as repay_detail
lateral view explode(map(
  'Pricinpal',    pricinpal,
  'Interest',     interest,
  'Penalty',      penalty,
  'RepayFee',     repayfee,
  'Compensation', compensation,
  'Damages',      damages,
  'EarlyRepayFee',earlyrepayfee,
  'OtherFee',     otherfee
)) tbl_map as bnp_type,repay_amount
where 1 > 0
  and repay_amount != 0 -- 过滤掉无用的数据
-- order by due_bill_no,repay_term,bnp_type
-- limit 50
;
