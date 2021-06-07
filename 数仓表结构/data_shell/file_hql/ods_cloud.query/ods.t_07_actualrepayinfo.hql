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




insert overwrite table ods.t_07_actualrepayinfo partition(project_id)
select
  is_empty(t1.asset_id,map_from_str(t1.extra_info)['借据号'])                   as serial_number,
  is_empty(t1.period,map_from_str(t1.extra_info)['期次'])                       as term,
  is_empty(map_from_str(t1.extra_info)['是否借款人本人还款'])                   as is_borrowers_oneself_repayment,
  is_empty(t1.rel_pay_date,map_from_str(t1.extra_info)['实际还清日期'])         as actual_repay_time,
  is_empty(t1.current_loan_balance,map_from_str(t1.extra_info)['当期贷款余额']) as current_period_loan_balance,
  is_empty(t1.repay_type,map_from_str(t1.extra_info)['还款类型'])               as repay_type,
  is_empty(t1.account_status,map_from_str(t1.extra_info)['当期账户状态'])       as current_account_status,
  is_empty(t1.rel_principal,map_from_str(t1.extra_info)['实还本金(元)'],0)      as actual_repay_principal,
  is_empty(t1.rel_interest,map_from_str(t1.extra_info)['实还利息(元)'],0)       as actual_repay_interest,
  is_empty(t1.rel_fee,map_from_str(t1.extra_info)['实还费用(元)'],0)            as actual_repay_fee,
  is_empty(map_from_str(t1.extra_info)['违约金'],0)                             as penalbond,
  is_empty(t1.rel_penalty,map_from_str(t1.extra_info)['罚息'],0)                as penalty_interest,
  is_empty(map_from_str(t1.extra_info)['赔偿金'],0)                             as compensation,
  is_empty(map_from_str(t1.extra_info)['提前还款手续费'],0)                     as advanced_commission_charge,
  is_empty(map_from_str(t1.extra_info)['其它相关费用'],0)                       as other_fee,
  is_empty(map_from_str(t1.extra_info)['实还执行利率'],0)                       as actual_work_interest_rate,
  t3.data_source                                                                as data_source,
  t2.import_id                                                                  as import_id,
  t1.create_time                                                                as create_time,
  t1.update_time                                                                as update_time,
  t2.project_id                                                                 as project_id
from stage.asset_07_t_repayment_info as t1
join dim.project_due_bill_no as t2
on  case is_empty(t1.project_id,map_from_str(t1.extra_info)['项目编号']) when 'Cl00333' then 'cl00333' else is_empty(t1.project_id,map_from_str(t1.extra_info)['项目编号']) end  = nvl(t2.related_project_id,t2.project_id)
and t1.asset_id = t2.due_bill_no
join dim.project_info as t3
on t2.project_id = t3.project_id;
