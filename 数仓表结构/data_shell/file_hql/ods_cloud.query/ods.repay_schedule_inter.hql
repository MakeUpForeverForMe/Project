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



-- ------------------------------------------------------- 执行一次的 Hql 截止 11 月 30 号 -------------------------------------------------------
-- insert overwrite table ods.repay_schedule_inter partition(biz_date,product_id)
-- select
--   abs_05.due_bill_no            as due_bill_no,
--   abs_01.loan_active_date       as loan_active_date,
--   abs_01.loan_init_principal    as loan_init_principal,
--   abs_01.loan_init_term         as loan_init_term,
--   abs_05.loan_term              as loan_term,
--   null                          as start_interest_date,
--   0                             as curr_bal,
--   abs_05.should_repay_date      as should_repay_date,
--   null                          as should_repay_date_history,
--   null                          as grace_date,
--   (abs_05.should_repay_principal + abs_05.should_repay_interest + abs_05.should_repay_term_fee) as should_repay_amount,
--   abs_05.should_repay_principal as should_repay_principal,
--   abs_05.should_repay_interest  as should_repay_interest,
--   abs_05.should_repay_term_fee  as should_repay_term_fee,
--   0                             as should_repay_svc_fee,
--   0                             as should_repay_penalty,
--   0                             as should_repay_mult_amt,
--   0                             as should_repay_penalty_acru,
--   null                          as schedule_status,
--   null                          as schedule_status_cn,
--   abs_05.repay_status           as repay_status,
--   null                          as paid_out_date,
--   null                          as paid_out_type,
--   null                          as paid_out_type_cn,
--   0                             as paid_amount,
--   0                             as paid_principal,
--   0                             as paid_interest,
--   0                             as paid_term_fee,
--   0                             as paid_svc_fee,
--   0                             as paid_penalty,
--   0                             as paid_mult,
--   0                             as reduce_amount,
--   0                             as reduce_principal,
--   0                             as reduce_interest,
--   0                             as reduce_term_fee,
--   0                             as reduce_svc_fee,
--   0                             as reduce_penalty,
--   0                             as reduce_mult_amt,
--   abs_05.effective_date         as effective_date,
--   abs_05.create_time            as create_time,
--   abs_05.update_time            as update_time,
--   abs_05.effective_date         as biz_date,
--   abs_05.project_id             as product_id
-- from (
--   select distinct
--     serial_number          as due_bill_no,
--     period                 as loan_term,
--     repay_status           as repay_status,
--     should_repay_date      as should_repay_date,
--     should_repay_principal as should_repay_principal,
--     should_repay_interest  as should_repay_interest,
--     should_repay_cost      as should_repay_term_fee,
--     effective_date         as effective_date,
--     create_time            as create_time,
--     update_time            as update_time,
--     case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id
--   from stage.abs_05_t_repaymentplan_history
--   where 1 > 0
--     and effective_date <= '2020-11-30'
--     and project_id not in (
--       -- 基础资产不需要的项目
--       'CL202010180087',
--       'pl10999',

--       -- 采用校验平台的数据
--       '001601',           -- 汇通
--       'WS0005200001',     -- 瓜子
--       -- 'CL202012280092',   -- 汇通国银
--       'DIDI201908161538', -- 滴滴

--       -- 过滤掉债转的项目
--       'CL202003230083',
--       'CL202007020086',
--       'CL202011090088',
--       'CL202011090089',
--       'CL202011090090',
--       'CL202012160091',
--       'CL202101220094',
--       'CL202102010097',
--       'CL202102050098',
--       'CL202102240099',
--       'CL202102240100',
--       'CL202103160101',
--       'CL202103260102',
--       'CL202104010103',
--       'CL202104010103',
--       'CL202104010103',
--       'CL202104080105',
--       'CL202104160106',
--       ''
--     )
-- ) as abs_05
-- left join (
--   select distinct
--     case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
--     serial_number                              as serial_number,
--     is_empty(actual_loan_date,loan_issue_date) as loan_active_date,
--     periods                                    as loan_init_term,
--     contract_amount                            as loan_init_principal
--   from stage.abs_01_t_loancontractinfo
-- ) as abs_01
-- on  abs_05.project_id  = abs_01.project_id
-- and abs_05.due_bill_no = abs_01.serial_number
-- -- limit 10
-- ;



-- ------------------------------------------------------- 执行一次的 Hql 执行 12 月 01 号 -------------------------------------------------------
-- insert overwrite table ods.repay_schedule_inter partition(biz_date,product_id)
-- select
--   asset_05.due_bill_no            as due_bill_no,
--   asset_01.loan_active_date       as loan_active_date,
--   asset_01.loan_init_principal    as loan_init_principal,
--   asset_01.loan_init_term         as loan_init_term,
--   asset_05.loan_term              as loan_term,
--   null                            as start_interest_date,
--   0                               as curr_bal,
--   asset_05.should_repay_date      as should_repay_date,
--   null                            as should_repay_date_history,
--   null                            as grace_date,
--   (asset_05.should_repay_principal + asset_05.should_repay_interest + asset_05.should_repay_term_fee) as should_repay_amount,
--   asset_05.should_repay_principal as should_repay_principal,
--   asset_05.should_repay_interest  as should_repay_interest,
--   asset_05.should_repay_term_fee  as should_repay_term_fee,
--   0                               as should_repay_svc_fee,
--   asset_05.should_repay_penalty   as should_repay_penalty,
--   0                               as should_repay_mult_amt,
--   0                               as should_repay_penalty_acru,
--   null                            as schedule_status,
--   null                            as schedule_status_cn,
--   null                            as repay_status,
--   null                            as paid_out_date,
--   null                            as paid_out_type,
--   null                            as paid_out_type_cn,
--   0                               as paid_amount,
--   0                               as paid_principal,
--   0                               as paid_interest,
--   0                               as paid_term_fee,
--   0                               as paid_svc_fee,
--   0                               as paid_penalty,
--   0                               as paid_mult,
--   0                               as reduce_amount,
--   0                               as reduce_principal,
--   0                               as reduce_interest,
--   0                               as reduce_term_fee,
--   0                               as reduce_svc_fee,
--   0                               as reduce_penalty,
--   0                               as reduce_mult_amt,
--   asset_05.effective_date         as effective_date,
--   asset_05.create_time            as create_time,
--   asset_05.update_time            as update_time,
--   '${ST9}'                        as biz_date,
--   asset_05.project_id             as product_id
-- from (
--   select distinct
--     case is_empty(map_from_str(extra_info)['项目编号'],project_id)
--       when 'Cl00333' then 'cl00333'
--       else is_empty(map_from_str(extra_info)['项目编号'],project_id)
--     end                                                                as project_id,
--     is_empty(map_from_str(extra_info)['借据号'],asset_id)              as due_bill_no,
--     is_empty(map_from_str(extra_info)['期次'],period)                  as loan_term,
--     is_empty(map_from_str(extra_info)['应还款日'],repay_date)          as should_repay_date,
--     is_empty(map_from_str(extra_info)['应还本金(元)'],repay_principal) as should_repay_principal,
--     is_empty(map_from_str(extra_info)['应还利息(元)'],repay_interest)  as should_repay_interest,
--     is_empty(map_from_str(extra_info)['应还费用(元)'],repay_fee)       as should_repay_term_fee,
--     is_empty(map_from_str(extra_info)['应还罚息(元)'],repay_penalty)   as should_repay_penalty,
--     is_empty(map_from_str(extra_info)['生效日期'],execute_date)        as effective_date,
--     create_time                                                        as create_time,
--     update_time                                                        as update_time
--   from stage.asset_05_t_repayment_schedule
--   where 1 > 0
--     and d_date = '2020-12-01'
--     and is_empty(map_from_str(extra_info)['项目编号'],project_id) not in (
--       '001601',           -- 汇通
--       'WS0005200001',     -- 瓜子
--       'CL202012280092',   -- 汇通国银
--       'DIDI201908161538', -- 滴滴
--       ''
--     )
-- ) as asset_05
-- left join (
--   select distinct
--     serial_number          as due_bill_no,
--     period                 as loan_term,
--     repay_status           as repay_status,
--     should_repay_date      as should_repay_date,
--     should_repay_principal as should_repay_principal,
--     should_repay_interest  as should_repay_interest,
--     should_repay_cost      as should_repay_term_fee,
--     effective_date         as effective_date,
--     case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id
--   from stage.abs_05_t_repaymentplan_history
--   where 1 > 0
--     and effective_date <= '2020-11-30'
--     and project_id not in (
--       '001601',           -- 汇通
--       'WS0005200001',     -- 瓜子
--       -- 'CL202012280092',   -- 汇通国银
--       'DIDI201908161538', -- 滴滴
--       ''
--     )
-- ) as abs_05
-- on  asset_05.project_id             = abs_05.project_id
-- and asset_05.due_bill_no            = abs_05.due_bill_no
-- and asset_05.loan_term              = abs_05.loan_term
-- and asset_05.should_repay_date      = abs_05.should_repay_date
-- and asset_05.should_repay_principal = abs_05.should_repay_principal
-- and asset_05.should_repay_interest  = abs_05.should_repay_interest
-- and asset_05.should_repay_term_fee  = abs_05.should_repay_term_fee
-- and asset_05.effective_date         = abs_05.effective_date
-- left join (
--   select distinct
--     case is_empty(map_from_str(extra_info)['项目编号'],project_id)
--       when 'Cl00333' then 'cl00333'
--       else is_empty(map_from_str(extra_info)['项目编号'],project_id)
--     end                                                                                as project_id,
--     is_empty(map_from_str(extra_info)['借据号'],asset_id)                              as due_bill_no,
--     is_empty(
--       case when length(map_from_str(extra_info)['实际放款时间']) = 19 then to_date(map_from_str(extra_info)['实际放款时间'])
--       else is_empty(datefmt(map_from_str(extra_info)['实际放款时间'],'','yyyy-MM-dd')) end,
--       case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
--       else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end
--     )                                                                                  as loan_active_date,
--     is_empty(map_from_str(extra_info)['总期数'],periods)                               as loan_init_term,
--     is_empty(map_from_str(extra_info)['贷款总金额(元)'],is_empty(loan_total_amount,0)) as loan_init_principal
--   from stage.asset_01_t_loan_contract_info
-- ) as asset_01
-- on  asset_05.project_id  = asset_01.project_id
-- and asset_05.due_bill_no = asset_01.due_bill_no
-- where 1 > 0
--   and abs_05.project_id  is null
--   and abs_05.due_bill_no is null
--   and abs_05.loan_term   is null
-- -- limit 10
-- ;





------------------------------------------------------- 连续执行的 Hql 执行 12 月 02 号 开始 -------------------------------------------------------
insert overwrite table ods.repay_schedule_inter partition(biz_date,product_id)
select
  asset_05_today.due_bill_no            as due_bill_no,
  asset_01.loan_active_date             as loan_active_date,
  asset_01.loan_init_principal          as loan_init_principal,
  asset_01.loan_init_term               as loan_init_term,
  asset_05_today.loan_term              as loan_term,
  null                                  as start_interest_date,
  0                                     as curr_bal,
  asset_05_today.should_repay_date      as should_repay_date,
  null                                  as should_repay_date_history,
  null                                  as grace_date,
  (asset_05_today.should_repay_principal + asset_05_today.should_repay_interest + asset_05_today.should_repay_term_fee) as should_repay_amount,
  asset_05_today.should_repay_principal as should_repay_principal,
  asset_05_today.should_repay_interest  as should_repay_interest,
  asset_05_today.should_repay_term_fee  as should_repay_term_fee,
  0                                     as should_repay_svc_fee,
  asset_05_today.should_repay_penalty   as should_repay_penalty,
  0                                     as should_repay_mult_amt,
  0                                     as should_repay_penalty_acru,
  null                                  as schedule_status,
  null                                  as schedule_status_cn,
  null                                  as repay_status,
  null                                  as paid_out_date,
  null                                  as paid_out_type,
  null                                  as paid_out_type_cn,
  0                                     as paid_amount,
  0                                     as paid_principal,
  0                                     as paid_interest,
  0                                     as paid_term_fee,
  0                                     as paid_svc_fee,
  0                                     as paid_penalty,
  0                                     as paid_mult,
  0                                     as reduce_amount,
  0                                     as reduce_principal,
  0                                     as reduce_interest,
  0                                     as reduce_term_fee,
  0                                     as reduce_svc_fee,
  0                                     as reduce_penalty,
  0                                     as reduce_mult_amt,
  asset_05_today.effective_date         as effective_date,
  asset_05_today.create_time            as create_time,
  asset_05_today.update_time            as update_time,
  '${ST9}'                              as biz_date,
  asset_05_today.project_id             as product_id
from (
  select distinct
    case is_empty(map_from_str(extra_info)['项目编号'],project_id)
      when 'Cl00333' then 'cl00333'
      else is_empty(map_from_str(extra_info)['项目编号'],project_id)
    end                                                                as project_id,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)              as due_bill_no,
    is_empty(map_from_str(extra_info)['期次'],period)                  as loan_term,
    is_empty(map_from_str(extra_info)['应还款日'],repay_date)          as should_repay_date,
    is_empty(map_from_str(extra_info)['应还本金(元)'],repay_principal) as should_repay_principal,
    is_empty(map_from_str(extra_info)['应还利息(元)'],repay_interest)  as should_repay_interest,
    is_empty(map_from_str(extra_info)['应还费用(元)'],repay_fee)       as should_repay_term_fee,
    is_empty(map_from_str(extra_info)['应还罚息(元)'],repay_penalty)   as should_repay_penalty,
    is_empty(map_from_str(extra_info)['生效日期'],execute_date)        as effective_date,
    create_time                                                        as create_time,
    update_time                                                        as update_time
  from stage.asset_05_t_repayment_schedule
  where 1 > 0
    and '${ST9}' >= '2020-12-02'
    and d_date = '${ST9}'
    and is_empty(map_from_str(extra_info)['项目编号'],project_id) not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
) as asset_05_today
left join (
  select distinct
    case is_empty(map_from_str(extra_info)['项目编号'],project_id)
      when 'Cl00333' then 'cl00333'
      else is_empty(map_from_str(extra_info)['项目编号'],project_id)
    end                                                                as project_id,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)              as due_bill_no,
    is_empty(map_from_str(extra_info)['期次'],period)                  as loan_term,
    is_empty(map_from_str(extra_info)['应还款日'],repay_date)          as should_repay_date,
    is_empty(map_from_str(extra_info)['应还本金(元)'],repay_principal) as should_repay_principal,
    is_empty(map_from_str(extra_info)['应还利息(元)'],repay_interest)  as should_repay_interest,
    is_empty(map_from_str(extra_info)['应还费用(元)'],repay_fee)       as should_repay_term_fee,
    is_empty(map_from_str(extra_info)['应还罚息(元)'],repay_penalty)   as should_repay_penalty,
    is_empty(map_from_str(extra_info)['生效日期'],execute_date)        as effective_date
  from stage.asset_05_t_repayment_schedule
  where 1 > 0
    and date_sub('${ST9}',1) >= date_sub('2020-12-02',1)
    and d_date = date_sub('${ST9}',1)
    and is_empty(map_from_str(extra_info)['项目编号'],project_id) not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
) as asset_05_yesterday
on  is_empty(asset_05_today.project_id,            'aa') = is_empty(asset_05_yesterday.project_id,            'aa')
and is_empty(asset_05_today.due_bill_no,           'aa') = is_empty(asset_05_yesterday.due_bill_no,           'aa')
and is_empty(asset_05_today.loan_term,             'aa') = is_empty(asset_05_yesterday.loan_term,             'aa')
and is_empty(asset_05_today.should_repay_date,     'aa') = is_empty(asset_05_yesterday.should_repay_date,     'aa')
and is_empty(asset_05_today.should_repay_principal,   0) = is_empty(asset_05_yesterday.should_repay_principal,   0)
and is_empty(asset_05_today.should_repay_interest,    0) = is_empty(asset_05_yesterday.should_repay_interest,    0)
and is_empty(asset_05_today.should_repay_term_fee,    0) = is_empty(asset_05_yesterday.should_repay_term_fee,    0)
and is_empty(asset_05_today.should_repay_penalty,     0) = is_empty(asset_05_yesterday.should_repay_penalty,     0)
and is_empty(asset_05_today.effective_date,        'aa') = is_empty(asset_05_yesterday.effective_date,        'aa')
left join (
  select distinct
    case is_empty(map_from_str(extra_info)['项目编号'],project_id)
      when 'Cl00333' then 'cl00333'
      else is_empty(map_from_str(extra_info)['项目编号'],project_id)
    end                                                                                as project_id,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                              as due_bill_no,
    is_empty(
      case when length(map_from_str(extra_info)['实际放款时间']) = 19 then to_date(map_from_str(extra_info)['实际放款时间'])
      else is_empty(datefmt(map_from_str(extra_info)['实际放款时间'],'','yyyy-MM-dd')) end,
      case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
      else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end
    )                                                                                  as loan_active_date,
    is_empty(map_from_str(extra_info)['总期数'],periods)                               as loan_init_term,
    is_empty(map_from_str(extra_info)['贷款总金额(元)'],is_empty(loan_total_amount,0)) as loan_init_principal
  from stage.asset_01_t_loan_contract_info
) as asset_01
on  asset_05_today.project_id  = asset_01.project_id
and asset_05_today.due_bill_no = asset_01.due_bill_no
where 1 > 0
  and asset_05_yesterday.project_id  is null
  and asset_05_yesterday.due_bill_no is null
  and asset_05_yesterday.loan_term   is null
-- order by biz_date,product_id,due_bill_no,loan_term,effective_date
-- limit 10
;
