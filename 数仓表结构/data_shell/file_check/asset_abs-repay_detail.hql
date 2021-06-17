select
  stage.project_id     as project_id_stage,    ods.project_id     as project_id_ods,    -- if(nvl(stage.project_id,'a')     != nvl(ods.project_id,'a'),    1,0) as project_id,
  count(stage.due_bill_no)          as due_bill_no_stage,    -- count(ods.due_bill_no)          as due_bill_no_ods,
  count(distinct stage.due_bill_no) as due_bill_no_stage_dis,-- count(distinct ods.due_bill_no) as due_bill_no_ods_dis,
  -- stage.due_bill_no    as due_bill_no_stage,   ods.due_bill_no    as due_bill_no_ods,   if(nvl(stage.due_bill_no,'a')    != nvl(ods.due_bill_no,'a'),   1,0) as due_bill_no,
  -- stage.repay_term     as repay_term_stage,    ods.repay_term     as repay_term_ods,    if(nvl(stage.repay_term,'a')     != nvl(ods.repay_term,'a'),    1,0) as repay_term,
  -- stage.overdue_days   as overdue_days_stage,  ods.overdue_days   as overdue_days_ods,  if(nvl(stage.overdue_days,'a')   != nvl(ods.overdue_days,'a'),  1,0) as overdue_days,
  -- stage.loan_status_cn as loan_status_cn_stage,ods.loan_status_cn as loan_status_cn_ods,if(nvl(stage.loan_status_cn,'a') != nvl(ods.loan_status_cn,'a'),1,0) as loan_status_cn,
  -- stage.txn_date       as txn_date_stage,      ods.txn_date       as txn_date_ods,      if(nvl(stage.txn_date,'a')       != nvl(ods.txn_date,'a'),      1,0) as txn_date,
  -- stage.pricinpal      as pricinpal_stage,     ods.pricinpal      as pricinpal_ods,     if(nvl(stage.pricinpal,'a')      != nvl(ods.pricinpal,'a'),     1,0) as pricinpal,
  -- stage.interest       as interest_stage,      ods.interest       as interest_ods,      if(nvl(stage.interest,'a')       != nvl(ods.interest,'a'),      1,0) as interest,
  -- stage.penalty        as penalty_stage,       ods.penalty        as penalty_ods,       if(nvl(stage.penalty,'a')        != nvl(ods.penalty,'a'),       1,0) as penalty,
  -- stage.txnfee         as txnfee_stage,        ods.txnfee         as txnfee_ods,        if(nvl(stage.txnfee,'a')         != nvl(ods.txnfee,'a'),        1,0) as txnfee,
  -- stage.compensation   as compensation_stage,  ods.compensation   as compensation_ods,  if(nvl(stage.compensation,'a')   != nvl(ods.compensation,'a'),  1,0) as compensation,
  -- stage.damages        as damages_stage,       ods.damages        as damages_ods,       if(nvl(stage.damages,'a')        != nvl(ods.damages,'a'),       1,0) as damages,
  -- stage.termfee        as termfee_stage,       ods.termfee        as termfee_ods,       if(nvl(stage.termfee,'a')        != nvl(ods.termfee,'a'),       1,0) as termfee,
  -- stage.svcfee         as svcfee_stage,        ods.svcfee         as svcfee_ods,        if(nvl(stage.svcfee,'a')         != nvl(ods.svcfee,'a'),        1,0) as svcfee,
  -- stage.create_time    as create_time_stage,   ods.create_time    as create_time_ods,   if(nvl(stage.create_time,'a')    != nvl(ods.create_time,'a'),   1,0) as create_time,
  -- stage.update_time    as update_time_stage,   ods.update_time    as update_time_ods,   if(nvl(stage.update_time,'a')    != nvl(ods.update_time,'a'),   1,0) as update_time,
  '' as aa
from (
  select
    is_empty(serial_number)                                        as due_bill_no,
    cast(is_empty(term,0) as int)                                  as repay_term,
    is_empty(repay_type)                                           as loan_status_cn,
    is_empty(null)                                                 as overdue_days,
    is_empty(actual_repay_time)                                    as txn_date,
    cast(is_empty(actual_repay_principal,0) as decimal(25,5))      as pricinpal,
    cast(is_empty(actual_repay_interest,0) as decimal(30,10))      as interest,
    cast(is_empty(actual_repay_fee,0) as decimal(30,10))           as txnfee,
    cast(is_empty(penalty_interest,0) as decimal(30,10))           as penalty,
    cast(is_empty(compensation,0) as decimal(30,10))               as compensation,
    cast(is_empty(penalbond,0) as decimal(30,10))                  as damages,
    cast(is_empty(advanced_commission_charge,0) as decimal(30,10)) as termfee,
    cast(is_empty(other_fee,0) as decimal(30,10))                  as svcfee,
    is_empty(create_time)                                          as create_time,
    is_empty(update_time)                                          as update_time,
    is_empty(project_id)                                           as project_id,
    '' as aa
  from stage.t_07_actualrepayinfo
  where 1 > 0
    and project_id in (select distinct is_empty(map_from_str(extra_info)['项目编号'],project_id) from ods.t_repayment_info)
    -- and project_id not in (
    --   '001601',
    --   'DIDI201908161538',
    --   'CL202101260095',
    --   'CL202102050098',
    --   'CL202011090089',
    --   'CL202007020086',
    --   'CL202011090088',
    --   'CL202012160091',
    --   'CL202003230083',
    --   'CL202011090090',
    --   'CL202101220094',
    --   ''
    -- )
) as stage
left join (
  select distinct
             is_empty(asset_id,     map_from_str(extra_info)['借据号'])                             as due_bill_no,    -- string
        cast(is_empty(period,       map_from_str(extra_info)['期次'])         as int)               as repay_term,     -- int
             is_empty(repay_type,   map_from_str(extra_info)['还款类型'])                           as loan_status_cn, -- string
             is_empty(overdue_day,  map_from_str(extra_info)['逾期天数'])                           as overdue_days,   -- int
             is_empty(rel_pay_date, map_from_str(extra_info)['实际还清日期'])                       as txn_date,       -- string
    nvl(cast(is_empty(rel_principal,map_from_str(extra_info)['实还本金(元)']) as decimal(25,5)), 0) as pricinpal,      -- decimal(16,2)
    nvl(cast(is_empty(rel_interest, map_from_str(extra_info)['实还利息(元)']) as decimal(30,10)),0) as interest,       -- decimal(16,2)
    nvl(cast(is_empty(rel_fee,      map_from_str(extra_info)['实还费用(元)']) as decimal(30,10)),0) as txnfee,         -- decimal(16,2)
    nvl(cast(is_empty(rel_penalty,  map_from_str(extra_info)['罚息'])         as decimal(30,10)),0) as penalty,        -- decimal(16,2)
        cast(is_empty(map_from_str(extra_info)['赔偿金'],0)                   as decimal(30,10))    as compensation,
        cast(is_empty(map_from_str(extra_info)['违约金'],0)                   as decimal(30,10))    as damages,
        cast(is_empty(map_from_str(extra_info)['提前还款手续费'],0)           as decimal(30,10))    as termfee,
        cast(is_empty(map_from_str(extra_info)['其它相关费用'],0)             as decimal(30,10))    as svcfee,
             is_empty(create_time)                                                                  as create_time,    -- string
             is_empty(update_time)                                                                  as update_time,    -- string
             is_empty(project_id,   map_from_str(extra_info)['项目编号'])                           as project_id,     -- string



    --          is_empty(map_from_str(extra_info)['借据号'],      asset_id)                            as due_bill_no,    -- string
    --     cast(is_empty(map_from_str(extra_info)['期次'],        period)        as int)               as repay_term,     -- int
    --          is_empty(map_from_str(extra_info)['还款类型'],    repay_type)                          as loan_status_cn, -- string
    --          is_empty(map_from_str(extra_info)['逾期天数'],    overdue_day)                         as overdue_days,   -- int
    --          is_empty(map_from_str(extra_info)['实际还清日期'],rel_pay_date)                        as txn_date,       -- string
    -- nvl(cast(is_empty(map_from_str(extra_info)['实还本金(元)'],rel_principal) as decimal(25,5)), 0) as pricinpal,      -- decimal(16,2)
    -- nvl(cast(is_empty(map_from_str(extra_info)['实还利息(元)'],rel_interest)  as decimal(30,10)),0) as interest,       -- decimal(16,2)
    -- nvl(cast(is_empty(map_from_str(extra_info)['实还费用(元)'],rel_fee)       as decimal(30,10)),0) as txnfee,         -- decimal(16,2)
    -- nvl(cast(is_empty(map_from_str(extra_info)['罚息'],        rel_penalty)   as decimal(30,10)),0) as penalty,        -- decimal(16,2)
    --     cast(is_empty(map_from_str(extra_info)['赔偿金'],0)                   as decimal(30,10))    as compensation,
    --     cast(is_empty(map_from_str(extra_info)['违约金'],0)                   as decimal(30,10))    as damages,
    --     cast(is_empty(map_from_str(extra_info)['提前还款手续费'],0)           as decimal(30,10))    as termfee,
    --     cast(is_empty(map_from_str(extra_info)['其它相关费用'],0)             as decimal(30,10))    as svcfee,
    --          is_empty(create_time)                                                                  as create_time,    -- string
    --          is_empty(update_time)                                                                  as update_time,    -- string
    --          is_empty(map_from_str(extra_info)['项目编号'],    project_id)                          as project_id,     -- string


    '' as aa
  from ods.t_repayment_info
) as ods
on  stage.project_id  = ods.project_id
and stage.due_bill_no = ods.due_bill_no
and stage.repay_term  = ods.repay_term
and stage.txn_date    = ods.txn_date
where 1 > 0
  -- and nvl(stage.txn_date,'a') = nvl(ods.txn_date,'a')
  and (
    -- nvl(stage.project_id,'a')     != nvl(ods.project_id,'a')     or
    -- nvl(stage.due_bill_no,'a')    != nvl(ods.due_bill_no,'a')    or
    -- nvl(stage.repay_term,'a')     != nvl(ods.repay_term,'a')     or
    -- nvl(stage.loan_status_cn,'a') != nvl(ods.loan_status_cn,'a') or
    -- nvl(stage.txn_date,'a')       != nvl(ods.txn_date,'a')       or
    -- nvl(stage.pricinpal,'a')      != nvl(ods.pricinpal,'a')      or
    -- nvl(stage.interest,'a')       != nvl(ods.interest,'a')       or
    -- nvl(stage.txnfee,'a')         != nvl(ods.txnfee,'a')         or -- 费用
    -- nvl(stage.penalty,'a')        != nvl(ods.penalty,'a')        or -- 罚息
    -- nvl(stage.compensation,'a')   != nvl(ods.compensation,'a')   or -- 赔偿金
    -- nvl(stage.damages,'a')        != nvl(ods.damages,'a')        or -- 违约金
    -- nvl(stage.termfee,'a')        != nvl(ods.termfee,'a')        or -- 提前还款手续费
    nvl(stage.svcfee,'a')         != nvl(ods.svcfee,'a')         or -- 其它相关费用
    false
  )
group by
stage.project_id,ods.project_id,
-- stage.txn_date,  ods.txn_date,
''
order by
project_id_stage,
-- due_bill_no_stage,
-- repay_term_stage,
''
limit 50
;





select * from stage.t_07_actualrepayinfo
where 1 > 0
  and serial_number = 'GALC-HL-1606300219'
  and term = 6
;


select distinct
  is_empty(project_id)                                                                           as project_id_col,
  is_empty(map_from_str(extra_info)['借据号'],asset_id)                                          as due_bill_no,
  is_empty(asset_id)                                                                             as asset_id_col,
  cast(is_empty(map_from_str(extra_info)['期次'],period) as int)                                 as repay_term,
  cast(period as int)                                                                            as period_col,
  is_empty(map_from_str(extra_info)['还款类型'],repay_type)                                      as loan_status_cn,
  is_empty(repay_type)                                                                           as repay_type_col,
  is_empty(map_from_str(extra_info)['逾期天数'],overdue_day)                                     as overdue_days,
  is_empty(overdue_day)                                                                          as overdue_day_col,
  is_empty(map_from_str(extra_info)['实际还清日期'],rel_pay_date)                                as txn_date,
  is_empty(rel_pay_date)                                                                         as rel_pay_date_col,
  nvl(cast(is_empty(map_from_str(extra_info)['实还本金(元)'],rel_principal) as decimal(25,5)),0) as pricinpal,
  nvl(cast(rel_principal as decimal(25,5)),0)                                                    as rel_principal_col,
  nvl(cast(is_empty(map_from_str(extra_info)['实还利息(元)'],rel_interest) as decimal(30,10)),0) as interest,
  nvl(cast(rel_interest as decimal(30,10)),0)                                                    as rel_interest_col,
  nvl(cast(is_empty(map_from_str(extra_info)['罚息'],rel_penalty) as decimal(30,10)),0)          as penalty,
  nvl(cast(rel_penalty as decimal(30,10)),0)                                                     as rel_penalty_col,
  nvl(cast(is_empty(map_from_str(extra_info)['实还费用(元)'],rel_fee) as decimal(30,10)),0)      as txnfee,
  nvl(cast(rel_fee as decimal(30,10)),0)                                                         as rel_fee_col,
  nvl(cast(is_empty(map_from_str(extra_info)['赔偿金'],0) as decimal(30,10)),0)                  as compensation,
  nvl(cast(is_empty(map_from_str(extra_info)['违约金'],0) as decimal(30,10)),0)                  as damages,
  nvl(cast(is_empty(map_from_str(extra_info)['提前还款手续费'],0) as decimal(30,10)),0)          as termfee,
  nvl(cast(is_empty(map_from_str(extra_info)['其它相关费用'],0) as decimal(30,10)),0)            as svcfee,
  is_empty(create_time)                                                                          as create_time,
  is_empty(update_time)                                                                          as update_time,
  is_empty(map_from_str(extra_info)['项目编号'],project_id)                                      as project_id,
  '123' as aa
from ods.t_repayment_info
where 1 > 0
  and is_empty(map_from_str(extra_info)['借据号'],asset_id) = 'GALC-HL-1606300219'
  and is_empty(map_from_str(extra_info)['期次'],period) = 6
;

