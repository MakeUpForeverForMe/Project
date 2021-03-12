
  'ods_new_s 放款 与 dw_new 放款 对比 累计借据数、累计放款金额' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  '${var:ST9}'                                          as biz_date,

====================================================================================================================================================================================================================
-- ods_new_s 与 dm_eagle 数据对比
====================================================================================================================================================================================================================

-- set var:db_suffix=;

set var:db_suffix=_cps;

set var:ST9=2020-11-28;

invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata ods_new_s${var:db_suffix}.repay_detail;
invalidate metadata dm_eagle${var:db_suffix}.eagle_should_repay_repaid_amount_day;
-- ods 应还、实还 与 dm 应还实还 对比 应还金额、实还金额
select
  'ods 应还、实还 与 dm 应还实还 对比 应还金额、实还金额'             as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                       as cps,
  nvl(schedule.biz_date,repay_detail.biz_date)                        as biz_date,
  nvl(schedule.project_id,repay_detail.project_id)                    as project_id,
  nvl(schedule.should_repay_amount,0)                                 as should_dw,
  nvl(dm.should_repay_amount,0)                                       as should_dm,
  nvl(schedule.should_repay_amount,0) - nvl(dm.should_repay_amount,0) as diff_should,
  nvl(repay_detail.repaid_amount,0)                                   as repaid_dw,
  nvl(dm.repaid_amount,0)                                             as repaid_dm,
  nvl(repay_detail.repaid_amount,0) - nvl(dm.repaid_amount,0)         as diff_repaid
from (
  select
    project_id               as project_id,
    should_repay_date        as biz_date,
    sum(should_repay_amount) as should_repay_amount
  from (
    select product_id,project_id
    from dim_new.biz_conf
    where 1 > 0
      and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  ) as biz_conf
  join (
    select
      product_id                             as product_id,
      should_repay_date                      as should_repay_date,
      sum(should_repay_amount - paid_amount) as should_repay_amount
    from ods_new_s${var:db_suffix}.repay_schedule
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and should_repay_date <= '${var:ST9}'
      and should_repay_date = '${var:ST9}'
    group by product_id,should_repay_date
  ) as repay_schedule
  on biz_conf.product_id = repay_schedule.product_id
  group by project_id,should_repay_date
) as schedule
full join (
  select
    project_id         as project_id,
    biz_date           as biz_date,
    sum(repaid_amount) as repaid_amount
  from (
    select product_id,project_id
    from dim_new.biz_conf
    where 1 > 0
      and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  ) as biz_conf
  join (
    select
      product_id        as product_id,
      biz_date          as biz_date,
      sum(repay_amount) as repaid_amount
    from ods_new_s${var:db_suffix}.repay_detail
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and biz_date <= '${var:ST9}'
      and biz_date = '${var:ST9}'
    group by product_id,biz_date
  ) as repay_detail
  on biz_conf.product_id = repay_detail.product_id
  group by project_id,biz_date
) as repay_detail
on  schedule.project_id = repay_detail.project_id
and schedule.biz_date   = repay_detail.biz_date
left join (
  select
    project_id               as project_id,
    biz_date                 as biz_date,
    sum(should_repay_amount) as should_repay_amount,
    sum(repaid_amount)       as repaid_amount
  from dm_eagle${var:db_suffix}.eagle_should_repay_repaid_amount_day
  where 1 > 0
    and project_id in ('WS0006200001','WS0006200002','WS0009200001')
    and biz_date <= '${var:ST9}'
    and biz_date = '${var:ST9}'
  group by biz_date,project_id
) as dm
on  nvl(schedule.project_id,repay_detail.project_id) = dm.project_id
and nvl(schedule.biz_date,repay_detail.biz_date)     = dm.biz_date
where nvl(schedule.should_repay_amount,0) - nvl(dm.should_repay_amount,0) != 0 or nvl(repay_detail.repaid_amount,0) - nvl(dm.repaid_amount,0) != 0
order by biz_date,project_id
;


====================================================================================================================================================================================================================

set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-11-02;

invalidate metadata ods_new_s${var:db_suffix}.loan_lending;
invalidate metadata dm_eagle${var:db_suffix}.eagle_loan_amount_day;
-- ods 放款 与 dm 放款规模 对比 放款金额
select
  'ods 放款 与 dm 放款规模 对比 放款金额'                      as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                as cps,
  loan_amount.biz_date                                         as biz_date,
  loan_amount.product_id                                       as product_id,
  nvl(loan_amount.loan_principal,0)                            as principal_dw,
  nvl(dm.loan_principal,0)                                     as principal_dm,
  nvl(loan_amount.loan_principal,0) - nvl(dm.loan_principal,0) as diff_principal
from (
  select
    product_id_vt as product_id,
    biz_date,
    sum(loan_principal) over(partition by product_id_vt,biz_date) as loan_principal
  from (
    select product_id,product_id_vt
    from dim_new.biz_conf
    where 1 > 0
      and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  ) as biz_conf
  join (
    select
      product_id               as product_id,
      biz_date                 as biz_date,
      sum(loan_init_principal) as loan_principal
    from ods_new_s${var:db_suffix}.loan_lending
    where 1 > 0
    and biz_date <= '${var:ST9}'
    -- and biz_date = '${var:ST9}'
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    group by product_id,biz_date
  ) as tmp
  on biz_conf.product_id = tmp.product_id
) as loan_amount
left join (
  select
    product_id       as product_id,
    biz_date         as biz_date,
    sum(loan_amount) as loan_principal
  from dm_eagle${var:db_suffix}.eagle_loan_amount_day
  where 1 > 0
    and biz_date <= '${var:ST9}'
    -- and biz_date = '${var:ST9}'
    and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  group by product_id,biz_date
) as dm
on  loan_amount.product_id = dm.product_id
and loan_amount.biz_date   = dm.biz_date
where nvl(loan_amount.loan_principal,0) - nvl(dm.loan_principal,0) != 0
order by biz_date,product_id;




set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-11-02;

invalidate metadata ods_new_s${var:db_suffix}.loan_lending;
invalidate metadata dm_eagle${var:db_suffix}.eagle_loan_amount_day;
-- ods 放款 与 dm 放款规模 对比 累计放款金额
select
  'ods 放款 与 dm 放款规模 对比 累计放款金额'                  as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                as cps,
  '${var:ST9}'                                                 as biz_date,
  loan_amount.product_id                                       as product_id,
  nvl(loan_amount.loan_principal,0)                            as principal_dw,
  nvl(dm.loan_principal,0)                                     as principal_dm,
  nvl(loan_amount.loan_principal,0) - nvl(dm.loan_principal,0) as diff_principal
from (
  select
    product_id_vt as product_id,
    sum(loan_init_principal) as loan_principal
  from dim_new.biz_conf
  join ods_new_s${var:db_suffix}.loan_lending as loan_lending
  on  biz_conf.product_id = loan_lending.product_id
  and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  and biz_date <= '${var:ST9}'
  group by product_id_vt
) as loan_amount
left join (
  select
    product_id                  as product_id,
    sum(loan_amount_accumulate) as loan_principal
  from dm_eagle${var:db_suffix}.eagle_loan_amount_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  group by product_id
) as dm
on  loan_amount.product_id = dm.product_id
where nvl(loan_amount.loan_principal,0) - nvl(dm.loan_principal,0) != 0
order by biz_date,product_id;


====================================================================================================================================================================================================================

set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-11-02;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dm_eagle${var:db_suffix}.eagle_asset_scale_principal_day;
-- ods 借据 与 dm 资产规模 对比 本金余额、逾期本金、未出账本金
select
  'ods 放款 与 dm 放款规模 对比 本金余额、逾期本金、未出账本金'           as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                           as cps,
  '${var:ST9}'                                                            as biz_date,
  nvl(loan_info.product_id,repay_schedule.product_id)                     as product_id,
  nvl(loan_info.remain_principal,0)                                       as remain_ods,
  nvl(dm.remain_principal,0)                                              as remain_dm,
  nvl(loan_info.remain_principal,0)        - nvl(dm.remain_principal,0)   as diff_remain,
  nvl(repay_schedule.unposted_principal,0)                                as unposted_ods,
  nvl(dm.unposted_principal,0)                                            as unposted_dm,
  nvl(repay_schedule.unposted_principal,0) - nvl(dm.unposted_principal,0) as diff_unposted,
  nvl(loan_info.overdue_principal,0)                                      as overdue_ods,
  nvl(dm.overdue_principal,0)                                             as overdue_dm,
  nvl(loan_info.overdue_principal,0)       - nvl(dm.overdue_principal,0)  as diff_overdue,
  nvl(dm.remain_principal,0) - (nvl(dm.unposted_principal,0) + nvl(dm.overdue_principal,0)) + if('${var:ST9}' >= '2020-06-23' and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802',200,0) as diff_principal
from (
  select
    product_id_vt          as product_id,
    sum(remain_principal)  as remain_principal,
    sum(overdue_principal) as overdue_principal
  from (
    select product_id,product_id_vt
    from dim_new.biz_conf
    where 1 > 0
      and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  ) as biz_conf
  join (
    select
      product_id,
      remain_principal,
      overdue_principal
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  ) as loan_info
  on biz_conf.product_id = loan_info.product_id
  group by product_id_vt
) as loan_info
full join (
  select
    product_id_vt               as product_id,
    sum(should_repay_principal) as unposted_principal
  from (
    select product_id,product_id_vt
    from dim_new.biz_conf
    where 1 > 0
      and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  ) as biz_conf
  join (
    select
      product_id,
      should_repay_principal
    from ods_new_s${var:db_suffix}.repay_schedule
    where 1 > 0
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and schedule_status = 'N'
  ) as loan_info
  on biz_conf.product_id = loan_info.product_id
  group by product_id_vt
) as repay_schedule
on loan_info.product_id = repay_schedule.product_id
left join (
  select
    product_id              as product_id,
    sum(remain_principal)   as remain_principal,
    sum(overdue_principal)  as overdue_principal,
    sum(unposted_principal) as unposted_principal
  from dm_eagle${var:db_suffix}.eagle_asset_scale_principal_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  group by product_id
) as dm
on nvl(loan_info.product_id,repay_schedule.product_id) = dm.product_id
where
nvl(loan_info.remain_principal,0)        - nvl(dm.remain_principal,0)   != 0 or
nvl(repay_schedule.unposted_principal,0) - nvl(dm.unposted_principal,0) != 0 or
nvl(loan_info.overdue_principal,0)       - nvl(dm.overdue_principal,0)  != 0 or
nvl(dm.remain_principal,0) - (nvl(dm.unposted_principal,0) + nvl(dm.overdue_principal,0)) + if('${var:ST9}' >= '2020-06-23' and nvl(loan_info.product_id,repay_schedule.product_id) = 'vt_001802',200,0) != 0
order by biz_date,product_id;


====================================================================================================================================================================================================================

set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-11-02;

invalidate metadata ods_new_s${var:db_suffix}.repay_detail;
invalidate metadata dm_eagle${var:db_suffix}.eagle_asset_scale_repaid_day;
-- ods 实还 与 dm 回款规模 对比 实还金额
select
  'ods 实还 与 dm 回款规模 对比 实还金额'                         as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                   as cps,
  repay_detail.biz_date                                           as biz_date,
  repay_detail.product_id                                         as product_id,
  nvl(repay_detail.repay_amount,0)                                as ods_amount,
  nvl(dm.repay_amount,0)                                          as dm_amount,
  nvl(repay_detail.repay_amount,0)    - nvl(dm.repay_amount,0)    as diff_overdue,
  nvl(repay_detail.repay_principal,0)                             as ods_principal,
  nvl(dm.repay_principal,0)                                       as dm_principal,
  nvl(repay_detail.repay_principal,0) - nvl(dm.repay_principal,0) as diff_overdue,
  nvl(repay_detail.repay_interest,0)                              as ods_interest,
  nvl(dm.repay_interest,0)                                        as dm_interest,
  nvl(repay_detail.repay_interest,0)  - nvl(dm.repay_interest,0)  as diff_overdue,
  nvl(repay_detail.repay_svc_fee,0)                               as ods_term_svc,
  nvl(dm.repay_svc_fee,0)                                         as dm_term_svc,
  nvl(repay_detail.repay_svc_fee,0)   - nvl(dm.repay_svc_fee,0)   as diff_overdue,
  nvl(repay_detail.repay_penalty,0)                               as ods_penalty,
  nvl(dm.repay_penalty,0)                                         as dm_penalty,
  nvl(repay_detail.repay_penalty,0)   - nvl(dm.repay_penalty,0)   as diff_overdue
from (
  select
    product_id_vt       as product_id,
    biz_date            as biz_date,
    sum(paid_amount)    as repay_amount,
    sum(paid_principal) as repay_principal,
    sum(paid_interest)  as repay_interest,
    sum(paid_term_svc)  as repay_svc_fee,
    sum(paid_penalty)   as repay_penalty
  from (
    select product_id,product_id_vt
    from dim_new.biz_conf
    where 1 > 0
      and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  ) as biz_conf
  join (
    select
      biz_date,
      product_id,
      sum(repay_amount)                               as paid_amount,    -- 金额
      sum(if(bnp_type = 'Pricinpal', repay_amount,0)) as paid_principal, -- 本金
      sum(if(bnp_type = 'Interest',  repay_amount,0)) as paid_interest,  -- 利息
      sum(if(bnp_type = 'TERMFee',   repay_amount,0)) + sum(if(bnp_type = 'SVCFee',repay_amount,0)) as paid_term_svc,  -- 费用
      sum(if(bnp_type = 'Penalty',   repay_amount,0)) as paid_penalty    -- 罚息
    from ods_new_s${var:db_suffix}.repay_detail
    where 1 > 0
      and biz_date <= '${var:ST9}'
      and biz_date = '${var:ST9}'
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and bnp_type in ('Pricinpal','Interest','TERMFee','SVCFee','Penalty')
    group by product_id,biz_date
  ) as repay_detail
  on biz_conf.product_id = repay_detail.product_id
  group by product_id_vt,biz_date
) as repay_detail
left join (
  select
    product_id           as product_id,
    biz_date             as biz_date,
    sum(repay_amount)    as repay_amount,
    sum(repay_principal) as repay_principal,
    sum(repay_interest)  as repay_interest,
    sum(repay_svc_fee)   as repay_svc_fee,
    sum(repay_penalty)   as repay_penalty
  from dm_eagle${var:db_suffix}.eagle_asset_scale_repaid_day
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and biz_date = '${var:ST9}'
    and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  group by product_id,biz_date
) as dm
on  repay_detail.product_id = dm.product_id
and repay_detail.biz_date   = dm.biz_date
where
nvl(repay_detail.repay_amount,0)    - nvl(dm.repay_amount,0)    != 0 or
nvl(repay_detail.repay_principal,0) - nvl(dm.repay_principal,0) != 0 or
nvl(repay_detail.repay_interest,0)  - nvl(dm.repay_interest,0)  != 0 or
nvl(repay_detail.repay_svc_fee,0)   - nvl(dm.repay_svc_fee,0)   != 0 or
nvl(repay_detail.repay_penalty,0)   - nvl(dm.repay_penalty,0)   != 0
order by biz_date,product_id;





set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-11-02;

invalidate metadata ods_new_s${var:db_suffix}.repay_detail;
invalidate metadata dm_eagle${var:db_suffix}.eagle_asset_scale_repaid_day;
-- ods 实还 与 dm 回款规模 对比 累计实还金额
select
  'ods 实还 与 dm 回款规模 对比 累计实还金额'                     as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                   as cps,
  '${var:ST9}'                                                    as biz_date,
  repay_detail.product_id                                         as product_id,
  nvl(repay_detail.repay_amount,0)                                as ods_amount,
  nvl(dm.repay_amount,0)                                          as dm_amount,
  nvl(repay_detail.repay_amount,0)    - nvl(dm.repay_amount,0)    as diff_overdue,
  nvl(repay_detail.repay_principal,0)                             as ods_principal,
  nvl(dm.repay_principal,0)                                       as dm_principal,
  nvl(repay_detail.repay_principal,0) - nvl(dm.repay_principal,0) as diff_overdue,
  nvl(repay_detail.repay_interest,0)                              as ods_interest,
  nvl(dm.repay_interest,0)                                        as dm_interest,
  nvl(repay_detail.repay_interest,0)  - nvl(dm.repay_interest,0)  as diff_overdue,
  nvl(repay_detail.repay_svc_fee,0)                               as ods_term_svc,
  nvl(dm.repay_svc_fee,0)                                         as dm_term_svc,
  nvl(repay_detail.repay_svc_fee,0)   - nvl(dm.repay_svc_fee,0)   as diff_overdue,
  nvl(repay_detail.repay_penalty,0)                               as ods_penalty,
  nvl(dm.repay_penalty,0)                                         as dm_penalty,
  nvl(repay_detail.repay_penalty,0)   - nvl(dm.repay_penalty,0)   as diff_overdue
from (
  select
    product_id_vt as product_id,
    sum(repay_amount)                                                                        as repay_amount,    -- 金额
    sum(if(bnp_type = 'Pricinpal', repay_amount,0))                                          as repay_principal, -- 本金
    sum(if(bnp_type = 'Interest',  repay_amount,0))                                          as repay_interest,  -- 利息
    sum(if(bnp_type = 'TERMFee',   repay_amount,0) + if(bnp_type = 'SVCFee',repay_amount,0)) as repay_svc_fee,  -- 费用
    sum(if(bnp_type = 'Penalty',   repay_amount,0))                                          as repay_penalty    -- 罚息
  from ods_new_s${var:db_suffix}.repay_detail
  join dim_new.biz_conf
  on  biz_conf.product_id = repay_detail.product_id
  and biz_date <= '${var:ST9}'
  and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  and bnp_type in ('Pricinpal','Interest','TERMFee','SVCFee','Penalty')
  group by product_id_vt
) as repay_detail
left join (
  select
    product_id          as product_id,
    sum(paid_amount)    as repay_amount,
    sum(paid_principal) as repay_principal,
    sum(paid_interest)  as repay_interest,
    sum(paid_svc_fee)   as repay_svc_fee,
    sum(paid_penalty)   as repay_penalty
  from dm_eagle${var:db_suffix}.eagle_asset_scale_repaid_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  group by product_id
) as dm
on repay_detail.product_id = dm.product_id
where
nvl(repay_detail.repay_amount,0)    - nvl(dm.repay_amount,0)    != 0 or
nvl(repay_detail.repay_principal,0) - nvl(dm.repay_principal,0) != 0 or
nvl(repay_detail.repay_interest,0)  - nvl(dm.repay_interest,0)  != 0 or
nvl(repay_detail.repay_svc_fee,0)   - nvl(dm.repay_svc_fee,0)   != 0 or
nvl(repay_detail.repay_penalty,0)   - nvl(dm.repay_penalty,0)   != 0
order by biz_date,product_id;


====================================================================================================================================================================================================================

set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-25;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dm_eagle${var:db_suffix}.eagle_inflow_rate_day;
-- ods_new_s 借据、应还 与 dm 逾期流入率 对比 应还日本金余额（应还日为T，T-1为C的T-1日实际应还本金）、应还日应还本金（应还日为T，T-1为C的T-1日实际本金余额）、逾期借据逾期本金、逾期借据本金余额
select
  nvl(ods_new_s.biz_date,dm_eagle.biz_date)                   as biz_date,
  nvl(ods_new_s.product_id,dm_eagle.product_id)               as product_id,
  nvl(ods_new_s.loan_active_month,dm_eagle.loan_active_month) as loan_active_month,
  nvl(ods_new_s.dob,dm_eagle.dob)                             as dob,
  nvl(ods_new_s.should_repay_date,dm_eagle.should_repay_date) as should_repay_date,
  nvl(ods_new_s.remain_principal,0)                           as remain_principal_ods,
  nvl(dm_eagle.remain_principal,0)                            as remain_principal_dm
from (
  select
    '${var:ST9}'          as biz_date,
    product_id            as product_id,
    loan_active_month     as loan_active_month,
    dob                   as dob,
    should_repay_date     as should_repay_date,
    sum(remain_principal) as remain_principal
  from (
    select
      product_id,
      due_bill_no,
      substr(loan_active_date,1,7) as loan_active_month,
      overdue_days as dob,
      remain_principal
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and overdue_days between 1 and 30
  ) as loan_info
  join (
    select
      product_id as product_id_c,
      due_bill_no as due_bill_no_c,
      substr(loan_active_date,1,7) as active_month_c,
      s_d_date,
      e_d_date
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and overdue_days = 0
  ) as loan_c
  on  product_id        = product_id_c
  and due_bill_no       = due_bill_no_c
  and loan_active_month = active_month_c
  join (
    select
      product_id as product_id_s,
      due_bill_no as due_bill_no_s,
      substr(loan_active_date,1,7) as active_month_s,
      should_repay_date
    from ods_new_s${var:db_suffix}.repay_schedule
    where 1 > 0
      and e_d_date = '3000-12-31'
      and should_repay_date < '${var:ST9}'
  ) as repay_schedule
  on  product_id        = product_id_s
  and due_bill_no       = due_bill_no_s
  and loan_active_month = active_month_s
  and should_repay_date = date_sub('${var:ST9}',dob - 1) -- 取应还日
  where 1 > 0
    and date_sub('${var:ST9}',dob) between s_d_date and date_sub(e_d_date,1) -- 应还日前一天
  group by product_id,loan_active_month,dob,should_repay_date
) as ods_new_s
full join (
  select
    '${var:ST9}'                  as biz_date,
    substr(product_id,4)          as product_id,
    loan_active_month             as loan_active_month,
    cast(dob as int)              as dob,
    should_repay_date             as should_repay_date,
    sum(overdue_remain_principal) as remain_principal
  from dm_eagle.eagle_inflow_rate_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and is_first = 'n'
  group by product_id,loan_active_month,dob,should_repay_date
) as dm_eagle
on  ods_new_s.biz_date          = dm_eagle.biz_date
and ods_new_s.product_id        = dm_eagle.product_id
and ods_new_s.loan_active_month = dm_eagle.loan_active_month
and ods_new_s.dob               = dm_eagle.dob
and ods_new_s.should_repay_date = dm_eagle.should_repay_date
where 1 > 0
  and nvl(ods_new_s.remain_principal,0) - nvl(dm_eagle.remain_principal,0) != 0
  and ods_new_s.product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  -- and ods_new_s.product_id = '001802'
order by product_id,loan_active_month,dob;













====================================================================================================================================================================================================================

















====================================================================================================================================================================================================================
















====================================================================================================================================================================================================================
















====================================================================================================================================================================================================================



















set var:db_suffix=;

-- set var:db_suffix=_cps;

invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_loan_num_day;
invalidate metadata dm_eagle${var:db_suffix}.eagle_inflow_rate_first_term_day;

-- 当天放款金额 dw 与 dm 对比   dw 放款表与 dm 首期流入表对比
select
  -- nvl(dw.loan_terms,dm.loan_terms)                    as loan_terms,
  nvl(dw.loan_principal,0)                            as principal_dw,
  nvl(dm.loan_principal,0)                            as principal_dm,
  nvl(dw.loan_principal,0) - nvl(dm.loan_principal,0) as principal_diff,
  nvl(dw.loan_active_date,dm.loan_active_date)        as loan_active_date,
  nvl(dw.product_id,dm.product_id)                    as product_id
from (
  select
    sum(loan_principal) as loan_principal,
    -- loan_terms          as loan_terms,
    biz_date            as loan_active_date,
    product_id
  from dw_new${var:db_suffix}.dw_loan_base_stat_loan_num_day
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by biz_date,product_id
  -- ,loan_terms
) as dw
full join (
  select
    sum(loan_amount) as loan_principal,
    -- loan_terms       as loan_terms,
    biz_date         as loan_active_date,
    product_id
  from (
    select distinct
      loan_terms,
      loan_amount,
      loan_active_date as biz_date,
      product_id
    from dm_eagle${var:db_suffix}.eagle_inflow_rate_first_term_day
    where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  ) as tmp
  group by biz_date,product_id
  -- ,loan_terms
) as dm
on  dw.product_id       = dm.product_id
and dw.loan_active_date = dm.loan_active_date
-- and dw.loan_terms       = dm.loan_terms
where 1 > 0
  -- and nvl(dw.loan_principal,0) - nvl(dm.loan_principal,0) != 0
order by loan_active_date,product_id
-- ,loan_terms
;




==============================================================================================================================



set var:ST9=2020-08-10;

set var:db_suffix=;
set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;
-- set var:tb_suffix=;

select '${var:ST9}' as biz_date;

invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day;
invalidate metadata dm_eagle${var:db_suffix}.eagle_inflow_rate_first_term_day;
-- 校验 dw 与 dm 的逾期本金 -- dw 逾期 与 dm 首期流入率 表 对比
select
  dw_new.overdue_principal                                     as overdue_principal_dw,
  nvl(dm_eagle.overdue_principal,0)                            as overdue_principal_dm,
  dw_new.overdue_principal - nvl(dm_eagle.overdue_principal,0) as overdue_principal_diff,
  dw_new.biz_date                                              as biz_date,
  dw_new.product_id                                            as product_id
from (
  select
    sum(overdue_principal) as overdue_principal,
    biz_date,
    product_id
  from dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and loan_term_overdue = 1
    and overdue_days between 1 and 30
    and overdue_mob <= 12
    -- and biz_date >= '${var:ST9}'
  group by product_id,biz_date
) as dw_new
left join (
  select
    sum(overdue_principal)  as overdue_principal,
    biz_date,
    product_id
  from dm_eagle${var:db_suffix}.eagle_inflow_rate_first_term_day
  group by biz_date,product_id
) as dm_eagle
on  dw_new.biz_date   = dm_eagle.biz_date
and dw_new.product_id = dm_eagle.product_id
order by biz_date,product_id
;







==============================================================================================================================






-- 验证 逾期率表现表 的金额
invalidate metadata dm_eagle.eagle_overdue_rate_month;
select
  *
  -- distinct biz_date,product_id
from dm_eagle.eagle_overdue_rate_month
where 1 > 0
  and product_id in ('vt_001801','vt_001802','vt_001803','vt_001804','vt_001901','vt_001902','vt_001903','vt_001904','vt_001905','vt_001906','vt_001907','vt_002001','vt_002002','vt_002003','vt_002004','vt_002005','vt_002006','vt_002007')
  -- and product_id = 'vt_001801'
  -- and biz_date like '2020-06%'
  -- and biz_date = '2020-08-11'
  -- and biz_date >= '2020-06-15'
  -- and biz_date in ('2020-09-04','2020-09-05','2020-09-06')
  -- and loan_terms = 6
  -- and dpd = '1+'
-- order by product_id,biz_date,loan_month,loan_terms,dpd
having
loan_amount < loan_principal_deferred
or
loan_principal_deferred < overdue_principal
or
overdue_remain_principal > overdue_remain_principal_once
order by product_id,biz_date
limit 100
;






==============================================================================================================================


invalidate metadata dm_eagle.eagle_inflow_rate_day;
select
  *
from dm_eagle.eagle_inflow_rate_day
where 1 > 0
  and product_id in ('vt_001801','vt_001802','vt_001803','vt_001804','vt_001901','vt_001902','vt_001903','vt_001904','vt_001905','vt_001906','vt_001907','vt_002001','vt_002002','vt_002003','vt_002004','vt_002005','vt_002006','vt_002007')
  and (
    -- dob is null
    -- or should_repay_loan_num = 0
    -- or
    should_repay_principal > remain_principal
  )
order by biz_date,product_id
limit 10
;













invalidate metadata dm_eagle.eagle_should_repay_repaid_amount_day;
invalidate metadata dm_eagle_cps.eagle_should_repay_repaid_amount_day;
select
  nvl(no_cps.project_id,hv_cps.project_id)                                      as project_id,
  nvl(no_cps.should_repay_amount_sum,0)                                         as should_amount_no,
  nvl(hv_cps.should_repay_amount_sum,0)                                         as should_amount_hv,
  nvl(no_cps.should_repay_amount_sum,0) - nvl(hv_cps.should_repay_amount_sum,0) as should_amount,
  nvl(no_cps.repaid_amount_sum,0)                                               as repaid_amount_no,
  nvl(hv_cps.repaid_amount_sum,0)                                               as repaid_amount_hv,
  nvl(no_cps.repaid_amount_sum,0) - nvl(hv_cps.repaid_amount_sum,0)             as should_amount,
  nvl(no_cps.repaid_amount_sum,0) < nvl(hv_cps.repaid_amount_sum,0)             as flag
from (
  select
    sum(should_repay_amount) as should_repay_amount_sum,
    sum(repaid_amount) as repaid_amount_sum,
    project_id
  from dm_eagle.eagle_should_repay_repaid_amount_day
  where 1 > 0
    and biz_date <= '2020-10-28'
    and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  group by project_id
) as no_cps
full join (
  select
    sum(should_repay_amount) as should_repay_amount_sum,
    sum(repaid_amount) as repaid_amount_sum,
    project_id
  from dm_eagle_cps.eagle_should_repay_repaid_amount_day
  where 1 > 0
    and biz_date <= '2020-10-28'
    and project_id in ('WS0006200001','WS0006200002','WS0009200001')
  group by project_id
) as hv_cps
on no_cps.project_id = hv_cps.project_id
order by project_id
;
