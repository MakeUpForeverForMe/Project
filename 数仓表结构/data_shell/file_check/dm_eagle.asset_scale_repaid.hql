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
      and project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
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
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
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
    and project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
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

