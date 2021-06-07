invalidate metadata ods${var:db_suffix}.repay_detail;
invalidate metadata dw${var:db_suffix}.dw_loan_base_stat_repay_detail_day;

-- ods 实还 与 dw 实还 对比 实还金额


--set var:ST9=2020-12-02;
--set var:db_suffix=;set var:tb_suffix=_asset;


select
  'ods 实还 与 dw 实还 对比 实还金额'         as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  nvl(dw.biz_date,ods.biz_date)                         as biz_date,
  nvl(dw.product_id,ods.product_id)                     as product_id,
  
  nvl(ods.paid_amount,0)                                as amount_ods,
  nvl(dw.paid_amount,0)                                 as amount_dws,
  nvl(ods.paid_amount,0)    - nvl(dw.paid_amount,0)     as diff_amount,
  
  nvl(ods.paid_principal,0)                             as principal_ods,
  nvl(dw.paid_principal,0)                              as principal_dws,
  nvl(ods.paid_principal,0) - nvl(dw.paid_principal,0)  as diff_principal,
  
  nvl(ods.paid_interest,0)                              as interest_ods,
  nvl(dw.paid_interest,0)                               as interest_dws,
  nvl(ods.paid_interest,0)  - nvl(dw.paid_interest,0)   as diff_interest,
  
  nvl(ods.paid_term_fee,0)                              as term_fee_ods,
  nvl(dw.paid_term_fee,0)                               as term_fee_dws,
  nvl(ods.paid_term_fee,0)  - nvl(dw.paid_term_fee,0)   as diff_term_fee,
  
  nvl(ods.paid_svc_fee,0)                               as svc_fee_ods,
  nvl(dw.paid_svc_fee,0)                                as svc_fee_dws,
  nvl(ods.paid_svc_fee,0)   - nvl(dw.paid_svc_fee,0)    as diff_svc_fee,
  
  nvl(ods.paid_penalty,0)                               as penalty_ods,
  nvl(dw.paid_penalty,0)                                as penalty_dws,
  nvl(ods.paid_penalty,0)   - nvl(dw.paid_penalty,0)    as diff_penalty
  
from (
  select
    biz_date,
    product_id,
    sum(repay_amount)                               as paid_amount,    -- 金额
    sum(if(bnp_type = 'Pricinpal', repay_amount,0)) as paid_principal, -- 本金
    sum(if(bnp_type = 'Interest',  repay_amount,0)) as paid_interest,  -- 利息
    sum(if(bnp_type = 'TERMFee',   repay_amount,0)) as paid_term_fee,  -- 手续费
    sum(if(bnp_type = 'SVCFee',    repay_amount,0)) as paid_svc_fee,   -- 服务费
    sum(if(bnp_type = 'Penalty',   repay_amount,0)) as paid_penalty    -- 罚息
  from ods${var:db_suffix}.repay_detail
  where 1 > 0
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
  -- order by biz_date,product_id
) as ods
full join (
  select
    biz_date,
    product_id,
    sum(repaid_amount)         as paid_amount,    -- 金额
    sum(repaid_principal)      as paid_principal, -- 本金
    sum(repaid_interest)       as paid_interest,  -- 利息
    sum(repaid_repay_term_fee) as paid_term_fee,  -- 手续费
    sum(repaid_repay_svc_fee)  as paid_svc_fee,   -- 服务费
    sum(repaid_penalty)        as paid_penalty    -- 罚息
  from dw${var:db_suffix}.dw_loan_base_stat_repay_detail_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402',
      ''
    )
  group by product_id,biz_date
  -- order by biz_date,product_id
) as dw
on  ods.product_id = dw.product_id
and ods.biz_date   = dw.biz_date
where
nvl(ods.paid_amount,0)    - nvl(dw.paid_amount,0)    != 0 or
nvl(ods.paid_principal,0) - nvl(dw.paid_principal,0) != 0 or
nvl(ods.paid_interest,0)  - nvl(dw.paid_interest,0)  != 0 or
nvl(ods.paid_term_fee,0)  - nvl(dw.paid_term_fee,0)  != 0 or
nvl(ods.paid_svc_fee,0)   - nvl(dw.paid_svc_fee,0)   != 0 or
nvl(ods.paid_penalty,0)   - nvl(dw.paid_penalty,0)   != 0
order by biz_date,product_id
-- limit 10
;

