--set var:db_suffix=;

--set var:db_suffix=_cps;

--set var:ST9=2021-01-25;

invalidate metadata ods${var:db_suffix}.repay_schedule;
invalidate metadata dw${var:db_suffix}.dw_loan_base_stat_should_repay_day;
-- ods 应还 与 dw 应还 对比 应还金额
select
  'ods 应还 与 dw 应还 对比 应还金额'                         as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                         as cps,
  '${var:ST9}'                                                          as biz_date,
  nvl(dws.should_repay_date,dw.should_repay_date)                       as should_repay_date,
  nvl(dws.product_id,dw.product_id)                                     as product_id,
  nvl(dw.should_repay_amount,0)                                         as amount_new,
  nvl(dws.should_repay_amount,0)                                        as amount_dw,
  nvl(dw.should_repay_amount,0)    - nvl(dws.should_repay_amount,0)     as diff_amount,
  nvl(dw.should_repay_principal,0)                                      as principal_new,
  nvl(dws.should_repay_principal,0)                                     as principal_dw,
  nvl(dw.should_repay_principal,0) - nvl(dws.should_repay_principal,0)  as diff_principal,
  nvl(dw.should_repay_interest,0)                                       as interest_new,
  nvl(dws.should_repay_interest,0)                                      as interest_dw,
  nvl(dw.should_repay_interest,0)  - nvl(dws.should_repay_interest,0)   as diff_interest,
  nvl(dw.should_repay_term_fee,0)                                       as term_fee_new,
  nvl(dws.should_repay_term_fee,0)                                      as term_fee_dw,
  nvl(dw.should_repay_term_fee,0)  - nvl(dws.should_repay_term_fee,0)   as diff_term_fee,
  nvl(dw.should_repay_svc_fee,0)                                        as svc_fee_new,
  nvl(dws.should_repay_svc_fee,0)                                       as svc_fee_dw,
  nvl(dw.should_repay_svc_fee,0)   - nvl(dws.should_repay_svc_fee,0)    as diff_svc_fee,
  nvl(dw.should_repay_penalty,0)                                        as penalty_new,
  nvl(dws.should_repay_penalty,0)                                       as penalty_dw,
  nvl(dw.should_repay_penalty,0)   - nvl(dws.should_repay_penalty,0)    as diff_penalty
from (
  select
    product_id                  as product_id,
    should_repay_date           as should_repay_date,
    sum(should_repay_amount)    as should_repay_amount,
    sum(should_repay_principal) as should_repay_principal,
    sum(should_repay_interest)  as should_repay_interest,
    sum(should_repay_term_fee)  as should_repay_term_fee,
    sum(should_repay_svc_fee)   as should_repay_svc_fee,
    sum(should_repay_penalty)   as should_repay_penalty
  from (
    select
      product_id,
      due_bill_no,
      loan_active_date,
      loan_term,
      should_repay_date,
      lag(should_repay_date,1,'1970-01-01') over(partition by due_bill_no order by loan_term) as lag_should_repay_date,
      loan_init_principal,
      should_repay_amount,
      should_repay_principal,
      should_repay_interest,
      should_repay_term_fee,
      should_repay_svc_fee,
      should_repay_penalty
    from ods${var:db_suffix}.repay_schedule -- 必须经过去重。原因：滴滴多期的应还日可能会变成同一天，这样的放款金额就会加倍
    where 1 > 0
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
      and ('${var:ST9}' between s_d_date and date_sub(e_d_date,1))
  ) as tmp
  where ('${var:ST9}' between date_add(lag_should_repay_date,1) and should_repay_date)
  group by product_id,should_repay_date
) as dw
full join (
  select
    product_id                  as product_id,
    should_repay_date           as should_repay_date,
    sum(should_repay_amount)    as should_repay_amount,
    sum(should_repay_principal) as should_repay_principal,
    sum(should_repay_interest)  as should_repay_interest,
    sum(should_repay_term_fee)  as should_repay_term_fee,
    sum(should_repay_svc_fee)   as should_repay_svc_fee,
    sum(should_repay_penalty)   as should_repay_penalty
  from dw${var:db_suffix}.dw_loan_base_stat_should_repay_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402',
      ''
    )
  group by product_id,should_repay_date
) as dws
on  dw.product_id        = dws.product_id
and dw.should_repay_date = dws.should_repay_date
where 1 > 0
  and (
    nvl(dw.should_repay_amount,0)    - nvl(dws.should_repay_amount,0)    != 0 or
    nvl(dw.should_repay_principal,0) - nvl(dws.should_repay_principal,0) != 0 or
    nvl(dw.should_repay_interest,0)  - nvl(dws.should_repay_interest,0)  != 0 or
    nvl(dw.should_repay_term_fee,0)  - nvl(dws.should_repay_term_fee,0)  != 0 or
    nvl(dw.should_repay_svc_fee,0)   - nvl(dws.should_repay_svc_fee,0)   != 0 or
    nvl(dw.should_repay_penalty,0)   - nvl(dws.should_repay_penalty,0)   != 0
  )
order by should_repay_date,product_id
limit 10
;
