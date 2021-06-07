 set var:db_suffix=;

--set var:db_suffix=_cps;
--set var:ST9=2021-01-25;

invalidate metadata ods${var:db_suffix}.repay_schedule;
invalidate metadata dw${var:db_suffix}.dw_loan_base_stat_should_repay_day;
-- ods 应还 与 dw 应还 对比 放款金额
select
  'ods 应还 与 dw 应还 对比 放款金额'                   as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  '${var:ST9}'                                          as biz_date,
  nvl(dw.loan_active_date,ods.loan_active_date)         as active_date,
  nvl(dw.product_id,ods.product_id)                     as product_id,
  nvl(dw.should_repay_date,ods.should_repay_date)       as should_repay_date,
  nvl(ods.loan_principal,0)                             as principal_new,
  nvl(dw.loan_principal,0)                              as principal_dw,
  nvl(ods.loan_principal,0) - nvl(dw.loan_principal,0)  as principal_diff
from (
  select
    product_id,
    loan_active_date,
    should_repay_date,
    sum(loan_init_principal) as loan_principal
  from (
    select
      product_id,
      due_bill_no,
      loan_active_date,
      loan_term,
      should_repay_date,
      lag(should_repay_date,1,'1970-01-01') over(partition by due_bill_no order by loan_term) as lag_should_repay_date,
      loan_init_principal
    from ods${var:db_suffix}.repay_schedule
    where 1 > 0
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
  ) as tmp
  where 1 > 0
    and '${var:ST9}' between date_add(lag_should_repay_date,1) and should_repay_date
  group by product_id,loan_active_date,should_repay_date
) as ods
full join (
  select
    product_id,
    loan_active_date,
    should_repay_date,
    sum(loan_principal) as loan_principal
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
  group by loan_active_date,product_id,should_repay_date
) as dw
on  ods.product_id        = dw.product_id
and ods.loan_active_date  = dw.loan_active_date
and ods.should_repay_date = dw.should_repay_date
where nvl(ods.loan_principal,0) - nvl(dw.loan_principal,0) != 0
order by should_repay_date,active_date,product_id
limit 10
;
