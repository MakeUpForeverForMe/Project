 --set var:ST9=2020-12-02;

 --set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;


invalidate metadata ods${var:db_suffix}.loan_lending;
invalidate metadata ods${var:db_suffix}.loan_info;
invalidate metadata ods${var:db_suffix}.repay_schedule;
-- ods  放款、借据、应还 借据数、应还金额 对比
--002501 百度医美项目
select
  'ods  放款、借据、应还 借据数、应还金额 对比'                                                as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                                                      as cps,
  coalesce(loan_lending.loan_active_date,loan_info.loan_active_date,repay_schedule.loan_active_date) as active_date,
  coalesce(loan_lending.due_bill_no,loan_info.due_bill_no,repay_schedule.due_bill_no)                as due_bill_no,
  nvl(loan_lending.loan_num,0)                                                                       as num_lengding,
  nvl(loan_info.loan_num,0)                                                                          as num_loan,
  nvl(loan_lending.loan_num,0) - nvl(loan_info.loan_num,0)                                           as num_diff1,
  nvl(repay_schedule.loan_num,0)                                                                     as num_schedule,
  nvl(loan_lending.loan_num,0) - nvl(repay_schedule.loan_num,0)                                      as num_diff2,
  nvl(loan_lending.loan_num_distinct,0)                                                              as num_dis_lengding,
  nvl(loan_info.loan_num_distinct,0)                                                                 as num_dis_loan,
  nvl(loan_lending.loan_num_distinct,0) - nvl(loan_info.loan_num_distinct,0)                         as num_dis_diff1,
  nvl(repay_schedule.loan_num_distinct,0)                                                            as num_dis_schedule,
  nvl(loan_lending.loan_num_distinct,0) - nvl(repay_schedule.loan_num_distinct,0)                    as num_dis_diff2,
  nvl(loan_lending.loan_original_principal,0)                                                        as prin_lengding,
  nvl(loan_info.loan_principal,0)                                                                    as prin_loan,
  nvl(loan_lending.loan_original_principal,0) - nvl(loan_info.loan_principal,0)                      as prin_diff1,
  nvl(repay_schedule.loan_principal,0)                                                               as prin_schedule,
  nvl(loan_lending.loan_original_principal,0) - nvl(repay_schedule.loan_principal,0)                 as prin_diff2,
  nvl(repay_schedule.loan_principal,0) - nvl(loan_info.loan_principal,0)                             as prin_diff3,    --百度医美当前在贷本金(还款计划)-在贷本金(借据)
  coalesce(loan_lending.product_id,loan_info.product_id,repay_schedule.product_id)                   as product_id
from (
  select
    due_bill_no                     as due_bill_no,
    count(due_bill_no)              as loan_num,
    count(distinct due_bill_no)     as loan_num_distinct,
    sum(loan_original_principal)    as loan_original_principal,
    loan_active_date                as loan_active_date,
    product_id                      as product_id
  from ods${var:db_suffix}.loan_lending
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402','002501',
      ''
    )
  group by loan_active_date,product_id
  ,due_bill_no
) as loan_lending
full join (
  select
    due_bill_no                 as due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_principal)    as loan_principal,
    loan_active_date            as loan_active_date,
    product_id                  as product_id
  from ods${var:db_suffix}.loan_info
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402','002501',
      ''
    )
  group by loan_active_date,product_id
  ,due_bill_no
) as loan_info
on  loan_lending.product_id       = loan_info.product_id
and loan_lending.loan_active_date = loan_info.loan_active_date
and loan_lending.due_bill_no      = loan_info.due_bill_no
full join(
  select
    due_bill_no                 as due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_principal)    as loan_principal,
    loan_active_date            as loan_active_date,
    product_id                  as product_id
  from ods${var:db_suffix}.repay_schedule
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402','002501',
      ''
    )
    and loan_term = 1 -- 所有的借据都有第一期，取第一期是取还款计划中每笔借据只取一条数据
  group by loan_active_date,product_id
  ,due_bill_no
) as repay_schedule
on  loan_lending.product_id       = repay_schedule.product_id
and loan_lending.loan_active_date = repay_schedule.loan_active_date
and loan_lending.due_bill_no      = repay_schedule.due_bill_no
having
nvl(loan_lending.loan_num,0)                   - nvl(loan_info.loan_num,0)                                                            != 0 or
nvl(loan_lending.loan_num,0)                   - nvl(repay_schedule.loan_num,0)                                                       != 0 or
nvl(loan_lending.loan_num_distinct,0)          - nvl(loan_info.loan_num_distinct,0)                                                   != 0 or
nvl(loan_lending.loan_num_distinct,0)          - nvl(repay_schedule.loan_num_distinct,0)                                              != 0 or
if(loan_lending.product_id = '002501', 0 ,(nvl(loan_lending.loan_original_principal,0) - nvl(loan_info.loan_principal,0)))            != 0 or
if(loan_lending.product_id = '002501', 0 ,(nvl(loan_lending.loan_original_principal,0) - nvl(repay_schedule.loan_principal,0)))       != 0 or
nvl(loan_info.loan_principal,0) - nvl(repay_schedule.loan_principal,0)                                                                != 0
order by active_date,product_id
limit 10
;
