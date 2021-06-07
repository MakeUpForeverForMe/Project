invalidate metadata ods${var:db_suffix}.loan_lending;
invalidate metadata dw${var:db_suffix}.dw_loan_base_stat_loan_num_day;

-- ods 放款 与 dw 放款 对比 累计借据数、累计放款金额
select
  'ods 放款 与 dw 放款 对比 累计借据数、累计放款金额'            as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                  as cps,
  '${var:ST9}'                                                   as biz_date,
                                                                 
  nvl(ods.loan_num,0)                                            as num_new,
  nvl(dw.loan_num,0)                                             as num_dw,
  nvl(ods.loan_num,0) - nvl(dw.loan_num,0)                       as num_diff2,
  
  nvl(ods.loan_original_principal,0)                             as principal_new,
  nvl(dw.loan_principal,0)                                       as principal_dw,
  nvl(ods.loan_original_principal,0) - nvl(dw.loan_principal,0)  as principal_diff2,
  
  nvl(dw.product_id,ods.product_id)                              as product_id
from (
  select
    count(due_bill_no)           as loan_num,
    sum(loan_original_principal) as loan_original_principal,
    product_id                   as product_id
  from ods${var:db_suffix}.loan_lending
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402',
      ''
    )
  group by product_id
) as ods
full join (
  select
    sum(loan_num_count)       as loan_num,
    sum(loan_principal_count) as loan_principal,
    product_id
  from dw${var:db_suffix}.dw_loan_base_stat_loan_num_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402',
      ''
    )
  group by product_id
) as dw
on ods.product_id = dw.product_id
where nvl(ods.loan_num,0) - nvl(dw.loan_num,0)                != 0 or 
nvl(ods.loan_original_principal,0) - nvl(dw.loan_principal,0) != 0
order by product_id
limit 10
;

