invalidate metadata ods_new_s${var:db_suffix}.loan_lending;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_loan_num_day;

-- ods_new_s 放款 与 dw_new 放款 对比 累计借据数、累计放款金额
select
  'ods_new_s 放款 与 dw_new 放款 对比 累计借据数、累计放款金额' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  '${var:ST9}'                                          as biz_date,
  nvl(new.loan_num,0)                                   as num_new,
  nvl(dws.loan_num,0)                                   as num_dw,
  nvl(new.loan_num,0) - nvl(dws.loan_num,0)             as num_diff2,
  nvl(new.loan_principal,0)                             as principal_new,
  nvl(dws.loan_principal,0)                             as principal_dw,
  nvl(new.loan_principal,0) - nvl(dws.loan_principal,0) as principal_diff2,
  nvl(dws.product_id,new.product_id)                    as product_id
from (
  select
    count(due_bill_no)       as loan_num,
    sum(loan_init_principal) as loan_principal,
    product_id               as product_id
  from ods_new_s${var:db_suffix}.loan_lending
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
) as new
full join (
  select
    sum(loan_num_count)       as loan_num,
    sum(loan_principal_count) as loan_principal,
    product_id
  from dw_new${var:db_suffix}.dw_loan_base_stat_loan_num_day
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
) as dws
on new.product_id = dws.product_id
where nvl(new.loan_num,0) - nvl(dws.loan_num,0) != 0 or nvl(new.loan_principal,0) - nvl(dws.loan_principal,0) != 0
order by product_id
limit 10
;

