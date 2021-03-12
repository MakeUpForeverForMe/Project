-- select '${var:ST9}' as biz_date,if('${var:db_suffix}' = '','代偿前','代偿后') as cps;

-- 当天放款数、金额对比 ods 与 ods_new_s 层
invalidate metadata ods.ecas_loan${var:tb_suffix};
invalidate metadata ods_new_s${var:db_suffix}.loan_info;
select
  'ods 与 ods_new_s 放款数、金额 对比'                        as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')               as cps,
  '${var:ST9}'                                                as biz_date,
  nvl(ods.due_bill_no,new.due_bill_no)                        as due_bill_no,
  nvl(ods.loan_num,0)                                         as num_ods,
  nvl(new.loan_num,0)                                         as num_new,
  nvl(ods.loan_num,0) - nvl(new.loan_num,0)                   as num_diff,
  nvl(ods.loan_num_distinct,0)                                as num_ods_distinct,
  nvl(new.loan_num_distinct,0)                                as num_new_distinct,
  nvl(ods.loan_num_distinct,0) - nvl(new.loan_num_distinct,0) as num_distinct_diff,
  nvl(ods.loan_principal,0)                                   as principal_ods,
  nvl(new.loan_principal,0)                                   as principal_new,
  nvl(ods.loan_principal,0) - nvl(new.loan_principal,0)       as principal_diff,
  nvl(ods.loan_active_date,new.loan_active_date)              as active_date,
  nvl(ods.product_id,new.product_id)                          as product_id
from (
  select
    due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_prin)         as loan_principal,
    active_date                 as loan_active_date,
    product_code                as product_id
  from ods.ecas_loan${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and product_code in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
  group by loan_active_date,product_id
  ,due_bill_no
) as ods
full join (
  select
    due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_principal)    as loan_principal,
    loan_active_date            as loan_active_date,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.loan_info
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
  group by loan_active_date,product_id
  ,due_bill_no
) as new
on  ods.product_id       = new.product_id
and ods.loan_active_date = new.loan_active_date
and ods.due_bill_no      = new.due_bill_no
having nvl(ods.loan_num,0) - nvl(new.loan_num,0) != 0 or nvl(ods.loan_principal,0) - nvl(new.loan_principal,0) != 0 or nvl(ods.loan_num_distinct,0) - nvl(new.loan_num_distinct,0) != 0
order by active_date,product_id
limit 10
;

