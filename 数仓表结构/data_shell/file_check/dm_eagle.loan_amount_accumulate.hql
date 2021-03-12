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
  and project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
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
    and project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
  group by product_id
) as dm
on  loan_amount.product_id = dm.product_id
where nvl(loan_amount.loan_principal,0) - nvl(dm.loan_principal,0) != 0
order by biz_date,product_id;

