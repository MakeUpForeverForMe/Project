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
      and project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
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
      and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
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
    and project_id in ('WS0006200001','WS0006200002','WS0006200003','WS0009200001')
  group by product_id,biz_date
) as dm
on  loan_amount.product_id = dm.product_id
and loan_amount.biz_date   = dm.biz_date
where nvl(loan_amount.loan_principal,0) - nvl(dm.loan_principal,0) != 0
order by biz_date,product_id;

