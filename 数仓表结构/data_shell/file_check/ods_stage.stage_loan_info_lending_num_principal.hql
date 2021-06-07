-- select '${var:ST9}' as biz_date,if('${var:db_suffix}' = '','代偿前','代偿后') as cps;
--set var:ST9=2020-12-02;

--set var:db_suffix=;set var:tb_suffix=_asset;

--set var:db_suffix=_cps;set var:tb_suffix=;
-- 当天放款数、金额对比 stage 与 ods 层        
invalidate metadata stage.ecas_loan${var:tb_suffix};
invalidate metadata ods${var:db_suffix}.loan_info;
select
  'stage 与 ods 放款数、金额 对比'                                                       as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                                          as cps,
  '${var:ST9}'                                                                           as biz_date,
  COALESCE(stage.due_bill_no,ods.due_bill_no,loan_lending.due_bill_no)                   as due_bill_no,
  
  nvl(stage.loan_num,0)                                                                  as num_stage,
  nvl(ods.loan_num,0)                                                                    as num_loan_info,
  nvl(loan_lending.loan_num,0)                                                           as num_lending,
  nvl(stage.loan_num,0) - nvl(ods.loan_num,0)                                            as num_diff1,
  nvl(stage.loan_num,0) - nvl(loan_lending.loan_num,0)                                   as num_diff2,
  
  nvl(stage.loan_num_distinct,0)                                                         as num_stage_distinct,
  nvl(ods.loan_num_distinct,0)                                                           as num_loan_info_distinct,
  nvl(loan_lending.loan_num_distinct,0)                                                  as num_lending_distinct, 
  nvl(stage.loan_num_distinct,0) - nvl(ods.loan_num_distinct,0)                          as num_distinct_diff1,
  nvl(stage.loan_num_distinct,0) - nvl(loan_lending.loan_num_distinct,0)                 as num_distinct_diff2, 
  
  nvl(stage.loan_principal,0)                                                            as principal_stage,
  nvl(ods.loan_principal,0)                                                              as principal_loan_info,
  nvl(loan_lending.loan_original_principal,0)                                            as principal_lending, 
  nvl(stage.loan_principal,0) - nvl(ods.loan_principal,0)                                as principal_diff1,
  nvl(stage.loan_principal,0) - nvl(loan_lending.loan_original_principal,0)              as principal_diff2, 
  
  COALESCE(stage.loan_active_date,ods.loan_active_date,loan_lending.loan_active_date)    as active_date,
  COALESCE(stage.product_id,ods.product_id,loan_lending.product_id)                      as product_id
from (
  select
    due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_prin)         as loan_principal,
    active_date                 as loan_active_date,
    product_code                as product_id
  from stage.ecas_loan${var:tb_suffix}
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
) as stage
full join (
  select
    due_bill_no,
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
        '002401','002402',
        ''
      )
  group by loan_active_date,product_id
  ,due_bill_no
) as ods
on  stage.product_id       = ods.product_id
and stage.loan_active_date = ods.loan_active_date
and stage.due_bill_no      = ods.due_bill_no
full join (
  select
    due_bill_no,
    count(due_bill_no)              as loan_num,
    count(distinct due_bill_no)     as loan_num_distinct,
    sum(loan_original_principal)    as loan_original_principal,
    loan_active_date                as loan_active_date,
    product_id                      as product_id
  from ods${var:db_suffix}.loan_lending
  where 1 > 0
    and product_id in (
        '001801','001802','001803','001804',
        '001901','001902','001903','001904','001905','001906','001907',
        '002001','002002','002003','002004','002005','002006','002007',
        '002401','002402',
        ''
      )
    and biz_date <= '${var:ST9}'
  group by loan_active_date,product_id
  ,due_bill_no
) as loan_lending
on  stage.product_id       = loan_lending.product_id
and stage.loan_active_date = loan_lending.loan_active_date
and stage.due_bill_no      = loan_lending.due_bill_no
having 
nvl(stage.loan_num,0)              - nvl(ods.loan_num,0)                  != 0 or 
nvl(stage.loan_principal,0)        - nvl(ods.loan_principal,0)            != 0 or 
nvl(stage.loan_num_distinct,0)     - nvl(ods.loan_num_distinct,0)         != 0 or

nvl(stage.loan_num,0)              - nvl(loan_lending.loan_num,0)                           != 0 or 
nvl(stage.loan_principal,0)        - nvl(loan_lending.loan_original_principal,0)            != 0 or 
nvl(stage.loan_num_distinct,0)     - nvl(loan_lending.loan_num_distinct,0)                  != 0
order by active_date,product_id
limit 10
;

