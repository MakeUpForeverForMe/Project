-- select '${var:ST9}' as biz_date,if('${var:db_suffix}' = '','代偿前','代偿后') as cps;

invalidate metadata stage.ecas_repay_schedule${var:tb_suffix};
invalidate metadata ods${var:db_suffix}.repay_schedule;
-- 应还对比
select
  'ods 应还 借据数 对比'                                      as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')               as cps,
  nvl(stage.due_bill_no,ods.due_bill_no)                      as due_bill_no,
  
  nvl(stage.should_principal,0)                               as should_principal_ods,
  nvl(ods.should_principal,0)                                 as should_principal_new,
  nvl(stage.should_principal,0) - nvl(ods.should_principal,0) as should_principal_diff,
  
  nvl(stage.should_repay_date,ods.should_repay_date)          as should_date,
  nvl(stage.product_id,ods.product_id)                        as product_id
from (
  select
    due_bill_no   as due_bill_no,
    due_term_prin as should_principal,
    pmt_due_date  as should_repay_date,
    product_code  as product_id
  from stage.ecas_repay_schedule${var:tb_suffix}
  where 1 > 0
    and d_date = '${var:ST9}'
    and product_code in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402',
      ''
    )
--    and pmt_due_date = '${var:ST9}'
) as stage
full join (
  select
    due_bill_no            as due_bill_no,
    should_repay_principal as should_principal,
    should_repay_date      as should_repay_date,
    product_id             as product_id
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
--    and should_repay_date = '${var:ST9}'
) as ods
on  stage.product_id        = ods.product_id
and stage.should_repay_date = ods.should_repay_date
and stage.due_bill_no       = ods.due_bill_no
having nvl(stage.should_principal,0) - nvl(ods.should_principal,0) != 0
order by should_date,product_id
limit 10
;

