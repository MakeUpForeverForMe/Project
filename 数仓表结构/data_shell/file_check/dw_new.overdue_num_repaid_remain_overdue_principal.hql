invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day;

-- ods_new_s 借据 与 dw_new 逾期 对比 本金余额、已还本金、逾期本金
select
  'ods_new_s 借据 与 dw_new 逾期 对比 本金余额、已还本金、逾期本金' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                     as cps,
  '${var:ST9}'                                                      as biz_date,
  nvl(new.product_id,dws.product_id)                                as product_id,
  nvl(new.repaid_principal,0)                                       as repaid_ods,
  nvl(dws.repaid_principal,0)                                       as repaid_dws,
  nvl(new.repaid_principal,0)  - nvl(dws.repaid_principal,0)        as diff_repaid,
  nvl(new.remain_principal,0)                                       as remain_ods,
  nvl(dws.remain_principal,0)                                       as remain_dws,
  nvl(new.remain_principal,0)  - nvl(dws.remain_principal,0)        as diff_remain,
  nvl(new.overdue_principal,0)                                      as overdue_ods,
  nvl(dws.overdue_principal,0)                                      as overdue_dws,
  nvl(new.overdue_principal,0) - nvl(dws.overdue_principal,0)       as diff_overdue
from (
  select
    product_id             as product_id,
    sum(paid_principal)    as repaid_principal,
    sum(remain_principal)  as remain_principal,
    sum(overdue_principal) as overdue_principal
  from ods_new_s${var:db_suffix}.loan_info
  where 1 > 0
    and product_id in (
      '001801','001802','001803','001804',
      '001901','001902','001903','001904','001905','001906','001907',
      '002001','002002','002003','002004','002005','002006','002007',
      '002401','002402',
      ''
    )
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
  group by product_id
  -- order by product_id
) as new
full join (
  select
    product_id             as product_id,
    sum(paid_principal)    as repaid_principal,
    sum(remain_principal)  as remain_principal,
    sum(overdue_principal) as overdue_principal
  from dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day
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
  -- order by product_id
) as dws
on new.product_id = dws.product_id
where
nvl(new.repaid_principal,0)  - nvl(dws.repaid_principal,0)  != 0 or
nvl(new.remain_principal,0)  - nvl(dws.remain_principal,0)  != 0 or
nvl(new.overdue_principal,0) - nvl(dws.overdue_principal,0) != 0
order by product_id
;

