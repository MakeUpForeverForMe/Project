invalidate metadata ods${var:db_suffix}.repay_schedule;
invalidate metadata dw${var:db_suffix}.dw_loan_base_stat_overdue_num_day;
-- ods 应还 与 dw 逾期 对比 未出账本金
--set var:ST9=2020-12-02;
--set var:db_suffix=;set var:tb_suffix=_asset;
select
  'ods 应还 与 dw 逾期 对比 未出账本金'               as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                 as cps,
  '${var:ST9}'                                                  as biz_date,
  ods.product_id                                                as product_id,
  
  nvl(ods.unposted_principal,0)                                 as principal_new,
  nvl(dw.unposted_principal,0)                                  as principal_dws,
  nvl(ods.unposted_principal,0) - nvl(dw.unposted_principal,0)  as diff_principal
  
from (
  select
    sum(should_repay_principal) as unposted_principal,
    product_id
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
    and schedule_status = 'N'
  group by product_id
) as ods
full join (
  select
    sum(unposted_principal) as unposted_principal,
    product_id
  from dw${var:db_suffix}.dw_loan_base_stat_overdue_num_day
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
where ods.unposted_principal - nvl(dw.unposted_principal,0) != 0
order by product_id
;

