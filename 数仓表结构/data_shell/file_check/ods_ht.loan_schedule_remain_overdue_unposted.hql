 --set var:db_suffix=_cps;


 --set var:ST9=2020-12-02;

--set var:db_suffix=;set var:tb_suffix=_asset;
invalidate metadata ods${var:db_suffix}.loan_info;
invalidate metadata ods${var:db_suffix}.repay_schedule;
-- ods  借据、应还 本金余额 = 逾期金额 + 未出账本金
select
  'ods  汇通借据、应还 本金余额 = 逾期金额 + 未出账本金'                                                            as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                                                                       as cps,
  '${var:ST9}'                                                                                                        as biz_date,
  nvl(loan_info.product_id,repay_schedule.product_id)                                                                 as product_id,
  nvl(loan_info.due_bill_no,repay_schedule.due_bill_no)                                                               as due_bill_no,
  loan_info.overdue_days                                                                                              as over_days,
  loan_info.paid_out_type_cn                                                                                          as paid_type_cn,
  nvl(loan_info.remain_principal,0)                                                                                   as remain_prin,
  nvl(loan_info.remain_principal,0) - (nvl(loan_info.overdue_principal,0) + nvl(repay_schedule.unposted_principal,0)) as remain_prin_diff,
  nvl(loan_info.overdue_principal,0) + nvl(repay_schedule.unposted_principal,0)                                       as remain_prin_compute,
  nvl(loan_info.overdue_principal,0)                                                                                  as overdue_prin,
  nvl(repay_schedule.unposted_principal,0)                                                                            as unposted_prin
from (
  select
    product_id             as product_id,
    due_bill_no            as due_bill_no,
    overdue_days           as overdue_days,
    paid_out_type_cn       as paid_out_type_cn,
    sum(remain_principal)  as remain_principal,
    sum(overdue_principal) as overdue_principal
  from ods${var:db_suffix}.loan_info
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in (
      '001601','001602','001603',
      ''
    ) and due_bill_no not in ('1000004836')
  group by product_id,due_bill_no,overdue_days,paid_out_type_cn
) as loan_info
full join (
  select
    product_id                                   as product_id,
    due_bill_no                                  as due_bill_no,
    sum(should_repay_principal - paid_principal) as unposted_principal
  from ods${var:db_suffix}.repay_schedule
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in (
      '001601','001602','001603',
      ''
    )
    and schedule_status = 'N'
    and due_bill_no not in ('1000004836')
  group by product_id,due_bill_no
) as repay_schedule
on  loan_info.product_id  = repay_schedule.product_id
and loan_info.due_bill_no = repay_schedule.due_bill_no
where nvl(loan_info.remain_principal,0) - (nvl(loan_info.overdue_principal,0) + nvl(repay_schedule.unposted_principal,0)) != 0
order by product_id,due_bill_no
limit 20
;
