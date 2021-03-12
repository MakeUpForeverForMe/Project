select
  nvl(loan_principal.due_bill_no,unposted_principal.due_bill_no)                      as due_bill_no,
  nvl(loan_init_principal,0)                                                          as init_principal,
  nvl(loan_init_principal,0) - (nvl(repaid_principal,0) + nvl(remain_principal,0))    as init_diff,
  nvl(repaid_principal,   0) + nvl(remain_principal,0)                                as init_sum,
  nvl(repaid_principal,   0)                                                          as repaid_principal,
  nvl(remain_principal,   0)                                                          as remain_principal,
  nvl(remain_principal,   0) - (nvl(overdue_principal,0) + nvl(unposted_principal,0)) as remain_diff,
  nvl(overdue_principal,  0) + nvl(unposted_principal,0)                              as remain_sum,
  nvl(overdue_principal,  0)                                                          as overdue,
  nvl(unposted_principal, 0)                                                          as unposted,
  '${var:ST9}'                                                                        as biz_date,
  nvl(loan_principal.product_id,unposted_principal.product_id)                        as product_id
from (
  -- 累计放款本金
  select
    due_bill_no,
    sum(loan_init_principal) as loan_init_principal,
    sum(paid_principal)      as repaid_principal,
    sum(remain_principal)    as remain_principal,
    sum(overdue_principal)   as overdue_principal,
    product_id
  from ods_new_s.loan_info
  where 1 > 0
    and product_id in ('DIDI201908161538')
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and loan_active_date<='${var:ST9}'
  group by product_id
  ,due_bill_no
) as loan_principal
left join (
  -- 未出账本金
  select
    due_bill_no,
    sum(should_repay_principal - paid_principal) as unposted_principal,
    product_id
  from ods_new_s.repay_schedule
  where 1 > 0
    and product_id in ('DIDI201908161538')
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and schedule_status = 'N'
  group by product_id
  ,due_bill_no
) as unposted_principal
on  loan_principal.product_id  = unposted_principal.product_id
and loan_principal.due_bill_no = unposted_principal.due_bill_no
having nvl(loan_init_principal,0) - (nvl(repaid_principal,0) + nvl(remain_principal,0)) != 0 or nvl(remain_principal,0) - (nvl(overdue_principal,0) + nvl(unposted_principal,0)) != 0
limit 200
;

