-- select '${var:ST9}' as biz_date,if('${var:db_suffix}' = '','代偿前','代偿后') as cps;
 --set var:ST9=2020-12-02;

--set var:db_suffix=;set var:tb_suffix=_asset;
invalidate metadata ods${var:db_suffix}.loan_info;
invalidate metadata ods${var:db_suffix}.repay_detail;
-- ods  借据、实还 借据数、已还金额 对比
select
  'ods 汇通 借据、实还 借据数、已还金额 对比'                                  as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                            as cps,
    '${var:ST9}'                                                                       as biz_date,
  nvl(loan_info.due_bill_no,repay_detail.due_bill_no)                      as due_bill_no,
  nvl(loan_info.repaid_num,0)                                              as repay_num_loan,
  nvl(repay_detail.repaid_num,0)                                           as repay_num_detail,
  nvl(loan_info.repaid_num,0) - nvl(repay_detail.repaid_num,0)             as repay_num,
  nvl(loan_info.repaid_principal,0)                                        as repay_principal_loan,
  nvl(repay_detail.repaid_principal,0)                                     as repay_principal_detail,
  nvl(loan_info.repaid_principal,0) - nvl(repay_detail.repaid_principal,0) as repay_principal,
  nvl(loan_info.product_id,repay_detail.product_id)                        as product_id
from (
  select
    due_bill_no                 as due_bill_no,
    count(distinct due_bill_no) as repaid_num,
    sum(paid_principal)         as repaid_principal,
    product_id                  as product_id
  from ods${var:db_suffix}.loan_info
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in (
     '001601','001602','001603',
      ''
    )
    and paid_principal != 0
  group by product_id
  ,due_bill_no
) as loan_info
full join (
  select
    due_bill_no                 as due_bill_no,
    count(distinct due_bill_no) as repaid_num,
    sum(repay_amount)           as repaid_principal,
    product_id                  as product_id
  from ods${var:db_suffix}.repay_detail
  where 1 > 0
    and biz_date <= '${var:ST9}'
   -- and to_date(txn_time) <= biz_date 汇通的实还交易日期 可能会在bizdate 之后
    and product_id in (
      '001601','001602','001603',
      ''
    )
    and bnp_type = 'Pricinpal'
  group by product_id
  ,due_bill_no
) as repay_detail
on  loan_info.product_id  = repay_detail.product_id
and loan_info.due_bill_no = repay_detail.due_bill_no
having
nvl(loan_info.repaid_num,0)       - nvl(repay_detail.repaid_num,0)       != 0 or
nvl(loan_info.repaid_principal,0) - nvl(repay_detail.repaid_principal,0) != 0
order by product_id,due_bill_no
limit 10
;

