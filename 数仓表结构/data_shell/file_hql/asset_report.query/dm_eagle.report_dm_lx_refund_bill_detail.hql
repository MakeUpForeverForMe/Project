
insert overwrite table dm_eagle.report_dm_lx_refund_bill_detail
select
b.project_id,
a.bill_no,
a.loan_date,
a.contact_amount,
a.contact_deadline,
a.refund_date
from
(select
a.due_bill_no as bill_no,
a.biz_date as loan_date,
cast(a.loan_init_principal as decimal(12,2))as contact_amount,
a.loan_init_term as contact_deadline,
b.business_date  as refund_date
from 
(select due_bill_no,loan_init_principal,biz_date,loan_init_term from ods_new_s_cps.loan_lending where biz_date<='${var:ST9}' and product_id in (${product_id})) a 
join(select due_bill_no,business_date from ods_new_s_cps.order_info where biz_date<='${var:ST9}' and product_id in (${product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no) a
left join
    (
            select *
            from dim_new.biz_conf biz
            where product_id in (${var:product_id})
    ) b
on a.product_id = b.product_id;
