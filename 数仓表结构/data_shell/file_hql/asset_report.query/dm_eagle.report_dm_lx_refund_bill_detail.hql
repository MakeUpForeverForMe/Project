set var:product_id='001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';


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
a.product_id,
a.due_bill_no as bill_no,
a.loan_active_date as loan_date,
cast(a.loan_init_principal as decimal(12,2))as contact_amount,
a.loan_init_term as contact_deadline,
b.business_date  as refund_date
from 
(select product_id,due_bill_no,loan_init_principal,loan_active_date,loan_init_term from ods_cps.loan_info  where '${var:ST9}' between s_d_date and date_sub(e_d_date,1)  and product_id in (${var:product_id})) a
join(select due_bill_no,business_date from ods_cps.order_info where biz_date<='${var:ST9}' and product_id in (${var:product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no
) a
left join
    (
      select distinct
       project_id,
       product_id
       from (
         select
           max(if(col_name = 'project_id',   col_val,null)) as project_id,
           max(if(col_name = 'product_id',   col_val,null)) as product_id
         from dim.data_conf
         where col_type = 'ac'
         group by col_id
    )tmp where product_id in (${var:product_id})
    ) b
on a.product_id = b.product_id;
