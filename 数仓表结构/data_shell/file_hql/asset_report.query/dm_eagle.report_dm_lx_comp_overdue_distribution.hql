
set var:product_id='001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';


insert overwrite table dm_eagle.report_dm_lx_comp_overdue_distribution partition(snapshot_date,project_id)
select
cast(sum(normal_prin) as DECIMAL(12,2)),
cast(sum(overdue_prin1) as DECIMAL(12,2)),
cast(sum(overdue_prin31) as DECIMAL(12,2)),
cast(sum(overdue_prin61) as DECIMAL(12,2)),
cast(sum(overdue_prin91) as DECIMAL(12,2)),
cast(sum(overdue_prin121) as DECIMAL(12,2)),
cast(sum(overdue_prin151) as DECIMAL(12,2)),
cast(sum(overdue_prin181) as DECIMAL(12,2)),
a.snapshot_date,
b.project_id
from
(select
cast(if(a.all_prin is null,0,a.all_prin)+if(b.last_prin is null,0,b.last_prin) as DECIMAL(12,2)) as normal_prin,
cast(if(b.overdue_prin1 is null,0,b.overdue_prin1) as DECIMAL(12,2)) as overdue_prin1,
cast(if(b.overdue_prin31 is null,0,b.overdue_prin31) as DECIMAL(12,2)) as overdue_prin31,
cast(if(b.overdue_prin61 is null,0,b.overdue_prin61) as DECIMAL(12,2)) as overdue_prin61,
cast(if(b.overdue_prin91 is null,0,b.overdue_prin91) as DECIMAL(12,2)) as overdue_prin91,
cast(if(b.overdue_prin121 is null,0,b.overdue_prin121) as DECIMAL(12,2)) as overdue_prin121,
cast(if(b.overdue_prin151 is null,0,b.overdue_prin151) as DECIMAL(12,2)) as overdue_prin151,
cast(if(b.overdue_prin181 is null,0,b.overdue_prin181) as DECIMAL(12,2)) as overdue_prin181,
a.snapshot_date,
a.product_id
from
(select sum(loan_init_principal) as all_prin,product_id,'${var:ST9}' as snapshot_date from ods.loan_info where product_id in (${var:product_id}) and "${var:ST9}" between  s_d_date and date_sub(e_d_date,1) and loan_active_date='${var:ST9}' group by product_id) a
left join
(select 
sum(if(a.overdue_days = 0 and a.loan_status='N',a.loan_init_principal-if(b.paid_principal is null,0,b.paid_principal),0)) as last_prin,
sum(if(a.overdue_days>0 and a.overdue_days<31,a.loan_init_principal-if(b.paid_principal is null,0,b.paid_principal),0)) as overdue_prin1,
sum(if(a.overdue_days>30 and a.overdue_days<61,a.loan_init_principal-if(b.paid_principal is null,0,b.paid_principal),0)) as overdue_prin31,
sum(if(a.overdue_days>60 and a.overdue_days<91,a.loan_init_principal-if(b.paid_principal is null,0,b.paid_principal),0)) as overdue_prin61,
sum(if(a.overdue_days>90 and a.overdue_days<121,a.loan_init_principal-if(b.paid_principal is null,0,b.paid_principal),0)) as overdue_prin91,
sum(if(a.overdue_days>120 and a.overdue_days<151,a.loan_init_principal-if(b.paid_principal is null,0,b.paid_principal),0)) as overdue_prin121,
sum(if(a.overdue_days>150 and a.overdue_days<181,a.loan_init_principal-if(b.paid_principal is null,0,b.paid_principal),0)) as overdue_prin151,
sum(if(a.overdue_days>180,a.loan_init_principal-if(b.paid_principal is null,0,b.paid_principal),0)) as overdue_prin181,
a.product_id,
'${var:ST9}' as snapshot_date from 
(select * from ods.loan_info where s_d_date<= date_sub('${var:ST9}',1) and e_d_date > date_sub('${var:ST9}',1) and product_id in (${var:product_id}) and ifnull(paid_out_type,'') != 'BUY_BACK') a
left join(select due_bill_no,sum(repay_amount) as paid_principal from ods.repay_detail where biz_date<= date_sub('${var:ST9}',1) and product_id in (${var:product_id}) and bnp_type='Pricinpal' group by due_bill_no) b
on a.due_bill_no =b.due_bill_no
group by a.product_id
) b
on a.snapshot_date=b.snapshot_date and a.product_id=b.product_id) a
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
on a.product_id = b.product_id
group by snapshot_date,project_id;