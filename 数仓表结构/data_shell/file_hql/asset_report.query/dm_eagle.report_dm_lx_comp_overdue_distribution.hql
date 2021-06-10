
insert overwrite table dm_eagle.report_dm_lx_comp_overdue_distribution partition(snapshot_date,project_id)
select
cast(sum(normal_prin) as DECIMAL(12,2)
cast(sum(overdue_prin1) as DECIMAL(12,2),
cast(sum(overdue_prin31) as DECIMAL(12,2),
cast(sum(overdue_prin61) as DECIMAL(12,2),
cast(sum(overdue_prin91) as DECIMAL(12,2),
cast(sum(overdue_prin121) as DECIMAL(12,2),
cast(sum(overdue_prin151) as DECIMAL(12,2),
cast(sum(overdue_prin181) as DECIMAL(12,2),
snapshot_date,
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
a.snapshot_date
a.product_id,
from
(select sum(loan_init_principal) as all_prin,product_id,'${var:ST9}' as snapshot_date from ods_new_s.loan_lending where product_id in (${product_id}) and biz_date='${var:ST9}' group by product_id) a
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
(select * from ods_new_s.loan_info where s_d_date<= date_sub('${var:ST9}',1) and e_d_date > date_sub('${var:ST9}',1) and product_id in (${product_id}) and ifnull(paid_out_type,'') != 'BUY_BACK') a
left join(select due_bill_no,sum(repay_amount) as paid_principal from ods_new_s.repay_detail where biz_date<= date_sub('${var:ST9}',1) and product_id in (${product_id}) and bnp_type='Pricinpal' group by due_bill_no) b
on a.due_bill_no =b.due_bill_no
group by a.product_id
) b
on a.snapshot_date=b.snapshot_date and a.product_id=b.product_id) a
left join
    (
            select *
            from dim_new.biz_conf biz
            where product_id in (${var:product_id})
    ) b
on a.product_id = b.product_id
group by snapshot_date,project_id;