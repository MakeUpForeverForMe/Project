delete from dm_eagle.report_ht_overdue_more9_assets_detail where snapshot_date='${var:ST9}';
upsert into table dm_eagle.report_ht_overdue_more9_assets_detail
select
 cast(concat(a.project_id,a.due_bill_no,a.biz_date,'${var:ST9}') as string)                                                   as id,
 a.project_id                                                                                                                 as project_id,
 a.due_bill_no                                                                                                                as due_bill_no,
 a.name                                                                                                                       as borrower_name,
 b.overdue_date_start                                                                                                         as overdue_start_date,
 cast((nvl(overdue_principal,0)+nvl(overdue_interest,0)) as decimal(15,4))                                                    as overdue_amount,
 '${var:ST9}'                                                                                                                 as snapshot_date
from
  (select
   project_id,
   biz_date,
   due_bill_no,
   name
  from dim_new.dim_ht_bag_asset) a
  join
  (select
    due_bill_no,
    overdue_date_start,
    overdue_principal,
    overdue_interest
   from ods_new_s.loan_info where '${var:ST9}' between s_d_date and date_sub(e_d_date,1) and product_id in ('001601','001602','001603') and overdue_days>9) b
  on a.due_bill_no=b.due_bill_no
