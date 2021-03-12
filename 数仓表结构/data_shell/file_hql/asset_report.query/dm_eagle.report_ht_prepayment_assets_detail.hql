delete from dm_eagle.report_ht_prepayment_assets_detail ;
upsert into table dm_eagle.report_ht_prepayment_assets_detail
select
  cast(concat(a.project_id,a.due_bill_no,a.biz_date) as string)                                          as id,
  a.project_id                                                                                           as project_id,
  a.due_bill_no                                                                                          as due_bill_no,
  a.name                                                                                                 as borrower_name,
  min(should_repay_date)                                                                                 as should_pay_date,
  cast(sum(should_repay_amount) as decimal(15,4))                                                        as should_pay_amount,
  a.paid_out_date                                                                                        as advance_paid_out_date,
  cast(sum(paid_amount) as decimal(15,4))                                                                as repayment_amount
from
(select
  a.project_id,
  a.biz_date,
  a.due_bill_no,
  a.name,
  b.paid_out_date
 from
  (select
    project_id,
    biz_date,
    due_bill_no,
    name
   from dim_new.dim_ht_bag_asset) a
   join
   (select
     a.due_bill_no,
     a.paid_out_date
    from
  (select
    due_bill_no,
    max(paid_out_date) paid_out_date
   from ods_new_s.repay_schedule where '${var:ST9}' between s_d_date and date_sub(e_d_date,1) and product_id in ('001601','001602','001603') and paid_out_date is not null and paid_out_type not in ('REDEMPTION') group by due_bill_no) a
   left join
   (select
     due_bill_no,
     max(should_repay_date) should_repay_date
    from ods_new_s.repay_schedule where '${var:ST9}' between s_d_date and date_sub(e_d_date,1) and product_id in ('001601','001602','001603') and paid_out_date is not null and paid_out_type not in ('REDEMPTION')  group by due_bill_no) b
    on a.due_bill_no=b.due_bill_no
    where a.paid_out_date<b.should_repay_date) b
 on a.due_bill_no=b.due_bill_no) a
left join
(select
  due_bill_no,
  paid_out_date,
  should_repay_date,
  should_repay_amount,
  paid_amount
from ods_new_s.repay_schedule where '${var:ST9}' between s_d_date and date_sub(e_d_date,1) and product_id in ('001601','001602','001603') and loan_term!=0 and paid_out_type not in ('REDEMPTION') ) b
on a.due_bill_no=b.due_bill_no and a.paid_out_date=b.paid_out_date
group by a.project_id,
  a.due_bill_no,
  a.name,
  a.paid_out_date,
  a.biz_date