
---封包日期 固定值
set hivevar:pack_date=2020-10-17;
set  hivevar:product_id_list='001601','001602','001603';


--- 汇通资产服务报告 封包时借据的剩余应还本金和剩余应还利息



insert overwrite  table dim_new.dim_ht_bag_asset partition(biz_date='${pack_date}')
select
"CL202011090089" as  project_id,
asset_bill.due_bill_no,
asset_bill.biz_date,
asset_bill.loan_rate,
loan_apply.customer_name                                                                                    as name ,
min(cast(remain_prin as DECIMAL(15,2) ))                                                                     as total_bag_remain_principal,
min(cast(remian_int as DECIMAL(15,2) ))                                                                     as total_bag_remain_interest
from
(
    select
    due_bill_no,
    cast(totle_int as DECIMAL(15,2) ) as loan_rate,
    biz_date
    from ods.ht_report_file where biz_date='${pack_date}'
)asset_bill
left join (
select
due_bill_no,
product_id,
get_json_object(ori_request,'$.borrower.name') as customer_name
from ods_new_s.loan_apply where product_id in ('001602','001601','001603')
)loan_apply on asset_bill.due_bill_no=loan_apply.due_bill_no
left join (
select
*
from  ods.ht_report_file_tmp
)schedule on asset_bill.due_bill_no=schedule.due_bill_no
group by
asset_bill.due_bill_no,
asset_bill.biz_date,
asset_bill.loan_rate,
loan_apply.customer_name
;


