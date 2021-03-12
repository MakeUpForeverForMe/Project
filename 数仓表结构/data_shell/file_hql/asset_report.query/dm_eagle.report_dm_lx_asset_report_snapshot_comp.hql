refresh ods_new_s_cps.loan_info;
refresh ods_new_s_cps.repay_schedule;
refresh ods_new_s_cps.order_info;
refresh ods_new_s_cps.repay_detail;

set var:product_id='001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';

upsert into dm_eagle.report_dm_lx_asset_report_snapshot_comp
select
    concat(biz.project_id,'${var:ST9}')							    as id,
    '${var:ST9}'													as snapshot_date,
    biz.project_id												as project_id,
    cast(sum(t1.loan_amount)	         as decimal(15,2))		as loan_amount,
    cast(sum(t2.total_collection_amount) as decimal(15,2))		as total_collection_amount,
    cast(sum(t2.total_collection_prin)   as decimal(15,2))		as total_collection_prin,
    cast(sum(t2.total_collection_inter)	 as decimal(15,2))		as total_collection_inter,
    cast(0	                             as decimal(15,2))		as normal_amount,
    cast(0	                             as decimal(15,2))		as normal_prin,
    cast(0	                             as decimal(15,2))		as normal_inter,
    cast(0	                             as decimal(15,2))		as overdue_amount,
    cast(0	                             as decimal(15,2))		as overdue_prin,
    cast(0	                             as decimal(15,2))		as overdue_inter,
    cast(0	                             as decimal(15,2))		as prepay_amount,
    cast(0	                             as decimal(15,2))		as prepay_prin,
    cast(0                               as decimal(15,2))		as prepay_inter,
    cast(sum(t3.compensation_amount)	 as decimal(15,2))		as compensation_amount,
    cast(sum(t3.compensation_prin)	     as decimal(15,2))	    as compensation_prin,
    cast(sum(t3.compensation_inter)	     as decimal(15,2))	    as compensation_inter,
    cast(sum(t3.repurchase_amount)	     as decimal(15,2))	    as repurchase_amount,
    cast(sum(t3.repurchase_prin)	     as decimal(15,2))	    as repurchase_prin,
    cast(sum(t3.repurchase_inter)	     as decimal(15,2))	    as repurchase_inter,
    cast(sum(t3.refund_contract_amount)  as decimal(15,2))		as refund_contract_amount
from
(
    select * from dim_new.biz_conf
    where product_id in (${var:product_id})
) biz
left join
(
    select
        product_id,
        sum(loan_init_principal)                                as loan_amount
    from ods_new_s_cps.loan_info
    where '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in (${var:product_id})
    and loan_active_date = '${var:ST9}'
    group by product_id
) t1
on biz.product_id = t1.product_id
left join
(
    select
        product_id,
        sum(repay_amount)                                       as total_collection_amount,
        sum(if(bnp_type='Pricinpal',repay_amount,0))            as total_collection_prin,
        sum(if(bnp_type<>'Pricinpal',repay_amount,0))           as total_collection_inter
    from ods_new_s_cps.repay_detail
    where to_date(txn_time) = date_sub('${var:ST9}',1)
    and product_id in (${var:product_id})
    group by product_id
) t2
on biz.product_id = t2.product_id
left join
(
    select
        ord.product_id                                                              as product_id,
        sum(if(ord.loan_usage='D',repay.repay_amount,0))                            as compensation_amount,
        sum(if(ord.loan_usage='D' and bnp_type='Pricinpal',repay.repay_amount,0))   as compensation_prin,
        sum(if(ord.loan_usage='D' and bnp_type<>'Pricinpal',repay.repay_amount,0))  as compensation_inter,
        sum(if(ord.loan_usage='B',repay.repay_amount,0))                            as repurchase_amount,
        sum(if(ord.loan_usage='B' and bnp_type='Pricinpal',repay.repay_amount,0))   as repurchase_prin,
        sum(if(ord.loan_usage='B' and bnp_type<>'Pricinpal',repay.repay_amount,0))  as repurchase_inter,
        sum(if(ord.loan_usage='T',repay.repay_amount,0))                            as refund_contract_amount
    from
    (
        select *
        from ods_new_s_cps.order_info
        where biz_date = date_sub('${var:ST9}',1)
        and product_id in (${var:product_id})
        and loan_usage<>'L' and order_status='S'
    ) ord
    left join
    (
        select *
        from ods_new_s_cps.repay_detail
        where product_id in (${var:product_id})
        and to_date(txn_time) = date_sub('${var:ST9}',1)
    ) repay
    on ord.order_id = repay.order_id
    group by ord.product_id
) t3
on biz.product_id = t3.product_id
group by biz.project_id;
exit;
