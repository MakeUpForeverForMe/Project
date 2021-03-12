-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;





insert overwrite table dm_eagle.abs_early_payment_asset_statistic partition(biz_date='${ST9}',project_id,bag_id)
select
    'Y'                                                                                                   as is_allBag,
    sum(if(cur_pre_settle.bnp_type = 'Pricinpal',cur_pre_settle.repay_amount,0))   as early_payment_principal,
    count(distinct cur_pre_settle.due_bill_no)                            as early_payment_asset_count,
    count(distinct cur_pre_settle.user_hash_no)                           as early_payment_cust_count,
    sum(if(accu_pre_settle.bnp_type = 'Pricinpal',accu_pre_settle.repay_amount,0)) as early_payment_principal_accu,
    count(distinct accu_pre_settle.due_bill_no)                          as early_payment_asset_count_accu,
    count(distinct accu_pre_settle.user_hash_no)                         as early_payment_cust_count_accu,
    sum(unsettle.unsettled_remain_principal)                                                                as remain_principal,
    count(unsettle.due_bill_no)                                                                             as asset_count,
    count(distinct unsettle.user_hash_no)                                                                   as cust_count,
    min(bag_info.bag_date)                                                                                  as bag_date,
    sum(bag_info.bag_remain_principal)                                                                      as package_remain_principal,
    count(bag_total.due_bill_no)                                                                            as package_asset_count,
    count(distinct bag_total.user_hash_no)                                                                  as package_cust_count,
    pro_due.project_id                                                                                  as project_id,
    'default_all_bag'                                                                                   as bag_id
from dim_new.project_due_bill_no pro_due
inner join dim_new.bag_due_bill_no bag_due
on pro_due.due_bill_no = bag_due.due_bill_no
inner join dim_new.bag_info bag_info
on bag_due.bag_id = bag_info.bag_id
inner join
(
    select
        total.due_bill_no,
        cust.user_hash_no
    from
    (
        select due_bill_no
        from ods_new_s.loan_info_cloud
        where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ) total
    left join ods_new_s.customer_info_cloud cust
    on total.due_bill_no = cust.due_bill_no
) bag_total
on pro_due.due_bill_no = bag_total.due_bill_no
left join
(
    select
        unsettled_loan.due_bill_no,
        unsettled_loan.unsettled_remain_principal,
        cust.user_hash_no
    from
    (
        select due_bill_no,sum(remain_principal) as unsettled_remain_principal
        from ods_new_s.loan_info_cloud
        where '${ST9}' between s_d_date and date_sub(e_d_date,1)
        and loan_status<>'F'
        group by due_bill_no
    ) unsettled_loan
    left join ods_new_s.customer_info_cloud cust
    on unsettled_loan.due_bill_no = cust.due_bill_no
) unsettle
on pro_due.due_bill_no = unsettle.due_bill_no
left join
(
    select
        repay_detail.due_bill_no,
        repay_detail.biz_date,
        repay_detail.bnp_type,
        repay_detail.repay_amount,
        settle_date.pre_settle_date,
        cust.user_hash_no
    from
    (
        select due_bill_no,bnp_type,repay_amount,biz_date
        from ods_new_s.repay_detail_cloud
        where biz_date = '${ST9}'
    ) repay_detail
    inner join
    (
        select
            loan_tmp.due_bill_no,
            repay_tmp.pre_settle_date
        from
        (
            select due_bill_no
            from ods_new_s.loan_info_cloud
            where '${ST9}' between s_d_date and date_sub(e_d_date,1)
            and loan_status='F' and paid_out_type='PRE_SETTLE'
        ) loan_tmp
        inner join
        (
            select due_bill_no,max(biz_date) as pre_settle_date
            from ods_new_s.repay_detail_cloud
            where biz_date <= '${ST9}'
            group by due_bill_no
        ) repay_tmp
        on loan_tmp.due_bill_no = repay_tmp.due_bill_no
    ) settle_date
    on repay_detail.due_bill_no = settle_date.due_bill_no
    left join ods_new_s.customer_info_cloud cust
    on repay_detail.due_bill_no = cust.due_bill_no
) cur_pre_settle
on pro_due.due_bill_no = cur_pre_settle.due_bill_no
left join
(
    select
        repay_detail.due_bill_no,
        repay_detail.biz_date,
        repay_detail.bnp_type,
        repay_detail.repay_amount,
        settle_date.pre_settle_date,
        cust.user_hash_no
    from
    (
        select due_bill_no,bnp_type,repay_amount,biz_date
        from ods_new_s.repay_detail_cloud
        where biz_date <= '${ST9}'
    ) repay_detail
    inner join
    (
        select
            loan_tmp.due_bill_no,
            repay_tmp.pre_settle_date
        from
        (
            select due_bill_no
            from ods_new_s.loan_info_cloud
            where '${ST9}' between s_d_date and date_sub(e_d_date,1)
            and loan_status='F' and paid_out_type='PRE_SETTLE'
        ) loan_tmp
        inner join
        (
            select due_bill_no,max(biz_date) as pre_settle_date
            from ods_new_s.repay_detail_cloud
            where biz_date <= '${ST9}'
            group by due_bill_no
        ) repay_tmp
        on loan_tmp.due_bill_no = repay_tmp.due_bill_no
    ) settle_date
    on repay_detail.due_bill_no = settle_date.due_bill_no
    left join ods_new_s.customer_info_cloud cust
    on repay_detail.due_bill_no = cust.due_bill_no
) accu_pre_settle
on pro_due.due_bill_no = accu_pre_settle.due_bill_no
group by pro_due.project_id
union all
select
    'N'                                                                                                  as is_allBag,
    sum(if(cur_pre_settle.bnp_type = 'Pricinpal',cur_pre_settle.repay_amount,0))   as early_payment_principal,
    count(distinct cur_pre_settle.due_bill_no)                            as early_payment_asset_count,
    count(distinct cur_pre_settle.user_hash_no)                           as early_payment_cust_count,
    sum(if(accu_pre_settle.bnp_type = 'Pricinpal',accu_pre_settle.repay_amount,0)) as early_payment_principal_accu,
    count(distinct accu_pre_settle.due_bill_no)                          as early_payment_asset_count_accu,
    count(distinct accu_pre_settle.user_hash_no)                         as early_payment_cust_count_accu,
    sum(unsettle.unsettled_remain_principal)                                                                as remain_principal,
    count(unsettle.due_bill_no)                                                                             as asset_count,
    count(distinct unsettle.user_hash_no)                                                                   as cust_count,
    bag_info.bag_date                                                                                       as bag_date,
    cast(bag_info.bag_remain_principal as decimal(15,4))                                                    as package_remain_principal,
    count(bag_total.due_bill_no)                                                                            as package_asset_count,
    count(distinct bag_total.user_hash_no)                                                                  as package_cust_count,
    pro_due.project_id                                                                                      as project_id,
    bag_due.bag_id                                                                                          as bag_id
from dim_new.project_due_bill_no pro_due
inner join dim_new.bag_due_bill_no bag_due
on pro_due.due_bill_no = bag_due.due_bill_no
inner join dim_new.bag_info bag_info
on bag_due.bag_id = bag_info.bag_id
inner join
(
    select
        total.due_bill_no,
        cust.user_hash_no
    from
    (
        select due_bill_no
        from ods_new_s.loan_info_cloud
        where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ) total
    left join ods_new_s.customer_info_cloud cust
    on total.due_bill_no = cust.due_bill_no
) bag_total
on pro_due.due_bill_no = bag_total.due_bill_no
left join
(
    select
        unsettled_loan.due_bill_no,
        unsettled_loan.unsettled_remain_principal,
        cust.user_hash_no
    from
    (
        select due_bill_no,sum(remain_principal) as unsettled_remain_principal
        from ods_new_s.loan_info_cloud
        where '${ST9}' between s_d_date and date_sub(e_d_date,1)
        and loan_status<>'F'
        group by due_bill_no
    ) unsettled_loan
    left join ods_new_s.customer_info_cloud cust
    on unsettled_loan.due_bill_no = cust.due_bill_no
) unsettle
on pro_due.due_bill_no = unsettle.due_bill_no
left join
(
    select
        repay_detail.due_bill_no,
        repay_detail.biz_date,
        repay_detail.bnp_type,
        repay_detail.repay_amount,
        settle_date.pre_settle_date,
        cust.user_hash_no
    from
    (
        select due_bill_no,bnp_type,repay_amount,biz_date
        from ods_new_s.repay_detail_cloud
        where biz_date = '${ST9}'
    ) repay_detail
    inner join
    (
        select
            loan_tmp.due_bill_no,
            repay_tmp.pre_settle_date
        from
        (
            select due_bill_no
            from ods_new_s.loan_info_cloud
            where '${ST9}' between s_d_date and date_sub(e_d_date,1)
            and loan_status='F' and paid_out_type='PRE_SETTLE'
        ) loan_tmp
        inner join
        (
            select due_bill_no,max(biz_date) as pre_settle_date
            from ods_new_s.repay_detail_cloud
            where biz_date <= '${ST9}'
            group by due_bill_no
        ) repay_tmp
        on loan_tmp.due_bill_no = repay_tmp.due_bill_no
    ) settle_date
    on repay_detail.due_bill_no = settle_date.due_bill_no
    left join ods_new_s.customer_info_cloud cust
    on repay_detail.due_bill_no = cust.due_bill_no
) cur_pre_settle
on pro_due.due_bill_no = cur_pre_settle.due_bill_no
left join
(
    select
        repay_detail.due_bill_no,
        repay_detail.biz_date,
        repay_detail.bnp_type,
        repay_detail.repay_amount,
        settle_date.pre_settle_date,
        cust.user_hash_no
    from
    (
        select due_bill_no,bnp_type,repay_amount,biz_date
        from ods_new_s.repay_detail_cloud
        where biz_date <= '${ST9}'
    ) repay_detail
    inner join
    (
        select
            loan_tmp.due_bill_no,
            repay_tmp.pre_settle_date
        from
        (
            select due_bill_no
            from ods_new_s.loan_info_cloud
            where '${ST9}' between s_d_date and date_sub(e_d_date,1)
            and loan_status='F' and paid_out_type='PRE_SETTLE'
        ) loan_tmp
        inner join
        (
            select due_bill_no,max(biz_date) as pre_settle_date
            from ods_new_s.repay_detail_cloud
            where biz_date <= '${ST9}'
            group by due_bill_no
        ) repay_tmp
        on loan_tmp.due_bill_no = repay_tmp.due_bill_no
    ) settle_date
    on repay_detail.due_bill_no = settle_date.due_bill_no
    left join ods_new_s.customer_info_cloud cust
    on repay_detail.due_bill_no = cust.due_bill_no
) accu_pre_settle
on pro_due.due_bill_no = accu_pre_settle.due_bill_no
group by pro_due.project_id,bag_due.bag_id,bag_info.bag_date,bag_info.bag_remain_principal;
