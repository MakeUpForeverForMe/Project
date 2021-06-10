set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=64000000;      -- 64M
set hive.merge.smallfiles.avgsize=64000000; -- 64M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;

set hivevar:pack_date=2020-10-17;
set hivevar:product_id='001601','001602','001603';


insert overwrite table dm_eagle.report_ht_asset_project partition (snapshot_date,project_id)
select
cast(res.total_remain_principal as decimal(15,2))                                                                                   as total_remain_principal,
cast(res.un_settle_num as bigint)                                                                                                   as un_settle_num,
cast(res.weight_avg_inter_rate as decimal(15,4) )                                                                                   as weight_avg_inter_rate,
cast(res.weight_remain_term  as decimal(15,4))                                                                                      as weight_remain_term,

cast(res.current_repay_amount as decimal(15,2))                                                                                     as current_repay_amount,
cast(res.current_repay_bill_num as bigint)                                                                                          as current_repay_bill_num,
cast(res.current_in_plan_repay_amount as decimal(15,2))                                                                             as current_in_plan_repay_amount,
cast(res.current_in_plan_repay_num as bigint)                                                                                       as current_in_plan_repay_num,
cast(res.current_early_settle_amount as decimal(15,2) )                                                                             as current_early_settle_amount,
cast(res.currnt_early_settle_num as bigint)                                                                                         as currnt_early_settle_num,
cast(res.current_back_amount as decimal(15,2))                                                                                      as current_back_amount,
cast(res.current_back_num as bigint)                                                                                                as current_back_num,

cast(res.total_bill_num as bigint)                                                                                                  as total_bill_num,
cast(res.total_amount  as decimal(15,2))                                                                                            as total_amount,
cast(res.normal_total_num as bigint)                                                                                                as normal_total_num,

cast(res.normal_num_rate as decimal(15,4))                                                                                          as normal_num_rate,
cast(res.normal_total_amount as decimal(15,2))                                                                                      as normal_total_amount,
cast(res.normal_amount_rate as decimal(15,4))                                                                                       as normal_amount_rate,

cast(res.overdue1to30_num_count as bigint)                                                                                          as overdue1to30_num_count,
cast(res.overdue1to30_num_rate as decimal(15,4))                                                                                    as overdue1to30_num_rate,
cast(res.overdue1to30_amount as decimal(15,2))                                                                                      as overdue1to30_amount,
cast(res.overdue1to30_amount_rate as decimal(15,4))                                                                                 as overdue1to30_amount_rate,

cast(res.overdue31to60_num_count as bigint)                                                                                         as overdue31to60_num_count,
cast(res.overdue31to60_num_rate as decimal(15,4))                                                                                   as overdue31to60_num_rate,
cast(res.overdue31to60_amount as decimal(15,2))                                                                                     as overdue31to60_amount,
cast(res.overdue31to60_amount_rate as decimal(15,4))                                                                                as overdue31to60_amount_rate,

cast(res.overdue61to90_num_count as bigint)                                                                                         as overdue610to90_num_count,
cast(res.overdue61to90_num_rate as decimal(15,4))                                                                                   as overdue61to90_num_rate,
cast(res.overdue61to90_amount as decimal(15,2))                                                                                     as overdue61to90_amount,
cast(res.overdue61to90_amount_rate as decimal(15,4))                                                                                as overdue61to90_amount_rate,

cast(res.overdue90_num_count as bigint)                                                                                             as overdue90_num_count,
cast(res.overdue90_num_rate  as decimal(15,4))                                                                                      as overdue90_num_rate,
cast(res.overdue90_amount  as decimal(15,2))                                                                                        as overdue90_amount,
cast(overdue90_amount_rate as decimal(15,4))                                                                                        as overdue90_amount_rate,

cast(res.overdue30_num_count as bigint)                                                                                             as overdue30_num_count,
cast(res.overdue30_amount  as decimal(15,2))                                                                                        as overdue30_amount,
cast(overdue30_amount_rate as decimal(15,4))                                                                                        as overdue30_amount_rate,

cast(res.overdue180_num_count  as bigint)                                                                                           as overdue180_num_count,
cast(res.overdue180_amount as decimal(15,2))                                                                                        as overdue180_amount,

cast(res.static_90to_num as bigint)                                                                                                 as static_90to_num,
cast(res.static_90to_amount  as decimal(15,2))                                                                                      as static_90to_amount,
cast(res.static_90to_amount_rate as decimal(15,4))                                                                                  as static_90to_amount_rate,

cast(res.accum_30to_amount as decimal(15,2))                                                                                        as accum_30_recycle_amount,
cast(res.accum_90to_amount as decimal(15,2))                                                                                        as accum_90to_amount,
snapshot_date                                                                                                                         ,
cast(res.project_id as String)                                                                                                      as project_id
from
(
select
asset_bill.project_id                                                                                                                      as project_id,
sum(nvl(loan_info.remain_principal,0))                                                                                                     as total_remain_principal,
sum(loan_info.un_settle_num)                                                                                                               as un_settle_num,
sum(loan_info.remain_principal*asset_bill.loan_rate)/sum(loan_info.remain_principal)                                                       as weight_avg_inter_rate,
sum(loan_info.remain_principal*schedule.remian_term)/sum(loan_info.remain_principal)                                                       as weight_remain_term,
--实还+订单
sum(nvl(repay_hst.current_in_plan_repay_amount,0)+nvl(repay_hst.current_settle_amount,0)+nvl(repay_hst.return_back_amount,0))              as current_repay_amount,
sum(nvl(repay_hst.current_in_plan_repay_num,0)+nvl(repay_hst.current_settle_num,0)+nvl(repay_hst.return_back_num,0))                       as current_repay_bill_num,
sum(nvl(repay_hst.current_in_plan_repay_amount,0))                                                                                         as current_in_plan_repay_amount,
sum(nvl(repay_hst.current_in_plan_repay_num,0))                                                                                            as current_in_plan_repay_num,
sum(nvl(repay_hst.current_settle_amount,0))                                                                                                as current_early_settle_amount,
sum(nvl(repay_hst.current_settle_num,0))                                                                                                   as currnt_early_settle_num,
sum(nvl(repay_hst.return_back_amount,0))                                                                                                   as current_back_amount,
sum(nvl(repay_hst.return_back_num,0))                                                                                                      as current_back_num,
--借据表
sum(loan_info.un_settle_num)                                                                                                               as total_bill_num,
sum(nvl(loan_info.total_amount,0))                                                                                                         as total_amount,
sum(nvl(loan_info.normal_num,0))                                                                                                           as normal_total_num,
sum(nvl(loan_info.normal_num,0))/sum(loan_info.un_settle_num)                                                                              as normal_num_rate,
sum(nvl(loan_info.normal_amount,0))                                                                                                        as normal_total_amount,
sum(nvl(loan_info.normal_amount,0))/sum(nvl(loan_info.total_amount,0))                                                                     as normal_amount_rate,
sum(nvl(loan_info.overdue1to30_num_count,0))                                                                                               as overdue1to30_num_count,
sum(nvl(loan_info.overdue1to30_num_count,0))/sum(loan_info.un_settle_num)                                                                  as overdue1to30_num_rate,
sum(nvl(loan_info.overdue1to30_amount,0))                                                                                                  as overdue1to30_amount,
sum(nvl(loan_info.overdue1to30_amount,0))/sum(nvl(loan_info.total_amount,0))                                                               as overdue1to30_amount_rate,

sum(nvl(loan_info.overdue31to60_num_count,0))                                                                                              as overdue31to60_num_count,
sum(nvl(loan_info.overdue31to60_num_count,0))/sum(loan_info.un_settle_num)                                                                 as overdue31to60_num_rate,
sum(nvl(loan_info.overdue31to60_amount,0))                                                                                                 as overdue31to60_amount,
sum(nvl(loan_info.overdue31to60_amount,0))/sum(nvl(loan_info.total_amount,0))                                                              as overdue31to60_amount_rate,

sum(nvl(loan_info.overdue61to90_num_count,0))                                                                                              as overdue61to90_num_count,
sum(nvl(loan_info.overdue61to90_num_count,0))/sum(loan_info.un_settle_num)                                                                 as overdue61to90_num_rate,
sum(nvl(loan_info.overdue61to90_amount,0))                                                                                                 as overdue61to90_amount,
sum(nvl(loan_info.overdue61to90_amount,0))/sum(nvl(loan_info.total_amount,0))                                                              as overdue61to90_amount_rate,

sum(nvl(loan_info.overdue90_num_count,0))                                                                                                  as overdue90_num_count,
sum(nvl(loan_info.overdue90_num_count,0))/sum(loan_info.un_settle_num)                                                                     as overdue90_num_rate,
sum(nvl(loan_info.overdue90_amount,0))                                                                                                     as overdue90_amount,
sum(nvl(loan_info.overdue90_amount,0))/sum(nvl(loan_info.total_amount,0))                                                                  as overdue90_amount_rate,

sum(nvl(loan_info.overdue30_num_count,0))                                                                                                  as overdue30_num_count,
sum(nvl(loan_info.overdue30_amount,0))                                                                                                     as overdue30_amount,
sum(nvl(loan_info.overdue30_amount,0))/sum(nvl(asset_bill.total_bag_remain_amount,0))                                                      as overdue30_amount_rate,

sum(nvl(loan_info.overdue180_num_count,0))                                                                                                 as overdue180_num_count,
sum(nvl(loan_info.overdue180_amount,0))                                                                                                    as overdue180_amount,
--静态逾期表
count(static_over.due_bill_no)                                                                                                             as static_90to_num,
sum(nvl(static_over.remain_principal,0)+nvl(static_over.remain_interest,0))                                                                as static_90to_amount,
sum(nvl(static_over.remain_principal,0)+nvl(static_over.remain_interest,0)) /sum(nvl(asset_bill.total_bag_remain_principal,0))             as static_90to_amount_rate,
--还款计划
sum(nvl(schedule.accum_30to_amount,0))                                                                                                     as accum_30to_amount,
sum(nvl(schedule.accum_90to_amount,0))                                                                                                     as accum_90to_amount,
'${ST9}'                                                                                                                               as snapshot_date
from
(
select
    due_bill_no,
    biz_date,
    project_id,
    loan_rate,
    nvl(total_bag_remain_principal,0) as total_bag_remain_principal,
    nvl(total_bag_remain_interest,0) as total_bag_remain_interest,
    nvl(total_bag_remain_principal,0) +nvl(total_bag_remain_interest,0) as total_bag_remain_amount
    from
    dim.dim_ht_bag_asset where biz_date='${pack_date}'
) as asset_bill
left join (
select
        due_bill_no,
        sum(remain_principal)                                                                    as remain_principal,
        sum(if(remain_principal>0,1,0))                                                          as un_settle_num,
        sum(if(remain_principal>0,nvl(remain_principal,0)+nvl(remain_interest,0),0))             as total_amount,
        sum(if(overdue_days=0 and remain_principal>0,1,0))                                                  as normal_num,
        sum(if(overdue_days=0 and remain_principal>0,nvl(remain_principal,0)+nvl(remain_interest,0),0))     as normal_amount,
        sum(if(overdue_days between 1 and 30,1,0))                                               as overdue1to30_num_count,
        sum(if(overdue_days between 1 and 30,nvl(remain_principal,0)+nvl(remain_interest,0),0))  as overdue1to30_amount,
        sum(if(overdue_days between 31 and 60,1,0))                                              as overdue31to60_num_count,
        sum(if(overdue_days between 31 and 60,nvl(remain_principal,0)+nvl(remain_interest,0),0)) as overdue31to60_amount,
        sum(if(overdue_days between 61 and 90,1,0))                                              as overdue61to90_num_count,
        sum(if(overdue_days between 61 and 90,nvl(remain_principal,0)+nvl(remain_interest,0),0)) as overdue61to90_amount,
        sum(if(overdue_days >=91,1,0))                                                           as overdue90_num_count,
        sum(if(overdue_days >=91,nvl(remain_principal,0)+nvl(remain_interest,0),0))              as overdue90_amount,
        sum(if(overdue_days >30,1,0))                                                            as overdue30_num_count,
        sum(if(overdue_days >30,nvl(remain_principal,0)+nvl(remain_interest,0),0))               as overdue30_amount,
        sum(if(overdue_days >180,1,0))                                                           as overdue180_num_count,
        sum(if(overdue_days >180,nvl(remain_principal,0)+nvl(remain_interest,0),0))              as overdue180_amount
    from ods.loan_info where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in (${product_id})
    group by due_bill_no
) as loan_info
on asset_bill.due_bill_no=loan_info.due_bill_no
left join (
    select
    asset_bill.due_bill_no,
    sum(if(paid_out_type not in ('REFUND','PRE_SETTLE','REDEMPTION','REFUND_SETTLEMENT') or repay_term=0 or paid_out_type is null,nvl(repay_amount,0),0 ))                                                              as current_in_plan_repay_amount,
    count(distinct if(paid_out_date='${ST9}' and (paid_out_type not in ('REFUND','PRE_SETTLE','REDEMPTION','REFUND_SETTLEMENT') or repay_term=0  or paid_out_type is null),asset_bill.due_bill_no,null ))           as current_in_plan_repay_num,
    sum(if(loan_info.paid_out_date='${ST9}' and paid_out_type in ('REFUND','PRE_SETTLE','REFUND_SETTLEMENT') and repay_term!=0,nvl(repay_amount,0),0))                                                              as current_settle_amount ,
    count(distinct if(loan_info.paid_out_date='${ST9}' and paid_out_type in ('REFUND','PRE_SETTLE','REFUND_SETTLEMENT') and repay_term!=0,asset_bill.due_bill_no,null))                                             as current_settle_num,
    sum(if(loan_info.paid_out_date='${ST9}' and paid_out_type in ('REDEMPTION') and repay_term!=0 ,nvl(repay_amount,0),0))                                                                                          as return_back_amount,
    count(distinct if(loan_info.paid_out_date='${ST9}' and paid_out_type in ('REDEMPTION') and repay_term!=0,asset_bill.due_bill_no,null))                                                                          as return_back_num
    from
    (
    select
    due_bill_no,
    biz_date
    from dim.dim_ht_bag_asset where biz_date='${pack_date}'
    )asset_bill
    left join
    (
        select
            distinct
            due_bill_no,
            paid_out_date,
            loan_term,
            paid_out_type
        from ods.repay_schedule where '${ST9}' between s_d_date and date_sub(e_d_date,1)
        and product_id in (${product_id})
    )loan_info
    on asset_bill.due_bill_no=loan_info.due_bill_no
    left join
    (
        select
            due_bill_no,
            order_id,
            repay_term,
            sum(repay_amount) as repay_amount
        from ods.repay_detail
            where product_id in (${product_id}) and biz_date='${ST9}'
            group by due_bill_no,order_id,repay_term
    )repayhst
    on loan_info.due_bill_no=repayhst.due_bill_no
    and loan_info.loan_term=repayhst.repay_term
    group by asset_bill.due_bill_no
)repay_hst
 on asset_bill.due_bill_no=repay_hst.due_bill_no
left join (
    select
    asset_bill.due_bill_no,
    sum(if(schedule.paid_out_date is not null and schedule.paid_out_date>asset_bill.biz_date and datediff(schedule.paid_out_date,schedule.should_repay_date)>30 ,nvl(schedule.paid_principal,0)+nvl(schedule.paid_interest,0),0)) as accum_30to_amount,
    sum(if(schedule.paid_out_date is not null and schedule.paid_out_date>static_over.biz_date and datediff(schedule.paid_out_date,schedule.should_repay_date)>90 ,nvl(schedule.paid_principal,0),0))                              as accum_90to_amount,
    sum(if(schedule.schedule_status='N' and schedule.loan_term!=0,1,0))                                                                                                                                                           as remian_term
    from
    (
    select
        due_bill_no,
        biz_date
        from
        dim.dim_ht_bag_asset where biz_date='${pack_date}'
    )asset_bill
    left join
    (
    select
        due_bill_no,
        paid_out_date,
        paid_out_type,
        paid_principal,
        paid_interest,
        should_repay_date,
        schedule_status,
        loan_term
    from ods.repay_schedule
        where product_id in (${product_id})
        and '${ST9}' between s_d_date and date_sub(e_d_date,1)
    )schedule
    on asset_bill.due_bill_no=schedule.due_bill_no
    left join (
        select
        due_bill_no,
        biz_date
        from
        dim.dim_static_overdue_bill
        where prj_type='HT' and  overdue_days=91 and biz_date <='${ST9}'
    )static_over on asset_bill.due_bill_no=static_over.due_bill_no
    group by asset_bill.due_bill_no
)schedule on asset_bill.due_bill_no=schedule.due_bill_no
left join (
    select
    due_bill_no,
    remain_principal,
    remain_interest
    from
    dim.dim_static_overdue_bill
    where prj_type='HT' and  overdue_days=91 and biz_date <='${ST9}'
)static_over on asset_bill.due_bill_no=static_over.due_bill_no
group by asset_bill.project_id,asset_bill.biz_date
)res
;
