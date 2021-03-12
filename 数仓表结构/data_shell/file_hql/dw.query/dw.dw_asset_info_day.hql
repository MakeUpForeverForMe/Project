set spark.executor.memory=2g;
set spark.executor.memoryOverhead=512M;
set hive.auto.convert.join=false;            -- 关闭自动 MapJoin
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
 
insert overwrite table dw.dw_asset_info_day partition (biz_date='${ST9}',project_id)
select
    loan.remain_principal                                                    as remain_principal,
    loan.loan_principal                                                      as loan_sum_daily,
    totalRp.repay_sum_daily                                                  as repay_sum_daily,
    totalRp.repay_principal                                                  as repay_principal,
    totalRp.repay_interest                                                   as repay_interest,
    totalRp.repay_penalty                                                    as repay_penalty,
    totalRp.repay_fee                                                        as repay_fee,
    cust.cust_repay_sum                                                      as cust_repay_sum,
    cust.cust_repay_principal                                                as cust_repay_principal,
    cust.cust_repay_interest                                                 as cust_repay_interest,
    cust.cust_repay_penalty                                                  as cust_repay_penalty,
    cust.cust_repay_fee                                                      as cust_repay_fee,
    other.back_repay_sum                                                     as back_repay_sum,
    other.back_repay_principal                                               as back_repay_principal,
    other.back_repay_interest                                                as back_repay_interest,
    other.back_repay_penalty                                                 as back_repay_penalty,
    other.back_repay_fee                                                     as back_repay_fee,
    loan.project_id                                                          as project_id
from
(
    select
        b.project_id                                                         as project_id,
        sum(loan.remain_principal)                                           as remain_principal,
        sum(if(lend.loan_active_date='${ST9}',loan_init_principal,0))        as loan_principal
    from
        (
            select product_id,due_bill_no,remain_principal
            from ods.loan_info
            where '${ST9}' between s_d_date and date_sub(e_d_date,1)  ${hive_param_str}
        ) loan
            left join
        (
            select due_bill_no,loan_active_date,loan_init_principal
            from ods.loan_lending
            where biz_date='${ST9}' ${hive_param_str}
        ) lend
        on loan.due_bill_no=lend.due_bill_no
            left join dim.biz_conf b
                      on loan.product_id=b.product_id
    group by b.project_id
) loan
    left join
(
    select
        b.project_id                                                          as project_id,
        sum(repay_amount)                                                     as repay_sum_daily,
        sum(if(bnp_type='Pricinpal',repay_amount,0))                          as repay_principal,
        sum(if(bnp_type='Interest',repay_amount,0))                           as repay_interest,
        sum(if(bnp_type='Penalty',repay_amount,0))                            as repay_penalty,
        sum(if(bnp_type='SVCFee',repay_amount,0))                             as repay_fee
    from
        (
            select product_id,repay_amount,bnp_type
            from ods.repay_detail
            where biz_date = '${ST9}'  ${hive_param_str}
        ) repay
            left join dim.biz_conf b
                      on repay.product_id=b.product_id
    group by b.project_id
) totalRp
on loan.project_id=totalRp.project_id
    left join
(
    select
        b.project_id                                                           as project_id,
        sum(r.repay_amount)                                                    as cust_repay_sum,
        sum(if(r.bnp_type='Pricinpal',r.repay_amount,0))                       as cust_repay_principal,
        sum(if(r.bnp_type='Interest',r.repay_amount,0))                        as cust_repay_interest,
        sum(if(r.bnp_type='Penalty',r.repay_amount,0))                         as cust_repay_penalty,
        sum(if(r.bnp_type='SVCFee',r.repay_amount,0))                          as cust_repay_fee
    from
        (
            select *
            from ods.order_info
            where biz_date='${ST9}' and command_type in ('SDB','BDB')
              and loan_usage in ('N', 'O', 'PD','R','W','M','I') and order_status='S'  ${hive_param_str}
        ) o
            left join
        (
            select product_id,repay_amount,bnp_type,order_id
            from ods.repay_detail
            where biz_date='${ST9}' ${hive_param_str}
        ) r
        on o.order_id=r.order_id
            left join dim.biz_conf b
                      on o.product_id=b.product_id
    group by b.project_id
) cust
on loan.project_id=cust.project_id
    left join
(
    select
        b.project_id                                                           as project_id,
        sum(if(o.loan_usage='F',r.repay_amount,0))                             as back_repay_sum,
        sum(if(o.loan_usage='F' and r.bnp_type='Pricinpal',r.repay_amount,0))  as back_repay_principal,
        sum(if(o.loan_usage='F' and r.bnp_type='Interest',r.repay_amount,0))   as back_repay_interest,
        sum(if(o.loan_usage='F' and r.bnp_type='Penalty',r.repay_amount,0))    as back_repay_penalty,
        sum(if(o.loan_usage='F' and r.bnp_type='SVCFee',r.repay_amount,0))     as back_repay_fee
    from
        (
            select product_id,order_id,loan_usage
            from ods.order_info
            where biz_date='${ST9}' and order_status='S' ${hive_param_str}
        ) o
            left join
        (
            select product_id,repay_amount,bnp_type,order_id
            from ods.repay_detail
            where biz_date='${ST9}' ${hive_param_str}
        ) r
        on o.order_id=r.order_id
            left join dim.biz_conf b
                      on o.product_id=b.product_id
    group by b.project_id
) other
on loan.project_id=other.project_id;
