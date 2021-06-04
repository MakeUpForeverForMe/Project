set spark.executor.memory=2g;
set spark.executor.memoryOverhead=512M;
set hive.auto.convert.join=false;            -- 关闭自动 MapJoin
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
 
set hivevar:product_id_list='DIDI201908161538','001601','001602','001603','001701','001702';

insert overwrite table dw_cps.dw_asset_info_day partition (biz_date='${ST9}',project_id)
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
    other.comp_repay_sum                                                     as comp_repay_sum,
    other.comp_repay_principal                                               as comp_repay_principal,
    other.comp_repay_interest                                                as comp_repay_interest,
    other.comp_repay_penalty                                                 as comp_repay_penalty,
    other.comp_repay_fee                                                     as comp_repay_fee,
    other.repo_repay_sum                                                     as repo_repay_sum,
    other.repo_repay_principal                                               as repo_repay_principal,
    other.repo_repay_interest                                                as repo_repay_interest,
    other.repo_repay_penalty                                                 as repo_repay_penalty,
    other.repo_repay_fee                                                     as repo_repay_fee,
    other.return_repay_sum                                                   as return_repay_sum,
    other.return_repay_principal                                             as return_repay_principal,
    other.return_repay_interest                                              as return_repay_interest,
    other.return_repay_penalty                                               as return_repay_penalty,
    other.return_repay_fee                                                   as return_repay_fee,
    loan.project_id                                                          as project_id
from
(
    select
        b.project_id                                                         as project_id,
        sum(loan.remain_principal)                                           as remain_principal,
        sum(if(lend.loan_active_date='${ST9}',loan_original_principal,0))        as loan_principal
    from
        (
            select product_id,due_bill_no,remain_principal
            from ods_cps.loan_info
            where '${ST9}' between s_d_date and date_sub(e_d_date,1)  ${hive_param_str}
        ) loan
        left join
        (
            select due_bill_no,loan_active_date,loan_original_principal
            from ods_cps.loan_lending
            where biz_date='${ST9}'  ${hive_param_str}
        ) lend
        on loan.due_bill_no=lend.due_bill_no
        left join (
            select distinct
                capital_id,
                channel_id,
                project_id,
                product_id_vt,
                product_id
              from (
                    select
                      max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
                      max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                      max(if(col_name = 'project_id',   col_val,null)) as project_id,
                      max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
                      max(if(col_name = 'product_id',   col_val,null)) as product_id
                    from dim.data_conf
                    where col_type = 'ac'
                    group by col_id
                )tmp
        ) b on loan.product_id=b.product_id
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
            from ods_cps.repay_detail
            where biz_date = '${ST9}'  ${hive_param_str}
        ) repay
        left join (
            select distinct
                capital_id,
                channel_id,
                project_id,
                product_id_vt,
                product_id
              from (
                select
                  max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
                  max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                  max(if(col_name = 'project_id',   col_val,null)) as project_id,
                  max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
                  max(if(col_name = 'product_id',   col_val,null)) as product_id
                from dim.data_conf
                where col_type = 'ac'
                group by col_id
                )tmp
        ) b on repay.product_id=b.product_id
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
            from ods_cps.order_info
            where biz_date='${ST9}' and command_type in ('SDB','BDB')
            and loan_usage in ('N', 'O', 'PD','R','W','M','I') and order_status='S'  ${hive_param_str}
        ) o
        left join
        (
            select product_id,repay_amount,bnp_type,order_id
            from ods_cps.repay_detail
            where biz_date='${ST9}'  ${hive_param_str}
        ) r
        on o.order_id=r.order_id
        left join (
            select distinct
                capital_id,
                channel_id,
                project_id,
                product_id_vt,
                product_id
              from (
                select
                  max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
                  max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                  max(if(col_name = 'project_id',   col_val,null)) as project_id,
                  max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
                  max(if(col_name = 'product_id',   col_val,null)) as product_id
                from dim.data_conf
                where col_type = 'ac'
                group by col_id
                )tmp
        ) b
        on o.product_id=b.product_id
    group by b.project_id
) cust
on loan.project_id=cust.project_id
left join
(
    select
        b.project_id                                                           as project_id,
        sum(if(o.loan_usage='D',r.repay_amount,0))                             as comp_repay_sum,
        sum(if(o.loan_usage='D' and r.bnp_type='Pricinpal',r.repay_amount,0))  as comp_repay_principal,
        sum(if(o.loan_usage='D' and r.bnp_type='Interest',r.repay_amount,0))   as comp_repay_interest,
        sum(if(o.loan_usage='D' and r.bnp_type='Penalty',r.repay_amount,0))    as comp_repay_penalty,
        sum(if(o.loan_usage='D' and r.bnp_type='SVCFee',r.repay_amount,0))     as comp_repay_fee,
        sum(if(o.loan_usage='B',r.repay_amount,0))                             as repo_repay_sum,
        sum(if(o.loan_usage='B' and r.bnp_type='Pricinpal',r.repay_amount,0))  as repo_repay_principal,
        sum(if(o.loan_usage='B' and r.bnp_type='Interest',r.repay_amount,0))   as repo_repay_interest,
        sum(if(o.loan_usage='B' and r.bnp_type='Penalty',r.repay_amount,0))    as repo_repay_penalty,
        sum(if(o.loan_usage='B' and r.bnp_type='SVCFee',r.repay_amount,0))     as repo_repay_fee,
        sum(if(o.loan_usage='T',r.repay_amount,0))                             as return_repay_sum,
        sum(if(o.loan_usage='T' and r.bnp_type='Pricinpal',r.repay_amount,0))  as return_repay_principal,
        sum(if(o.loan_usage='T' and r.bnp_type='Interest',r.repay_amount,0))   as return_repay_interest,
        sum(if(o.loan_usage='T' and r.bnp_type='Penalty',r.repay_amount,0))    as return_repay_penalty,
        sum(if(o.loan_usage='T' and r.bnp_type='SVCFee',r.repay_amount,0))     as return_repay_fee
    from
        (
            select product_id,order_id,loan_usage
            from ods_cps.order_info
            where biz_date='${ST9}' and order_status='S'  ${hive_param_str}
        ) o
            left join
        (
            select product_id,repay_amount,bnp_type,order_id
            from ods_cps.repay_detail
            where biz_date='${ST9}'  ${hive_param_str}
        ) r
        on o.order_id=r.order_id
        left join (
            select
                distinct
                capital_id,
                channel_id,
                project_id,
                product_id_vt,
                product_id
              from (
                    select
                      max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
                      max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                      max(if(col_name = 'project_id',   col_val,null)) as project_id,
                      max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
                      max(if(col_name = 'product_id',   col_val,null)) as product_id
                    from dim.data_conf
                    where col_type = 'ac'
                    group by col_id
                )tmp
        ) b
        on o.product_id=b.product_id
    group by b.project_id
) other
on loan.project_id=other.project_id
union all
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
    other.comp_repay_sum                                                     as comp_repay_sum,
    other.comp_repay_principal                                               as comp_repay_principal,
    other.comp_repay_interest                                                as comp_repay_interest,
    other.comp_repay_penalty                                                 as comp_repay_penalty,
    other.comp_repay_fee                                                     as comp_repay_fee,
    other.repo_repay_sum                                                     as repo_repay_sum,
    other.repo_repay_principal                                               as repo_repay_principal,
    other.repo_repay_interest                                                as repo_repay_interest,
    other.repo_repay_penalty                                                 as repo_repay_penalty,
    other.repo_repay_fee                                                     as repo_repay_fee,
    other.return_repay_sum                                                   as return_repay_sum,
    other.return_repay_principal                                             as return_repay_principal,
    other.return_repay_interest                                              as return_repay_interest,
    other.return_repay_penalty                                               as return_repay_penalty,
    other.return_repay_fee                                                   as return_repay_fee,
    loan.project_id                                                          as project_id
from
(
    select
        b.project_id                                                         as project_id,
        sum(loan.remain_principal)                                           as remain_principal,
        sum(if(lend.loan_active_date='${ST9}',loan_original_principal,0))        as loan_principal
    from
        (
            select product_id,due_bill_no,remain_principal
            from ods.loan_info
            where '${ST9}' between s_d_date and date_sub(e_d_date,1)
            and product_id in (${product_id_list})
            and loan_active_date <= '${ST9}'  ${hive_param_str}
        ) loan
        left join
        (
            select due_bill_no,loan_active_date,loan_original_principal
            from ods.loan_lending
            where biz_date='${ST9}'  ${hive_param_str}
            and product_id in (${product_id_list})
        ) lend
        on loan.due_bill_no=lend.due_bill_no
        left join (
            select distinct
                capital_id,
                channel_id,
                project_id,
                product_id_vt,
                product_id
              from (
                select
                  max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
                  max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                  max(if(col_name = 'project_id',   col_val,null)) as project_id,
                  max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
                  max(if(col_name = 'product_id',   col_val,null)) as product_id
                from dim.data_conf
                where col_type = 'ac'
                group by col_id
                )tmp
        ) b
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
            and product_id in (${product_id_list})
        ) repay
        left join (
            select
                distinct
                capital_id,
                channel_id,
                project_id,
                product_id_vt,
                product_id
              from (
                select
                  max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
                  max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                  max(if(col_name = 'project_id',   col_val,null)) as project_id,
                  max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
                  max(if(col_name = 'product_id',   col_val,null)) as product_id
                from dim.data_conf
                where col_type = 'ac'
                group by col_id
                )tmp
        ) b
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
            and loan_usage in ('N', 'O', 'PD','R','W','M','I') and order_status='S'
            and product_id in (${product_id_list})  ${hive_param_str}
        ) o
            left join
        (
            select product_id,repay_amount,bnp_type,order_id
            from ods.repay_detail
            where biz_date='${ST9}'
            and product_id in (${product_id_list})  ${hive_param_str}
        ) r
        on o.order_id=r.order_id
        left join (
            select distinct
                capital_id,
                channel_id,
                project_id,
                product_id_vt,
                product_id
              from (
                select
                  max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
                  max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                  max(if(col_name = 'project_id',   col_val,null)) as project_id,
                  max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
                  max(if(col_name = 'product_id',   col_val,null)) as product_id
                from dim.data_conf
                where col_type = 'ac'
                group by col_id
                )tmp
        ) b
        on o.product_id=b.product_id
    group by b.project_id
) cust
on loan.project_id=cust.project_id
left join
(
    select
        b.project_id                                                           as project_id,
        sum(if(o.loan_usage='D',r.repay_amount,0))                             as comp_repay_sum,
        sum(if(o.loan_usage='D' and r.bnp_type='Pricinpal',r.repay_amount,0))  as comp_repay_principal,
        sum(if(o.loan_usage='D' and r.bnp_type='Interest',r.repay_amount,0))   as comp_repay_interest,
        sum(if(o.loan_usage='D' and r.bnp_type='Penalty',r.repay_amount,0))    as comp_repay_penalty,
        sum(if(o.loan_usage='D' and r.bnp_type='SVCFee',r.repay_amount,0))     as comp_repay_fee,
        sum(if(o.loan_usage='B',r.repay_amount,0))                             as repo_repay_sum,
        sum(if(o.loan_usage='B' and r.bnp_type='Pricinpal',r.repay_amount,0))  as repo_repay_principal,
        sum(if(o.loan_usage='B' and r.bnp_type='Interest',r.repay_amount,0))   as repo_repay_interest,
        sum(if(o.loan_usage='B' and r.bnp_type='Penalty',r.repay_amount,0))    as repo_repay_penalty,
        sum(if(o.loan_usage='B' and r.bnp_type='SVCFee',r.repay_amount,0))     as repo_repay_fee,
        sum(if(o.loan_usage in ('T','R'),r.repay_amount,0))                             as return_repay_sum,
        sum(if(o.loan_usage in ('T','R') and r.bnp_type='Pricinpal',r.repay_amount,0))  as return_repay_principal,
        sum(if(o.loan_usage in ('T','R') and r.bnp_type='Interest',r.repay_amount,0))   as return_repay_interest,
        sum(if(o.loan_usage in ('T','R') and r.bnp_type='Penalty',r.repay_amount,0))    as return_repay_penalty,
        sum(if(o.loan_usage in ('T','R')  and r.bnp_type='SVCFee',r.repay_amount,0))     as return_repay_fee
    from
        (
            select product_id,order_id,loan_usage
            from ods.order_info
            where biz_date='${ST9}' and order_status='S'
            and product_id in (${product_id_list})  ${hive_param_str}
        ) o
            left join
        (
            select product_id,repay_amount,bnp_type,order_id
            from ods.repay_detail
            where biz_date='${ST9}'
            and product_id in (${product_id_list})  ${hive_param_str}
        ) r
        on o.order_id=r.order_id
        left join (
            select distinct
                capital_id,
                channel_id,
                project_id,
                product_id_vt,
                product_id
              from (
                select
                  max(if(col_name = 'capital_id',   col_val,null)) as capital_id,
                  max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                  max(if(col_name = 'project_id',   col_val,null)) as project_id,
                  max(if(col_name = 'product_id_vt',col_val,null)) as product_id_vt,
                  max(if(col_name = 'product_id',   col_val,null)) as product_id
                from dim.data_conf
                where col_type = 'ac'
                group by col_id
                )tmp
        ) b
        on o.product_id=b.product_id
    group by b.project_id
) other
on loan.project_id=other.project_id
;
