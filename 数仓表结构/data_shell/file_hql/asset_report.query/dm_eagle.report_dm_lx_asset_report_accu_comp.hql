refresh ods_new_s_cps.loan_info;
refresh ods_new_s_cps.repay_schedule;
refresh ods_new_s_cps.order_info;
refresh ods_new_s_cps.repay_detail;

set var:product_id='001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';

upsert into dm_eagle.report_dm_lx_asset_report_accu_comp
select
    concat(p1.project_id,'${var:ST9}')                                  as id,
    '${var:ST9}'                                                        as snapshot_date,
    p1.project_id                                                       as project_id,
    cast(p1.total_remain_prin as decimal(15,2))                         as total_remain_prin,
    cast(p1.total_remain_num as int)                                    as total_remain_num,
    cast(p1.average_remain as decimal(15,6))                            as average_remain,
    cast(p2.weighted_average_rate as decimal(15,6))                     as weighted_average_rate,
    cast(p2.weighted_average_remain_tentor as decimal(15,6))            as weighted_average_remain_tentor,
    cast(p3.total_contract_amount as decimal(15,2))                     as total_contract_amount,
    cast(p3.total_contract_num as int)                                  as total_contract_num,
    cast(p3.average_contract_amount as decimal(15,6))                   as average_contract_amount,
    cast(p3.total_repurchase_contract as decimal(15,2))                 as total_repurchase_contract,
    cast(p3.total_repurchase_num as int)                                as total_repurchase_num,
    cast(p3.weighted_average_rate_by_contract as decimal(15,6))         as weighted_average_rate_by_contract,
    cast(p3.weighted_average_term_by_contract as decimal(15,6))         as weighted_average_term_by_contract,
    cast(p3.loan_amount_accum as decimal(15,2))                         as loan_amount_accum,
    cast(p3.total_amount_accum  as decimal(15,2))                       as total_amount_accum,
    cast(p3.total_prin_accum as decimal(15,2))                          as total_prin_accum,
    cast(p3.total_inter_accum as decimal(15,2))                         as total_inter_accum,
    cast(p3.prepay_amount as decimal(15,2))                             as prepay_amount_accum,
    cast(p3.prepay_prin as decimal(15,2))                               as prepay_prin_accouum,
    cast(p3.prepay_inter as decimal(15,2))                              as prepay_inter_accum,
    cast(p3.compensation_amount as decimal(15,2))                       as compensation_amount_accum,
    cast(p3.compensation_prin as decimal(15,2))                         as compensation_prin_accum,
    cast(p3.compensation_inter as decimal(15,2))                        as compensation_inter_accum,
    cast(p3.repurchase_amount as decimal(15,2))                         as repurchase_amount_accum,
    cast(p3.repurchase_prin as decimal(15,2))                           as repurchase_prin_accum,
    cast(p3.repurchase_inter as decimal(15,2))                          as repurchase_inter_accum,
    cast(p3.refund_contract_amount as decimal(15,2))                    as refund_contract_amount_accum,
    cast(p3.refund_contract_num as int)                                 as refund_contract_num_accum
from
(
    select
        biz.project_id,
        sum(if(today_loan_sum is null,0,today_loan_sum) + if(yest_remain_sum is null,0,yest_remain_sum))              as total_remain_prin,
        sum(if(today_loan_count is null,0,today_loan_count) + if(yest_remain_count is null,0,yest_remain_count))      as total_remain_num,
        if(sum(if(today_loan_count is null,0,today_loan_count) + if(yest_remain_count is null,0,yest_remain_count)) > 0,
            sum(if(today_loan_sum is null,0,today_loan_sum) + if(yest_remain_sum is null,0,yest_remain_sum)) / sum(if(today_loan_count is null,0,today_loan_count) + if(yest_remain_count is null,0,yest_remain_count)),
            0)                                                                                 as average_remain
    from
    (
        select *
        from dim_new.biz_conf biz
        where product_id in (${var:product_id})
    ) biz
    left join
    (
        select
            product_id,
            sum(loan_init_principal)                                                           as today_loan_sum,
            count(*)                                                                           as today_loan_count
        from ods_new_s_cps.loan_info
        where product_id in (${var:product_id})
        and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
        and loan_active_date = '${var:ST9}'
       group by product_id
    ) today
    on biz.product_id = today.product_id
    left join
    (
        select
            product_id,
            cast(sum(if(remain_principal > 0,remain_principal,0)) as decimal(15,2))            as yest_remain_sum,
            cast(count(if(remain_principal > 0,1,null)) as decimal(15,2))                      as yest_remain_count
        from ods_new_s_cps.loan_info
        where product_id in (${var:product_id})
        and date_sub('${var:ST9}',1) between s_d_date and date_sub(e_d_date,1)
        and loan_active_date <= '${var:ST9}'
       group by product_id
    ) yest
    on biz.product_id = yest.product_id
    group by biz.project_id
) p1
left join
(
    select
        nvl(t1.project_id,t2.project_id)                                                           as project_id,
        if(if(t1.prin is null,0,t1.prin) + if(t2.prin is null,0,t2.prin) > 0,
        cast((if(t1.rate is null,0,t1.rate) + if(t2.rate is null,0,t2.rate)) as decimal(20,8)) / (if(t1.prin is null,0,t1.prin) + if(t2.prin is null,0,t2.prin)),0)     as weighted_average_rate,
        if(if(t1.prin is null,0,t1.prin) + if(t2.prin is null,0,t2.prin) > 0,
           cast((if(t1.term is null,0,t1.term) + if(t2.term is null,0,t2.term)) as decimal(20,6)) / (if(t1.prin is null,0,t1.prin) + if(t2.prin is null,0,t2.prin)),0)     as weighted_average_remain_tentor
    from
    (
        select
            biz.project_id                                                                         as project_id,
            sum(li.loan_init_principal * ll.loan_init_interest_rate)                               as rate,
            sum(li.loan_init_principal)                                                            as prin,
            sum(li.loan_init_principal * li.loan_init_term)                                        as term
        from
        (
            select *
            from ods_new_s_cps.loan_info
            where product_id in (${var:product_id})
            and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
            and loan_active_date = '${var:ST9}'
       ) li
        left join
        (
            select due_bill_no,loan_init_interest_rate
            from ods_new_s_cps.loan_lending
            where product_id in (${var:product_id})
            and biz_date = '${var:ST9}'
       ) ll
        on li.due_bill_no = ll.due_bill_no
        left join
        (
            select *
            from dim_new.biz_conf biz
            where product_id in (${var:product_id})
        ) biz
        on li.product_id = biz.product_id
        group by biz.project_id
    ) t1
    full join
    (
        select
            biz.project_id                                                                         as project_id,
            sum(li.remain_principal * ll.loan_init_interest_rate)                                  as rate,
            sum(li.remain_principal)                                                               as prin,
            sum(li.remain_principal * li.loan_term_remain)                                         as term
        from
        (
            select *
            from ods_new_s_cps.loan_info
            where product_id in (${var:product_id})
            and date_sub('${var:ST9}',1) between s_d_date and date_sub(e_d_date,1)
            and loan_active_date <= date_sub('${var:ST9}',1)
           and remain_principal > 0
        ) li
        left join
        (
            select due_bill_no,loan_init_interest_rate
            from ods_new_s_cps.loan_lending
            where product_id in (${var:product_id})
        ) ll
        on li.due_bill_no = ll.due_bill_no
        left join
        (
            select *
            from dim_new.biz_conf biz
            where product_id in (${var:product_id})
        ) biz
        on li.product_id = biz.product_id
        group by biz.project_id
    ) t2
    on t1.project_id = t2.project_id
) p2
on p1.project_id = p2.project_id
left join
(
    select
        biz.project_id                                                                             as project_id,
        sum(t1.total_contract_amount)                                                              as total_contract_amount,
        sum(t1.total_contract_num)                                                                 as total_contract_num,
        if( sum(t1.total_contract_num) > 0,
            cast((sum(t1.total_contract_amount) / sum(t1.total_contract_num)) as decimal(15,6)),
            cast(0 as decimal(15,6)))                                                              as average_contract_amount,
        sum(t2.total_repurchase_contract)                                                          as total_repurchase_contract,
        sum(t2.total_repurchase_num)                                                               as total_repurchase_num,
        if(sum(t3.total_principal) > 0,
            cast((sum(t3.total_rate_by_contract) / sum(t3.total_principal)) as decimal(15,6)),
            cast(0 as decimal(15,6)))                                                              as weighted_average_rate_by_contract,
        if(sum(t3.total_principal) > 0,
            cast((sum(t3.total_term_by_contract) / sum(t3.total_principal)) as decimal(15,6)),
            cast(0 as decimal(15,6)))                                                              as weighted_average_term_by_contract,
        sum(t1.total_contract_amount)                                                              as loan_amount_accum,
        sum(t4.total_amount_accum)                                                                 as total_amount_accum,
        sum(t4.total_prin_accum)                                                                   as total_prin_accum,
        sum(t4.total_inter_accum)                                                                  as total_inter_accum,
        0                                                                                          as normal_amount,
        0                                                                                          as normal_prin,
        0                                                                                          as normal_inter,
        0                                                                                          as overdue_amount,
        0                                                                                          as overdue_prin,
        0                                                                                          as overdue_inter,
        0                                                                                          as prepay_amount,
        0                                                                                          as prepay_prin,
        0                                                                                          as prepay_inter,
        sum(t5.compensation_amount)                                                                as compensation_amount,
        sum(t5.compensation_prin)                                                                  as compensation_prin,
        sum(t5.compensation_inter)                                                                 as compensation_inter,
        sum(t5.repurchase_amount)                                                                  as repurchase_amount,
        sum(t5.repurchase_prin)                                                                    as repurchase_prin,
        sum(t5.repurchase_inter)                                                                   as repurchase_inter,
        sum(t5.refund_contract_amount)                                                             as refund_contract_amount,
        sum(t5.refund_contract_num)                                                                as refund_contract_num
    from
    (
        select
            product_id                                                                             as product_id,
            sum(loan_init_principal)                                                               as total_contract_amount,
            count(due_bill_no)                                                                     as total_contract_num
        from ods_new_s_cps.loan_info
        where product_id in (${var:product_id})
        and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
        and loan_active_date <= '${var:ST9}'
       group by product_id
    ) t1
    left join
    (
        select
            l.product_id                                                                           as  product_id,
            sum(if(r.if_buy_back > 0,l.loan_init_principal,0))                                     as  total_repurchase_contract,
            count(if(r.if_buy_back > 0,1,null))                                                    as  total_repurchase_num
        from
        (
            select product_id,due_bill_no,loan_init_principal
            from ods_new_s_cps.loan_info
            where product_id in (${var:product_id})
            and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
            and loan_active_date <= '${var:ST9}'
       ) l
        left join
        (
            select
                due_bill_no,
                sum(if(paid_out_type='BUY_BACK',1,0))   as if_buy_back
            from ods_new_s_cps.repay_schedule
            where product_id in (${var:product_id})
            and '${var:ST9}' between s_d_date and date_sub(e_d_date, 1)
            group by due_bill_no
        ) r
        on l.due_bill_no = r.due_bill_no
        group by l.product_id
    ) t2
    on t1.product_id = t2.product_id
    left join
    (
        select
            product_id                                                                            as product_id,
            sum(loan_init_principal * loan_init_interest_rate)                                    as total_rate_by_contract,
            sum(loan_init_principal * loan_init_term)                                             as total_term_by_contract,
            sum(loan_init_principal)                                                              as total_principal
        from ods_new_s_cps.loan_lending
        where product_id in (${var:product_id})
        and biz_date <= '${var:ST9}'
       group by product_id
    ) t3
    on t1.product_id = t3.product_id
    left join
    (
        select
            product_id                                                                            as product_id,
            sum(repay_amount)                                                                     as total_amount_accum,
            sum(if(bnp_type='Pricinpal',repay_amount,0))                                          as total_prin_accum,
            sum(if(bnp_type<>'Pricinpal',repay_amount,0))                                         as total_inter_accum
        from ods_new_s_cps.repay_detail
        where product_id in (${var:product_id})
        and to_date(txn_time) <= date_sub('${var:ST9}',1)
       group by product_id
    ) t4
    on t1.product_id = t4.product_id
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
            sum(if(ord.loan_usage='T',repay.repay_amount,0))                            as refund_contract_amount,
            count(if(ord.loan_usage='T',ord.order_id,null))                             as refund_contract_num
        from
        (
            select *
            from ods_new_s_cps.order_info
            where product_id in (${var:product_id})
            and biz_date <= date_sub('${var:ST9}',1)
           and loan_usage<>'L' and order_status='S'
        ) ord
        left join
        (
            select *
            from ods_new_s_cps.repay_detail
            where product_id in (${var:product_id})
            and to_date(txn_time) <= date_sub('${var:ST9}',1)
       ) repay
        on ord.order_id = repay.order_id
        group by ord.product_id
    ) t5
    on t1.product_id = t5.product_id
    left join
    (
        select *
        from dim_new.biz_conf biz
        where product_id in (${var:product_id})
    ) biz
    on t1.product_id = biz.product_id
    group by biz.project_id
) p3
on p1.project_id = p3.project_id;
exit;
