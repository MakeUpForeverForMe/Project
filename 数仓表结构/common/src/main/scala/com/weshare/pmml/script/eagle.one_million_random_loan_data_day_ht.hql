--set hive.exec.parallel=true;
--set hive.exec.parallel.thread.number=8;
--SET hivevar:ST9=2021-04-14;
--set hivevar:product_id_list='001801','001802';
--set hivevar:product_id_list='001801','001802';
SET hivevar:product_id_list='001601','001602','001603';
set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=256000000;      -- 64M
set hive.merge.smallfiles.avgsize=256000000; -- 64M
set mapreduce.input.fileinputformat.split.minsize=268435456;

-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
set hive.auto.convert.join.noconditionaltask=false;
set hive.auto.convert.join=false;
INSERT OVERWRITE TABLE eagle.one_million_random_loan_data_day PARTITION(product_id)
select
 t3.due_bill_no                                             --'借据编号'
,null as risk_level                                         --'风险等级'
,t3.loan_init_principal                                     --'放款本金'
,t3.loan_init_term                                          --'贷款期数'
,t3.loan_init_interest_rate                                 --'利息利率（8d/%）'
,null as credit_coef                                             --'综合融资成本（8d/%）'
,t3.loan_init_penalty_rate                                  --'罚息利率（8d/%）'
,t3.loan_active_date                                        --'放款日期'
,if(t3.loan_status = 'F','是','否') as is_settled           --'是否结清'
,t3.paid_out_date                                           --'结清日期'
,t3.loan_out_reason                                         --'借据终止原因'
,t3.paid_out_type                                           --'结清类型'
,t5.schedule as schedule_detail                             --'还款计划-包含该条借据观察日所有还款计划'
,t3.Loan_status                                             --'借据状态'
,t3.paid_amount                                             --'已还金额'
,t3.paid_principal                                          --'已还本金'
,t3.paid_interest                                           --'已还利息'
,t3.paid_penalty                                            --'已还罚息'
,t3.paid_svc_fee                                            --'已还服务费'
,t3.paid_term_fee                                           --'已还手续费'
,t3.paid_mult                                               --'已还滞纳金'
,t3.remain_amount                                           --'剩余金额：本息费'
,t3.remain_principal                                        --'剩余本金'
,t3.remain_interest                                         --'剩余利息'
,t3.remain_svc_fee                                          --'剩余服务费'
,t3.remain_term_fee                                         --'剩余手续费'
,t3.overdue_principal                                       --'逾期本金'
,t3.overdue_interest                                        --'逾期利息'
,t3.overdue_svc_fee                                         --'逾期服务费'
,t3.overdue_term_fee                                        --'逾期手续费'
,t3.overdue_penalty                                         --'逾期罚息'
,t3.overdue_mult_amt                                        --'逾期滞纳金'
,t3.overdue_date_first                                      --'首次逾期日期'
,t3.overdue_date_start                                      --'逾期起始日期'
,t3.overdue_days                                            --'逾期天数'
,t3.overdue_date                                            --'逾期日期'
,t3.dpd_begin_date                                          --'DPD起始日期'
,t3.dpd_days                                                --'DPD天数'
,t3.dpd_days_count                                          --'累计DPD天数'
,t3.dpd_days_max                                            --'历史最大DPD天数'
,t3.collect_out_date                                        --'出催日期'
,t3.overdue_term                                            --'当前逾期期数'
,t3.overdue_terms_count                                     --'累计逾期期数'
,t3.overdue_terms_max                                       --'历史单次最长逾期期数'
,t3.overdue_principal_accumulate                            --'累计逾期本金'
,t3.overdue_principal_max                                   --'历史最大逾期本金',
,'${ST9}' as biz_date
,t3.product_id                                              --'产品编号'

from
(
    select
     t1.*
    ,t2.loan_init_penalty_rate,
    loan_init_interest_rate
    from
    (
        select
        *
        from
        ods.loan_info
        where
        '${ST9}' between s_d_date
        and date_sub(e_d_date,1) and   product_id in (${product_id_list})
    ) t1
    join dim.dim_ht_bag_asset on t1.due_bill_no=dim_ht_bag_asset.due_bill_no and dim_ht_bag_asset.biz_date='2020-10-17'
    join (
    select
        due_bill_no,
        product_id,
        loan_init_penalty_rate,
        loan_init_interest_rate
    from ods.loan_lending
    where biz_date<='${ST9}' and   product_id in (${product_id_list})
    )t2
    on t1.due_bill_no = t2.due_bill_no
    and t1.product_id = t2.product_id
) t3
join
(
    select
    t1.due_bill_no
    ,concat('{',concat_ws(',',collect_list(concat('"',loan_term,'"',':','{',concat_ws(',',
     concat('"','schedule_id'              , '"', ':', '"',   'default'              ,'"')
    ,concat('"','due_bill_no'              , '"', ':', '"',   t1.due_bill_no           ,'"')
    ,concat('"','loan_active_date'         , '"', ':', '"',   loan_active_date         ,'"')
    ,concat('"','loan_init_principal'      , '"', ':', '"',   loan_init_principal      ,'"')
    ,concat('"','loan_init_term'           , '"', ':', '"',   loan_init_term           ,'"')
    ,concat('"','loan_term'                , '"', ':', '"',   loan_term                ,'"')
    ,concat('"','start_interest_date'      , '"', ':', '"',   start_interest_date      ,'"')
    ,concat('"','curr_bal'                 , '"', ':', '"',   curr_bal                 ,'"')
    ,concat('"','should_repay_date'        , '"', ':', '"',   should_repay_date        ,'"')
    ,concat('"','should_repay_date_history', '"', ':', '"',   should_repay_date_history,'"')
    ,concat('"','grace_date'               , '"', ':', '"',   grace_date               ,'"')
    ,concat('"','should_repay_amount'      , '"', ':', '"',   should_repay_amount      ,'"')
    ,concat('"','should_repay_principal'   , '"', ':', '"',   should_repay_principal   ,'"')
    ,concat('"','should_repay_interest'    , '"', ':', '"',   should_repay_interest    ,'"')
    ,concat('"','should_repay_term_fee'    , '"', ':', '"',   should_repay_term_fee    ,'"')
    ,concat('"','should_repay_svc_fee'     , '"', ':', '"',   should_repay_svc_fee     ,'"')
    ,concat('"','should_repay_penalty'     , '"', ':', '"',   should_repay_penalty     ,'"')
    ,concat('"','should_repay_mult_amt'    , '"', ':', '"',   should_repay_mult_amt    ,'"')
    ,concat('"','should_repay_penalty_acru', '"', ':', '"',   should_repay_penalty_acru,'"')
    ,concat('"','schedule_status'          , '"', ':', '"',   schedule_status          ,'"')
    ,concat('"','schedule_status_cn'       , '"', ':', '"',   schedule_status_cn       ,'"')
    ,concat('"','paid_out_date'            , '"', ':', '"',   paid_out_date            ,'"')
    ,concat('"','paid_out_type'            , '"', ':', '"',   paid_out_type            ,'"')
    ,concat('"','paid_out_type_cn'         , '"', ':', '"',   paid_out_type_cn         ,'"')
    ,concat('"','paid_amount'              , '"', ':', '"',   paid_amount              ,'"')
    ,concat('"','paid_principal'           , '"', ':', '"',   paid_principal           ,'"')
    ,concat('"','paid_interest'            , '"', ':', '"',   paid_interest            ,'"')
    ,concat('"','paid_term_fee'            , '"', ':', '"',   paid_term_fee            ,'"')
    ,concat('"','paid_svc_fee'             , '"', ':', '"',   paid_svc_fee             ,'"')
    ,concat('"','paid_penalty'             , '"', ':', '"',   paid_penalty             ,'"')
    ,concat('"','paid_mult'                , '"', ':', '"',   paid_mult                ,'"')
    ,concat('"','reduce_amount'            , '"', ':', '"',   reduce_amount            ,'"')
    ,concat('"','reduce_principal'         , '"', ':', '"',   reduce_principal         ,'"')
    ,concat('"','reduce_interest'          , '"', ':', '"',   reduce_interest          ,'"')
    ,concat('"','reduce_term_fee'          , '"', ':', '"',   reduce_term_fee          ,'"')
    ,concat('"','reduce_svc_fee'           , '"', ':', '"',   reduce_svc_fee           ,'"')
    ,concat('"','reduce_penalty'           , '"', ':', '"',   reduce_penalty           ,'"')
    ,concat('"','reduce_mult_amt'          , '"', ':', '"',   reduce_mult_amt          ,'"')
    ,concat('"','s_d_date'                 , '"', ':', '"',   s_d_date                 ,'"')
    ,concat('"','e_d_date'                 , '"', ':', '"',   e_d_date                 ,'"')
    ,concat('"','effective_time'           , '"', ':', '"',   'default'           ,'"')
    ,concat('"','expire_time'              , '"', ':', '"',   'default'              ,'"')
    ),'}'))),'}') as schedule
    from
    (
    select
    *
    from
    ods.repay_schedule
    where
    '${ST9}' between s_d_date and date_sub(e_d_date,1)  and  product_id in (${product_id_list}) and  loan_term>0
    ) t1
   join dim.dim_ht_bag_asset on t1.due_bill_no=dim_ht_bag_asset.due_bill_no and dim_ht_bag_asset.biz_date='2020-10-17'

    group by t1.due_bill_no
) t5
on t3.due_bill_no = t5.due_bill_no