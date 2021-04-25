set hive.execution.engine=mr;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
set mapred.job.queue.name=hive;
set spark.app.name="bill_cript";
SET hivevar:ST9=2021-01-31;
set mapreduce.input.fileinputformat.split.maxsize=1024000000;
--set hivevar:product_id_list='001801','001802','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';
set hivevar:product_id_list='001801','001802';
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;


INSERT OVERWRITE TABLE eagle.one_million_random_loan_data_day PARTITION(biz_date = '${ST9}')
select
 t3.due_bill_no                                             --'借据编号'
,t3.product_id                                              --'产品编号'
,null as risk_level                                         --'风险等级'
,t3.loan_init_principal                                     --'放款本金'
,t3.loan_init_term                                          --'贷款期数'
,t3.loan_init_interest_rate                                 --'利息利率（8d/%）'
,t4.credit_coef                                             --'综合融资成本（8d/%）'
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
,t3.overdue_principal_max                                   --'历史最大逾期本金'
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
        ods_new_s.loan_info
        where
        '${ST9}' between s_d_date
        and date_sub(e_d_date,1) and   product_id in (${product_id_list}) and loan_status!='F'
    ) t1
    left join (
    select
        due_bill_no,
        product_id,
        loan_init_penalty_rate,
        loan_init_interest_rate
    from ods_new_s.loan_lending
    where biz_date<='${ST9}' and   product_id in (${product_id_list})
    )t2
    on t1.due_bill_no = t2.due_bill_no
    and t1.product_id = t2.product_id
) t3
left join
(
    select
    distinct
    due_bill_no
    ,credit_coef
    ,product_id
    from
    ods_new_s.loan_apply
    where  product_id in (${product_id_list})
) t4
on t3.due_bill_no = t4.due_bill_no
and t3.product_id = t4.product_id
join
(
    select
    t1.due_bill_no
    ,concat('{',concat_ws(',',collect_list(concat('"',loan_term,'"',':','{',concat_ws(',',
     concat('"','schedule_id'              , '"', ':', '"',   schedule_id              ,'"')
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
    ,concat('"','effective_time'           , '"', ':', '"',   effective_time           ,'"')
    ,concat('"','expire_time'              , '"', ':', '"',   expire_time              ,'"')
    ),'}'))),'}') as schedule
    from
    (
    select
    *
    from
    ods_new_s.repay_schedule
    where
    '${ST9}' between s_d_date and date_sub(e_d_date,1)  and  product_id in (${product_id_list})
    ) t1
    group by t1.due_bill_no
) t5
on t3.due_bill_no = t5.due_bill_no
;

insert overwrite table eagle.one_million_random_customer_reqdata
select
    t1.due_bill_no          ,
    t1.product_id           ,
    t1.creditLimit          ,
    t1.creditCoef           ,
    t1.availableCreditLimit ,
    t1.edu                  ,
    t1.degree               ,
    t1.homests              ,
    t1.marriage             ,
    t1.mincome              ,
    t1.income               ,
    t1.homeIncome           ,
    t1.zxhomeIncome         ,
    t1.custType             ,
    t1.workWay              ,
    t1.workType             ,
    t1.workDuty             ,
    t1.workTitle            ,
    t1.appUse               ,
    t1.ifCar                ,
    t1.ifCarCred            ,
    t1.ifRoom               ,
    t1.ifMort               ,
    t1.ifCard               ,
    t1.cardAmt              ,
    t1.ifApp                ,
    t1.ifId                 ,
    t1.ifPact               ,
    t1.ifLaunder            ,
    t1.launder              ,
    t1.ifAgent              ,
    t1.isBelowRisk          ,
    t1.hasOverdueLoan       ,
    t2.idcard_area          ,
    t2.resident_area        ,
    t1.riskLevel
from
(
    select
    distinct
    get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')                                       as due_bill_no
    ,sha256(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),'idNumber',1)                    as user_hash_no
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.proCode')                                      as product_id
    ,cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.creditLimit' )         as decimal(10,2))  as creditLimit
    ,cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.creditCoef')           as decimal(10,2))  as creditCoef
    ,cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.availableCreditLimit') as decimal(10,2))  as availableCreditLimit
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.edu'                 )                         as edu
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.degree'              )                         as degree
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.homeSts'             )                         as homeSts
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.marriage'            )                         as marriage
    ,cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.mincome') as decimal(10,2))               as mincome
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.income'              )                         as income
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.homeIncome'          )                         as homeIncome
    ,cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.zxhomeIncome' )as decimal(10,2))          as zxhomeIncome
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.custType'            )                         as custType
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.workWay'             )                         as workWay
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.workType'            )                         as workType
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.workDuty'            )                         as workDuty
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.workTitle'           )                         as workTitle
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.appUse')                                       as appUse
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifCar')                                        as ifCar
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifCarCred')                                    as ifCarCred
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifRoom')                                       as ifRoom
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifMort')                                       as ifMort
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifCard')                                       as ifCard
    ,cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.cardAmt') as decimal(10,2))               as cardAmt
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifApp')                                        as ifApp
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifId')                                         as ifId
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifPact')                                       as ifPact
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifLaunder')                                    as ifLaunder
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.launder')                                      as launder
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.ifAgent')                                      as ifAgent
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.isBelowRisk')                                  as isBelowRisk
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.hasOverdueLoan')                               as hasOverdueLoan
    ,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.riskLevel')                                    as riskLevel
    from
    (
    select
        regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\\"\\\{','\\\{'),'\\\}\\\\\"','\\\}'),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\\\\\\\\\"','\\\"'),'\\\\\"','\\\"'),'\\\\\\\\','\\\\') as original_msg
        from ods.ecas_msg_log
        where 1 > 0
        and msg_type = 'WIND_CONTROL_CREDIT'
        and original_msg is not null
        and deal_date <='${ST9}'
    ) loan_apply
    join (
    select
        due_bill_no
        from
        ods_new_s.loan_info
        where
        '${ST9}' between s_d_date
        and date_sub(e_d_date,1) and   product_id in (${product_id_list}) and loan_status!='F'
    )loan on  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')=loan.due_bill_no
    where  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.proCode') in (${product_id_list})
) t1
inner join
(select user_hash_no,max(idcard_area) as idcard_area, max(resident_area) as resident_area from ods_new_s.customer_info where  product_id in (${product_id_list})  group by user_hash_no) t2
on t1.user_hash_no = t2.user_hash_no
;


insert overwrite table eagle.one_million_random_risk_data partition (biz_date,project_id,cycle_key)
select
    t1.due_bill_no                                          ,
    t1.product_id                                           ,
    t1.risk_level                                      ,
    t1.loan_init_principal                                  ,
    t1.loan_init_term                                       ,
    t1.loan_init_interest_rate                              ,
    t1.credit_coef                                          ,
    t1.loan_init_penalty_rate                               ,
    t1.loan_active_date                                     ,
    t1.settled          ,
    t1.paid_out_date                                        ,
    t1.loan_out_reason                                      ,
    t1.paid_out_type                                        ,
    t1.schedule as schedule_detail                          ,
    t1.Loan_status                                          ,
    t1.paid_amount                                          ,
    t1.paid_principal                                       ,
    t1.paid_interest                                        ,
    t1.paid_penalty                                         ,
    t1.paid_svc_fee                                         ,
    t1.paid_term_fee                                        ,
    t1.paid_mult                                            ,
    t1.remain_amount                                        ,
    t1.remain_principal                                     ,
    t1.remain_interest                                      ,
    t1.remain_svc_fee                                       ,
    t1.remain_term_fee                                      ,
    t1.overdue_principal                                    ,
    t1.overdue_interest                                     ,
    t1.overdue_svc_fee                                      ,
    t1.overdue_term_fee                                     ,
    t1.overdue_penalty                                      ,
    t1.overdue_mult_amt                                     ,
    t1.overdue_date_first                                   ,
    t1.overdue_date_start                                   ,
    t1.overdue_days                                         ,
    t1.overdue_date                                         ,
    t1.dpd_begin_date                                       ,
    t1.dpd_days                                             ,
    t1.dpd_days_count                                       ,
    t1.dpd_days_max                                         ,
    t1.collect_out_date                                     ,
    t1.overdue_term                                         ,
    t1.overdue_terms_count                                  ,
    t1.overdue_terms_max                                    ,
    t1.overdue_principal_accumulate                         ,
    t1.overdue_principal_max                                ,
    t2.creditLimit as creditlimit
    ,t2.edu  --
    ,t2.degree --
    ,t2.homests --
    ,t2.marriage --
    ,t2.mincome    --
    ,t2.homeIncome as homeincome
    ,t2.zxhomeIncome as zxhomeincome
    ,t2.custType  as custtype
    ,t2.workType
    ,t2.workDuty as workduty  --
    ,t2.workTitle as worktitle --
    ,t2.idcard_area
    ,t2.riskLevel
    ,'6' AS scoreRange,
    t1.biz_date,
    biz_conf.project_id,
    "0" as cycle_key
from
(
    select * from eagle.one_million_random_loan_data_day  where biz_date='${ST9}'
)t1
join
    eagle.one_million_random_customer_reqdata t2 on t1.due_bill_no = t2.due_bill_no and  t1.product_id = t2.product_id
    join dim_new.biz_conf biz_conf on t1.product_id =biz_conf.product_id
;