--set hive.exec.parallel=true;
--set hive.exec.parallel.thread.number=8;
SET hivevar:ST9=2020-10-31;
set hivevar:product_id_list='001801','001802','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004',
'002005','002006','002007','001601','001602','001603';
--set hivevar:product_id_list='001801','001802';
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
-- 禁用 Hive 矢量执行
--set hive.vectorized.execution.enabled=false;
--set hive.vectorized.execution.reduce.enabled=false;
--set hive.vectorized.execution.reduce.groupby.enabled=false;


INSERT OVERWRITE TABLE eagle.one_million_random_loan_data_day PARTITION(biz_date = '${ST9}',product_id)
select
 t3.due_bill_no                                             --'借据编号'
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
,t3.overdue_principal_max                                   --'历史最大逾期本金',
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
        and date_sub(e_d_date,1) and   product_id in (${product_id_list}) and loan_status!='F'
    ) t1
    left join (
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
left join
(
    select
    distinct
    due_bill_no
    ,credit_coef
    ,product_id
    from
    ods.loan_apply
    where  product_id in (${product_id_list})
) t4
on t3.due_bill_no = t4.due_bill_no
and t3.product_id = t4.product_id
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
    group by t1.due_bill_no
) t5
on t3.due_bill_no = t5.due_bill_no
;




--乐信
insert overwrite table eagle.one_million_random_customer_reqdata partition (product_id)
select
    t1.due_bill_no          ,
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
    id_no.idcard_area          ,
    id_no.resident_area        ,
    t1.riskLevel,
     t1.product_id
from
(
    select
    distinct
    get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')                                       as due_bill_no,
    substring(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),1,6)                           as idno_addr
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
        from stage.ecas_msg_log
        where 1 > 0
        and msg_type = 'WIND_CONTROL_CREDIT'
        and original_msg is not null
        and deal_date <=date_add('${ST9}',20)
    ) loan_apply
    join (
    select
        due_bill_no
        from
        ods.loan_info
        where
        '${ST9}' between s_d_date
        and date_sub(e_d_date,1) and   product_id in (${product_id_list}) and loan_status!='F'
    )loan on  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')=loan.due_bill_no
    where  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.proCode') in (${product_id_list})
) t1
inner join
(
select
    idno_addr,
    idno_area_cn as idcard_area,
    idno_area_cn as resident_area
    from
    dim.dim_idno
)id_no
on t1.idno_addr=id_no.idno_addr;


-- 汇通
insert overwrite table eagle.one_million_random_customer_reqdata partition (product_id)
select
distinct
nms_apply.apply_no,
nms_apply.apply_amount                                       as creditLimit,
nms_apply.loan_rate                                          as creditCoef,
nms_apply.apply_amount                                       as availableCreditLimit,
education                                          as edu,
null                                               as degree               ,
null                                               as homests              ,
marital_status                                     as marriage   ,
null                                               as mincome              ,
null                                               as income               ,
null                                               as homeIncome           ,
null                                               as zxhomeIncome         ,
null                                               as custType             ,
null                                               as  workWay              ,
null                                               as workType             ,
null                                               as workDuty             ,
null                                               as workTitle            ,
loan_usage                                         as appUse               ,
null                                               as ifCar                ,
null                                               as ifCarCred            ,
null                                               as ifRoom               ,
null                                               as ifMort               ,
null                                               as ifCard               ,
null                                               as cardAmt              ,
null                                               as ifApp                ,
null                                               as ifId                 ,
null                                               as  ifPact               ,
null                                               as ifLaunder            ,
null                                               as launder              ,
null                                               as ifAgent              ,
null                                               as isBelowRisk          ,
null                                               as hasOverdueLoan       ,
id_no.idcard_area          ,
id_no.resident_area        ,
null as riskLevel           ,
nms_apply.product_no as product_id
from
(
      select
        substring(get_json_object(standard_req_msg,'$.borrower.id_no'),1,6)   as idno_addr,
        deal_date                                                                 as deal_date,
        datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')                           as create_time,
        datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss')                           as update_time,
        sha256(get_json_object(standard_req_msg,'$.borrower.id_no'),'idNumber',1) as id_no,
        sha256(get_json_object(standard_req_msg,'$.borrower.name'),'userName',1)  as name,
        get_json_object(standard_req_msg,'$.pre_apply_no')                        as pre_apply_no,
        get_json_object(standard_req_msg,'$.apply_no')                            as apply_no,
        get_json_object(standard_req_msg,'$.product.product_no')                  as product_no,
        get_json_object(standard_req_msg,'$.product.currency_amt')                as apply_amount,
        get_json_object(standard_req_msg,'$.product.loan_terms')                  as loan_terms,
        get_json_object(standard_req_msg,'$.product.loan_apply_use')              as loan_apply_use,
        get_json_object(standard_req_msg,'$.product.repay_type')                  as repay_type,
        get_json_object(standard_req_msg,'$.product.loan_rate')                   as loan_rate,
        get_json_object(standard_req_msg,'$.product.loan_penalty_rate')           as loan_penalty_rate,
        datefmt(substring(get_json_object(standard_req_msg,'$.borrower.id_no'),7,8),'yyyyMMdd','yyyy-MM-dd') as birthday,
        --standard_req_msg                                                          as standard_req_msg,
    case get_json_object(standard_req_msg,'$.borrower.education')
        when 'A' then '博士及以上'
        when 'B' then '硕士'
        when 'C' then '大学本科'
        when 'D' then '大学专科/专科学校'
        when 'E' then '高中/中专/技校'
        when 'F' then '初中'
        when 'G' then '初中以下'
        else get_json_object(standard_req_msg,'$.borrower.education')
        end                                                                                                 as education,
    case get_json_object(standard_req_msg,'$.borrower.marital_status')
        when 'C' then '已婚'
        when 'S' then '未婚'
        when 'O' then '其他'
        when 'D' then '离异'
        when 'P' then '丧偶'
        else get_json_object(standard_req_msg,'$.borrower.marital_status')
        end                                                                                                 as marital_status,
        get_json_object(standard_req_msg,'$.product.loan_apply_use')                                        as loan_usage
      from stage.nms_interface_resp_log
      where 1 > 0
        and sta_service_method_name = 'setupCustCredit'
        and standard_req_msg is not null
        and datefmt(update_time,'ms','yyyy-MM-dd') <= '${ST9}'
    ) as nms_apply
    left join (
      select
        nvl(due_bills['APPLY_NO'],         due_bills['apply_no'])                           as apply_no,
        nvl(due_bills['LOAN_AMT'],         due_bills['loan_amt'])                           as loan_amt,
        nvl(due_bills['LOAN_RATE'],        due_bills['loan_rate'])                          as loan_rate,
        nvl(due_bills['LOAN_PENALTY_RATE'],due_bills['loan_penalty_rate'])                  as loan_penalty_rate,
        nvl(due_bills['product_no']   ,due_bills['PRODUCT_NO'])                             as product_no,
        datefmt(nvl(due_bills['LOAN_DATE'],due_bills['loan_date']),'yyyyMMdd','yyyy-MM-dd') as loan_date,
        case resp_code when '0000' then 4 else 5 end                                        as apply_status,
        resp_desc                                                                           as apply_resut_msg,
        deal_date                                                                           as deal_date,
        regexp_replace(regexp_replace(regexp_replace(standard_req_msg,'\\\\',''),'\\\"\\\[','\\\['),'\\\]\\\"','\\\]') as standard_req_msg
      from stage.nms_interface_resp_log
      lateral view explode(json_array_to_array(nvl(get_json_object(standard_req_msg,'$.DUE_BILLS'),get_json_object(standard_req_msg,'$.due_bills')))) bills as due_bills
      where 1> 0
        and standard_req_msg is not null
        and sta_service_method_name = 'loanApply'
        and nvl(due_bills['APPLY_NO'],due_bills['apply_no']) != '7634562346454355'
        and datefmt(update_time,'ms','yyyy-MM-dd')  <= '${ST9}'
    ) as nms_loan
    on nms_apply.apply_no = nms_loan.apply_no
    inner join
    (
    select
        idno_addr,
        idno_area_cn as idcard_area,
        idno_area_cn as resident_area
        from
        dim.dim_idno
    )id_no
    on nms_apply.idno_addr=id_no.idno_addr;






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
    cast(row_number() over ( partition by biz_conf.project_id order by t1.due_bill_no asc ) as int) as rn,

    t1.biz_date,
    biz_conf.project_id,
    "0" as cycle_key
from
(select * from eagle.one_million_random_loan_data_day  where biz_date='${ST9}')t1
join eagle.one_million_random_customer_reqdata t2 on t1.due_bill_no = t2.due_bill_no and  t1.product_id = t2.product_id
join(select
max(if(col_name='project_id',col_val,null)) as project_id,
max(if(col_name='product_id',col_val,null)) as product_id
from dim.data_conf
where col_type='ac'
group by col_id
)biz_conf on t1.product_id =biz_conf.product_id;