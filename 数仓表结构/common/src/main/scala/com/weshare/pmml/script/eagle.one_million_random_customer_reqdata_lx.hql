--set hive.exec.parallel=true;
--set hive.exec.parallel.thread.number=8;
--SET hivevar:ST9=2021-04-30;
SET hivevar:product_id_list='001801','001802';
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

set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
set hive.auto.convert.join.noconditionaltask=false;
set hive.auto.convert.join=false;
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

