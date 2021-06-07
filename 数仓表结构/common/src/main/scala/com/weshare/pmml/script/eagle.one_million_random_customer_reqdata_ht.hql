--set hive.exec.parallel=true;
--set hive.exec.parallel.thread.number=8;
--SET hivevar:ST9=2021-04-14;
SET hivevar:product_id_list='001601','001602','001603';
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
    on nms_apply.idno_addr=id_no.idno_addr
    join dim.dim_ht_bag_asset on nms_apply.apply_no=dim_ht_bag_asset.due_bill_no and dim_ht_bag_asset.biz_date='2020-10-17';



