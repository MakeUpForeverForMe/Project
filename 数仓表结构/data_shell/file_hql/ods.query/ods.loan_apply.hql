set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000;      -- 64M
set hive.merge.smallfiles.avgsize=128000000; -- 64M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;

set hive.auto.convert.join=false;

---------------------------------【增量跑】-------------------------------------
-- 滴滴
--set hivevar:where_date=and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}';
--预计执行时间 2min

insert overwrite table ods.loan_apply partition(biz_date,product_id)
select distinct *
from (
  select
    concat_ws('_','10000',loan_apply.id_no,loan_apply.name) as cust_id,
    loan_apply.id_no                                 as user_hash_no,
    customer_info.birthday                           as birthday,
    age_birth(
      customer_info.birthday,
      is_empty(
        to_date(loan_result.issuetime),
        loan_result.deal_date,
        least(to_date(nvl(loan_result.issuetime,'9999-99-99')),to_date(nvl(loan_apply.create_time,'9999-99-99')))
      )
    )                                                as age,
    null                                             as pre_apply_no,
    loan_apply.credit_id                             as apply_id,
    loan_apply.loan_order_id                         as due_bill_no,
    cast(least(nvl(loan_result.issuetime,'9999-99-99 00:00:00'),nvl(loan_apply.create_time,'9999-99-99 00:00:00')) as timestamp) as loan_apply_time,
    loan_apply.loan_amount                           as loan_amount_apply,
    cast(loan_apply.loan_terms as decimal(3,0))      as loan_terms,
    is_empty(loan_apply.loan_usage)                  as loan_usage,
    case loan_apply.loan_usage
      when 1 then '日常消费'
      when 2 then '汽车加油'
      when 3 then '修车保养'
      when 4 then '购买汽车'
      when 5 then '医疗支出'
      when 6 then '教育深造'
      when 7 then '房屋装修'
      when 8 then '旅游出行'
      when 9 then '其他消费'
      else is_empty(loan_apply.loan_usage,'未知')
    end                                              as loan_usage_cn,
    loan_apply.repay_type                            as repay_type,
    case loan_apply.repay_type
      when 1 then '等额本金'
      when 2 then '等额本息'
      else loan_apply.repay_type
    end                                              as repay_type_cn,
    loan_apply.loan_rating                           as interest_rate,
    loan_apply.loan_rating                           as credit_coef,
    loan_apply.penalty_interest_rate                 as penalty_rate,
    cast(nvl(loan_result.status,5) as decimal(2,0))  as apply_status,
    nvl(loan_result.message,'用信失败 - 自填')       as apply_resut_msg,
    cast(loan_result.issuetime as timestamp)         as issue_time,
    cast(if(loan_result.status = 1,loan_result.loan_amount,0) as decimal(15,4)) as loan_amount_approval,
    cast(if(loan_result.status = 1,loan_result.loan_amount,0) as decimal(15,4)) as loan_amount,
    null                                             as risk_level,
    null                                             as risk_score,
    loan_apply.original_msg                          as ori_request,
    loan_result.original_msg                         as ori_response,
    loan_apply.create_time                           as create_time,
    loan_apply.update_time                           as update_time,
    cast(least(nvl(to_date(loan_result.issuetime),'9999-99-99'),loan_apply.deal_date) as string) as biz_date,
    loan_apply.product_id                            as product_id
  from (
    select
      'DIDI201908161538'                                                                                               as product_id,
      deal_date                                                                                                        as deal_date,
      datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')                                                                  as create_time,
      datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss')                                                                  as update_time,
      sha256(get_json_object(original_msg,'$.idNo'),'idNumber',1)                                                      as id_no,
      sha256(get_json_object(original_msg,'$.name'),'userName',1)                                                      as name,
      get_json_object(original_msg,'$.creditId')                                                                       as credit_id,
      get_json_object(original_msg,'$.loanOrderId')                                                                    as loan_order_id,
      cast(get_json_object(original_msg,'$.loanAmount') / 100 as decimal(10,4))                                        as loan_amount,
      is_empty(get_json_object(original_msg,'$.totalInstallment'),0)                                                   as loan_terms,
      get_json_object(original_msg,'$.loanUsage')                                                                      as loan_usage,
      get_json_object(original_msg,'$.repayType')                                                                      as repay_type,
      cast(is_empty(get_json_object(original_msg,'$.loanRating'),0) / 1000000 * 365 as decimal(10,8))                  as loan_rating,
      cast(is_empty(get_json_object(original_msg,'$.penaltyInterestRate'),0) / 1000000 * 365 as decimal(10,8))         as penalty_interest_rate,
      regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"') as original_msg
    from stage.ecas_msg_log
    where 1 > 0
      and msg_type = 'LOAN_APPLY'
      and original_msg is not null
      ---${where_date}
  ) as loan_apply
  left join (
    select
      deal_date                                          as deal_date,
      get_json_object(original_msg,'$.loanOrderId')      as loan_order_id,
      get_json_object(original_msg,'$.issueTime')        as issuetime,
      case get_json_object(original_msg,'$.status')
        when '1' then 1
        when '3' then 3
        else 2
      end                                                as status,
      get_json_object(original_msg,'$.message')          as message,
      get_json_object(original_msg,'$.loanAmount') / 100 as loan_amount,
      original_msg                                       as original_msg
    from stage.ecas_msg_log
    where 1 > 0
      and original_msg is not null
      and msg_type='LOAN_RESULT'
  ) as loan_result
  on loan_apply.loan_order_id = loan_result.loan_order_id
  left join (
    select
      get_json_object(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"'),'$.applicationId') as credit_id,
      is_empty(
        datefmt(substring(get_json_object(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"'),'$.userInfo.ocrInfo.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd'),
        get_json_object(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"'),'$.userInfo.ocrInfo.birthday')
      ) as birthday
    from stage.ecas_msg_log
    where msg_type = 'CREDIT_APPLY'
      and original_msg is not null
  ) as customer_info
  on loan_apply.credit_id = customer_info.credit_id
) as tmp
-- limit 1
;




-- 汇通
---set hivevar:where_date=and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}';
--预计执行时间 74s

insert overwrite table ods.loan_apply partition(biz_date,product_id)
select
  cust_id,
  user_hash_no,
  birthday,
  age,
  pre_apply_no,
  apply_id,
  due_bill_no,
  loan_apply_time,
  loan_amount_apply,
  loan_terms,
  loan_usage,
  loan_usage_cn,
  repay_type,
  repay_type_cn,
  interest_rate,
  credit_coef,
  penalty_rate,
  apply_status,
  apply_resut_msg,
  issue_time,
  loan_amount_approval,
  loan_amount,
  risk_level,
  risk_score,
  ori_request,
  ori_response,
  create_time,
  update_time,
  biz_date,
  product_id
from (
  select
    tmp.*,
    row_number() over(partition by due_bill_no,product_id order by biz_date) as rn
  from (
    select
      concat_ws('_','0003',nms_apply.id_no,nms_apply.name)                          as cust_id,
      nms_apply.id_no                                                                            as user_hash_no,
      nms_apply.birthday                                                                         as birthday,
      age_birth(
        nms_apply.birthday,
        is_empty(
          to_date(nms_loan.loan_date),
          nms_loan.deal_date,
          nms_apply.deal_date
        )
      )                                                                                          as age,
      nms_apply.pre_apply_no                                                                     as pre_apply_no,
      nms_apply.apply_no                                                                         as apply_id,
      nms_apply.apply_no                                                                         as due_bill_no,
      cast(nvl(nms_apply.deal_date,nms_loan.deal_date) as timestamp)                             as loan_apply_time,
      cast(nms_apply.apply_amount as decimal(15,4))                                              as loan_amount_apply,
      cast(nms_apply.loan_terms as decimal(3,0))                                                 as loan_terms,
      is_empty(nms_apply.loan_apply_use)                                                         as loan_usage,
      case nms_apply.loan_apply_use
        when 'LAU99' then '其他类消费'
        when 'LAU01' then '购车'
        when 'LAU02' then '购房'
        when 'LAU03' then '医疗'
        when 'LAU04' then '国内教育'
        when 'LAU05' then '出境留学'
        when 'LAU06' then '装修'
        when 'LAU07' then '婚庆'
        when 'LAU08' then '旅游'
        when 'LAU09' then '租赁'
        when 'LAU10' then '美容'
        when 'LAU11' then '家具'
        when 'LAU12' then '生活用品'
        when 'LAU13' then '家用电器'
        when 'LAU14' then '数码产品'
        else is_empty(nms_apply.loan_apply_use,'未知')
      end                                                                                        as loan_usage_cn,
      nms_apply.repay_type                                                                       as repay_type,
      case nms_apply.repay_type
        when 'RT01' then '等额本息'
        when 'RT02' then '等额本金'
        when 'RT03' then '等本等息'
        when 'RT04' then '一次还本付息'
        when 'RT05' then '按月付息-到期一次性还本'
        when 'RT06' then '循环授信-随借随还'
        when 'RT07' then '循环授信-随借随还'
        when 'RT08' then '循环授信-随借随还'
        else nms_apply.repay_type
      end                                                                                        as repay_type_cn,
      cast(nvl(nms_apply.loan_rate,nms_loan.loan_rate) as decimal(15,8))                         as interest_rate,
      cast(nvl(nms_apply.loan_rate,nms_loan.loan_rate) as decimal(15,8))                         as credit_coef,
      cast(nms_apply.loan_penalty_rate as decimal(15,8))                                         as penalty_rate,
      cast(nvl(nms_loan.apply_status,5) as decimal(2,0))                                         as apply_status,
      nvl(nms_loan.apply_resut_msg,'放款失败')                                                   as apply_resut_msg,
      cast(nms_loan.loan_date as timestamp)                                                      as issue_time,
      case nms_loan.apply_status when 4 then cast(nms_loan.loan_amt as decimal(15,8)) else 0 end as loan_amount_approval,
      cast(nms_loan.loan_amt as decimal(15,8))                                                   as loan_amount,
      null                                                                                       as risk_level,
      null                                                                                       as risk_score,
      nms_apply.standard_req_msg                                                                 as ori_request,
      nms_loan.standard_req_msg                                                                  as ori_response,
      nms_apply.create_time                                                                      as create_time,
      nms_apply.update_time                                                                      as update_time,
      nms_apply.deal_date                                                                        as biz_date,
      nms_apply.product_no                                                                       as product_id
    from (
      select
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
        standard_req_msg                                                          as standard_req_msg
      from stage.nms_interface_resp_log
      where 1 > 0
        and sta_service_method_name = 'setupCustCredit'
        and standard_req_msg is not null
        ---${where_date}
    ) as nms_apply
    left join (
      select
        nvl(due_bills['APPLY_NO'],         due_bills['apply_no'])                           as apply_no,
        nvl(due_bills['LOAN_AMT'],         due_bills['loan_amt'])                           as loan_amt,
        nvl(due_bills['LOAN_RATE'],        due_bills['loan_rate'])                          as loan_rate,
        nvl(due_bills['LOAN_PENALTY_RATE'],due_bills['loan_penalty_rate'])                  as loan_penalty_rate,
        nvl(due_bills['product_no'],       due_bills['PRODUCT_NO'])                         as product_no,
        datefmt(nvl(due_bills['LOAN_DATE'],due_bills['loan_date']),'yyyyMMdd','yyyy-MM-dd') as loan_date,
        case resp_code
          when '0000' then 4
          else 5
        end                                                                                 as apply_status,
        resp_desc                                                                           as apply_resut_msg,
        deal_date                                                                           as deal_date,
        regexp_replace(regexp_replace(regexp_replace(standard_req_msg,'\\\\',''),'\\\"\\\[','\\\['),'\\\]\\\"','\\\]') as standard_req_msg
      from stage.nms_interface_resp_log
      lateral view explode(json_array_to_array(nvl(get_json_object(standard_req_msg,'$.DUE_BILLS'),get_json_object(standard_req_msg,'$.due_bills')))) bills as due_bills
      where 1> 0
        and standard_req_msg is not null
        and sta_service_method_name = 'loanApply'
        and nvl(due_bills['APPLY_NO'],due_bills['apply_no']) != '7634562346454355'
    ) as nms_loan
    on nms_apply.apply_no = nms_loan.apply_no
    ) as tmp
  ) as temp
where temp.rn = 1
-- limit 1
;





-- 乐信
set hivevar:where_date=and deal_date between date_sub('${ST9}',2) and '${ST9}';
--预计执行时间 251s
--explain
insert into table ods.loan_apply partition(biz_date,product_id)
select
  concat_ws('_','0006',sha256(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),'idNumber',1),sha256(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.custName'),'userName',1)) as cust_id,
  sha256(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),'idNumber',1)       as user_hash_no,
  is_empty(
    datefmt(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.birth'),'yyyyMMdd','yyyy-MM-dd'),
    datefmt(substring(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd')
  )                                                                                                               as birthday,
  age_birth(
    is_empty(
      datefmt(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.birth'),'yyyyMMdd','yyyy-MM-dd'),
      datefmt(substring(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd')
    ),
    is_empty(
      ecas_loan.active_date,
      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanDate')
    )
  )                                                                                                               as age,
  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')                         as pre_apply_no,
  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')                         as apply_id,
  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')                         as due_bill_no,
  cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanTime') as timestamp)     as loan_apply_time,
  cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.pactAmt') as decimal(15,4))  as loan_amount_apply,
  cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanTerm') as decimal(3,0))  as loan_terms,
  is_empty(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.appUse'))                as loan_usage,
  case get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.appUse')
    when '01' then '企业经营'
    when '02' then '购置车辆'
    when '03' then '装修'
    when '04' then '子女教育(出国留学)'
    when '05' then '购置古玩字画'
    when '06' then '股票/期货等金融工具投资'
    when '07' then '其他消费类'
    else is_empty(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.appUse'),'未知')
  end                                                                                                             as loan_usage_cn,
  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.rpyMethod')                       as repay_type,
  case get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.rpyMethod')
    when '01' then '等本等息'
    when '02' then '随借随还'
    when '03' then '等额本息'
    else is_empty(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.rpyMethod'))
  end                                                                                                             as repay_type_cn,
  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.lnRate') * 12 /100                as interest_rate,
  cast(
    is_empty(
      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.creditCoef'),
      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.lnRate')
    ) as decimal(15,8)
  ) * 12 /100                                                                                                     as credit_coef,
  0                                                                                                               as penalty_rate,
  case
    when ecas_loan.loan_init_prin is not null then 1
    when get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass') = 'Yes' then 4
    when get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass') != 'Yes' then 5
    else 2
  end                                                                                                             as apply_status,
  case
    when ecas_loan.loan_init_prin is not null then '放款成功'
    when get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass') = 'Yes' then '用信通过'
    when get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass') != 'Yes' then '用信失败'
    else '放款失败'
  end                                                                                                             as apply_resut_msg,
  cast(nvl(ecas_loan.active_date,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanTime')) as timestamp) as issue_time,
  case get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass')
    when 'Yes' then cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.pactAmt') as decimal(15,4))
    else 0
  end                                                                                                             as loan_amount_approval,
  cast(is_empty(ecas_loan.loan_init_prin,0) as decimal(15,4))                                                     as loan_amount,
  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.scoreLevel')                      as risk_level,
  null                                                                                                            as risk_score,
  loan_apply.original_msg                                                                                         as ori_request,
  null                                                                                                            as ori_response,
  loan_apply.create_time                                                                                          as create_time,
  loan_apply.update_time                                                                                          as update_time,
  get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanDate')                        as biz_date,
  loan_apply.product_id                                                                                           as product_id
from (
select
t.create_time
,t.update_time
,t.original_msg
,get_json_object(t.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo') as due_bill_no
,get_json_object(t.original_msg,'$.reqContent.jsonReq.content.reqData.proCode') as product_id
from
(
  select
    datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
    datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
    replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\\"\\\{','\\\{'),'\\\}\\\\\"','\\\}'),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\\\\\\\\\"','\\\"'),'\\\\\"','\\\"'),'\\\\\\\\','\\\\'),'\\\\\"',"") as original_msg
  from stage.ecas_msg_log
  where 1 > 0
    and msg_type = 'WIND_CONTROL_CREDIT'
    and original_msg is not null
    and is_his='N'
    ${where_date}
    ) t
) as loan_apply
left join (
  select distinct
    due_bill_no,
    loan_init_prin,
    active_date,
    product_code
  from stage.ecas_loan
  where 1 > 0
    and p_type in ('lx','lx2','lxzt','lx3')
    and d_date between date_sub(current_date,2) and current_date
) as ecas_loan
on  loan_apply.due_bill_no = ecas_loan.due_bill_no
and loan_apply.product_id = ecas_loan.product_code
;





-- 瓜子
---set hivevar:where_date=and datefmt(update_time,'ms','yyyy-MM-dd') = '${ST9}';
--预计执行时间 40s

insert overwrite table ods.loan_apply partition(biz_date,product_id)
select distinct *
from (
  select
    concat_ws('_',biz_conf.channel_id,sha256(get_json_object(credit_apply.original_msg,'$.data.borrower.idNo'),'idNumber',1),sha256(get_json_object(credit_apply.original_msg,'$.data.borrower.name'),'userName',1))                      as cust_id,
    sha256(get_json_object(credit_apply.original_msg,'$.data.borrower.idNo'),'idNumber',1)                                                      as user_hash_no,
    datefmt(substring(get_json_object(loan_apply.original_msg,'$.data.borrower.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd')                             as birthday,
    age_birth(
      datefmt(substring(get_json_object(loan_apply.original_msg,'$.data.borrower.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd'),
      is_empty(
        to_date(loan_result.issue_time),
        loan_result.deal_date,
        least(
          to_date(loan_apply.create_time),
          datefmt(get_json_object(loan_apply.original_msg,'$.timeStamp'),'ms','yyyy-MM-dd')
        )
      )
    )                                                                                                                                           as age,
    get_json_object(credit_apply.original_msg,'$.data.preApplyNo')                                                                              as pre_apply_no,
    get_json_object(loan_apply.original_msg,'$.data.applyNo')                                                                                   as apply_id,
    loan_result.due_bill_no                                                                                                                     as due_bill_no,
    cast(least(loan_apply.create_time,datefmt(get_json_object(loan_apply.original_msg,'$.timeStamp'),'ms','yyyy-MM-dd HH:mm:ss')) as timestamp) as loan_apply_time,
    cast(get_json_object(credit_apply.original_msg,'$.data.product.currencyAmt') as decimal(15,4))                                              as loan_amount_apply,
    cast(get_json_object(credit_apply.original_msg,'$.data.product.loanTerms') as decimal(3,0))                                                 as loan_terms,
    is_empty(get_json_object(credit_apply.original_msg,'$.data.product.loanApplyUse'))                                                          as loan_usage,
    case get_json_object(credit_apply.original_msg,'$.data.product.loanApplyUse')
      when 'LAU01' then '购车'
      when 'LAU02' then '购房'
      when 'LAU03' then '医疗'
      when 'LAU04' then '国内教育'
      when 'LAU05' then '出境留学'
      when 'LAU06' then '装修'
      when 'LAU07' then '婚庆'
      when 'LAU08' then '旅游'
      when 'LAU09' then '租赁'
      when 'LAU10' then '美容'
      when 'LAU11' then '家具'
      when 'LAU12' then '生活用品'
      when 'LAU13' then '家用电器'
      when 'LAU14' then '数码产品'
      when 'LAU15' then '资金周转'
      when 'LAU16' then '企业经营'
      when 'LAU17' then '日常消费'
      when 'LAU99' then '其他类消费'
      else is_empty(get_json_object(credit_apply.original_msg,'$.data.product.loanApplyUse'),'未知')
    end                                                                                                                                         as loan_usage_cn,
    get_json_object(credit_apply.original_msg,'$.data.product.repayType')                                                                       as repay_type,
    case get_json_object(credit_apply.original_msg,'$.data.product.repayType')
      when 'RT01' then '等额本息'
      else get_json_object(credit_apply.original_msg,'$.data.product.repayType')
    end                                                                                                                                         as repay_type_cn,
    cast(get_json_object(credit_apply.original_msg,'$.data.product.loanRate') as decimal(15,8))                                                 as interest_rate,
    cast(get_json_object(credit_apply.original_msg,'$.data.product.loanRate') as decimal(15,8))                                                 as credit_coef,
    cast(get_json_object(credit_apply.original_msg,'$.data.product.loanPenaltyRate') as decimal(15,8))                                          as penalty_rate,
    nvl(loan_result.apply_status,5)                                                                                                             as apply_status,
    nvl(loan_result.apply_resut_msg,'用信失败 - 自填')                                                                                          as apply_resut_msg,
    loan_result.issue_time                                                                                                                      as issue_time,
    case loan_result.apply_status
      when 4 then cast(get_json_object(credit_apply.original_msg,'$.data.product.loanAmt') as decimal(15,4))
      else 0
    end                                                                                                                                         as loan_amount_approval,
    cast(get_json_object(credit_apply.original_msg,'$.data.product.loanAmt') as decimal(15,4))                                                  as loan_amount,
    null                                                                                                                                        as risk_level,
    null                                                                                                                                        as risk_score,
    credit_apply.original_msg                                                                                                                   as ori_request,
    loan_result.original_msg                                                                                                                    as ori_response,
    least(loan_apply.create_time,datefmt(get_json_object(loan_apply.original_msg,'$.timeStamp'),'ms','yyyy-MM-dd HH:mm:ss'))                    as create_time,
    loan_apply.update_time                                                                                                                      as update_time,
    cast(
      least(
        to_date(loan_apply.create_time),
        datefmt(get_json_object(loan_apply.original_msg,'$.timeStamp'),'ms','yyyy-MM-dd')
      ) as string
    )                                                                                                                                           as biz_date,
    get_json_object(credit_apply.original_msg,'$.data.product.productNo')                                                                       as product_id
  from (
    select
      deal_date                                       as deal_date,
      datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
      datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
      regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"') as original_msg
    from stage.ecas_msg_log
    where 1 > 0
      and msg_type = 'GZ_LOAN_APPLY'
      and original_msg is not null
      ---${where_date}
  ) as loan_apply
  left join (
    select
      deal_date                                                                as deal_date,
      get_json_object(original_msg,'$.applyNo')                                as apply_no,
      case get_json_object(original_msg,'$.status') when '4' then 4 else 5 end as apply_status,
      original_msg                                                             as original_msg,
      is_empty(
        get_json_object(original_msg,'$.message'),
        case get_json_object(original_msg,'$.status')
          when '1' then '复核通过'
          when '2' then '复核拒绝'
          when '4' then '用信成功'
          when '5' then '用信失败'
          when '7' then '申请拒绝'
          else '未知原因'
        end
      )                                                                        as apply_resut_msg,
      cast(get_json_object(original_msg,'$.planDate') as timestamp)            as issue_time,
      get_json_object(original_msg,'$.loanNo')                                 as due_bill_no
    from stage.ecas_msg_log
    where 1 > 0
      and msg_type = 'GZ_LOAN_RESULT'
      and original_msg is not null
  ) as loan_result
  on get_json_object(loan_apply.original_msg,'$.data.applyNo') = loan_result.apply_no
  left join (
    select regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"') as original_msg
    from stage.ecas_msg_log
    where 1 > 0
      and msg_type = 'GZ_CREDIT_APPLY'
      and original_msg is not null
  ) as credit_apply
  on get_json_object(loan_apply.original_msg,'$.data.applyNo') = get_json_object(credit_apply.original_msg,'$.data.applyNo')
  left join (
    select distinct
      product_id as dim_product_id,
      channel_id
    from (
      select
        max(if(col_name = 'product_id',  col_val,null)) as product_id,
        max(if(col_name = 'channel_id',  col_val,null)) as channel_id
      from dim.data_conf
      where col_type = 'ac'
      group by col_id
    ) as tmp
  ) as biz_conf
  on get_json_object(credit_apply.original_msg,'$.data.product.productNo') = biz_conf.dim_product_id
) as tmp
-- limit 1
;




-- 新核心 乐信云信
msck repair table stage.kafka_credit_msg;
insert overwrite table ods.loan_apply partition(biz_date,product_id)
  select
    distinct *
  from (
    select
      concat_ws('_',biz_conf.channel_id,sha256(loan_apply.reqdata["idNo"],'idNumber',1),sha256(loan_apply.reqdata["custName"],'userName',1)) as cust_id,
      sha256(loan_apply.reqdata["idNo"],'idNumber',1)                                                     as user_hash_no,
      is_empty(
        datefmt(loan_apply.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
        datefmt(substring(loan_apply.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
      )                                                                                                   as birthday,
      age_birth(
        is_empty(
          datefmt(loan_apply.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
          datefmt(substring(loan_apply.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
        ),
        to_date(nvl(loan_contract.active_date,loan_apply.reqdata["loanDate"]))
      )                                                                                                   as age,
      loan_apply.reqdata["applyNo"]                                                                       as pre_apply_no,
      loan_apply.reqdata["applyNo"]                                                                       as apply_id,
      loan_contract.due_bill_no                                                                           as due_bill_no,
      cast(loan_apply.reqdata["loanDate"] as timestamp)                                                   as loan_apply_time,
      cast(loan_apply.reqdata["pactAmt"] as decimal(15,4))                                                as loan_amount_apply,
      cast(loan_apply.reqdata["loanTerm"] as decimal(3,0))                                                as loan_terms,
      is_empty(loan_apply.reqdata["appUse"])                                                              as loan_usage,
      case loan_apply.reqdata["appUse"]
        when '01' then '企业经营'
        when '02' then '购置车辆'
        when '03' then '装修'
        when '04' then '子女教育(出国留学)'
        when '05' then '购置古玩字画'
        when '06' then '股票/期货等金融工具投资'
        when '07' then '其他消费类'
        when '08' then '医美消费'
        else is_empty(loan_apply.reqdata["appUse"],'未知')
      end                                                                                                 as loan_usage_cn,
      loan_apply.reqdata["rpyMethod"]                                                                     as repay_type,
      case loan_apply.reqdata["rpyMethod"]
        when '01' then '等本等息'
        when '02' then '随借随还'
        when '03' then '等额本息'
        when '04' then '等本等费'
        else is_empty(loan_apply.reqdata["rpyMethod"])
      end                                                                                                 as repay_type_cn,
      loan_apply.reqdata["lnRate"] * 12 / 100                                                             as interest_rate,
      cast(nvl(loan_apply.reqdata["creditCoef"],loan_apply.reqdata["lnRate"])as decimal(15,8)) * 12 / 100 as credit_coef,
      0                                                                                                   as penalty_rate,
      case
        when loan_contract.loan_init_prin is not null then 1
        when loan_apply.resdata["pass"] = 'Yes' then 4
        when loan_apply.resdata["pass"] != 'Yes' then 5
        else 2
      end                                                                                                 as apply_status,
      case
        when loan_contract.loan_init_prin is not null then '放款成功'
        when loan_apply.resdata["pass"] = 'Yes' then '用信通过'
        when loan_apply.resdata["pass"] != 'Yes' then '用信失败'
        else '放款失败'
      end                                                                                                 as apply_resut_msg,
      cast(nvl(loan_contract.active_date,loan_apply.reqdata["loanDate"]) as timestamp)                    as issue_time,
      case loan_apply.resdata["pass"]
        when 'Yes' then cast(loan_apply.reqdata["pactAmt"] as decimal(15,4))
        else 0
      end                                                                                                 as loan_amount_approval,
      cast(is_empty(loan_contract.loan_init_prin,0) as decimal(15,4))                                     as loan_amount,
      loan_apply.reqdata["scoreLevel"]                                                                    as risk_level,
      null                                                                                                as risk_score,
      loan_apply.ori_request                                                                              as ori_request,
      null                                                                                                as ori_response,
      loan_apply.create_time                                                                              as create_time,
      loan_apply.update_time                                                                              as update_time,
      loan_apply.reqdata["loanDate"]                                                                      as biz_date,
      loan_apply.reqdata["proCode"]                                                                       as product_id
    from (
      select
        reqdata["loanDate"] as create_time,
        reqdata["loanDate"] as update_time,
        reqdata,
        resdata,
        concat('{"', concat_ws('","', collect_list(concat_ws('":"', k,v) ) ), '"}') as ori_request
      from stage.kafka_credit_msg
      lateral view outer explode(reqdata) kv as k,v
      where batch_date between date_sub('${ST9}',7) and '${ST9}'
        and p_type = 'WS0013200001'
        and interfacename = 'WIND_CONTROL_CREDIT'
      group by reqdata["loanDate"],reqdata,resdata
    ) as loan_apply
    left join (
      select
        apply_no,
        due_bill_no,
        product_no,
        contract_amount as loan_init_prin,
        actual_loan_date as active_date
      from (
        select
          *,
          row_number() over(partition by due_bill_no order by batch_date desc) as rn
        from stage.loan_contract
        where d_date <= '${ST9}'
          and p_type = 'WS0013200001'
      ) as tmp
      where rn = 1
    ) as loan_contract
    on  loan_apply.reqdata["proCode"] = loan_contract.product_no
    and loan_apply.reqdata["applyNo"] = loan_contract.apply_no
    left join (
      select distinct
        product_id as dim_product_id,
        channel_id
      from (
        select
          max(if(col_name = 'product_id',  col_val,null)) as product_id,
          max(if(col_name = 'channel_id',  col_val,null)) as channel_id
        from dim.data_conf
        where col_type = 'ac'
        group by col_id
      ) as tmp
    ) as biz_conf
    on loan_apply.reqdata["proCode"] = biz_conf.dim_product_id
  ) as tmp
;





-- 百度医美
insert overwrite table ods.loan_apply partition(biz_date,product_id)
  select
    distinct *
  from (
    select
      concat_ws('_',biz_conf.channel_id,sha256(loan_apply.reqdata["idNo"],'idNumber',1),sha256(loan_apply.reqdata["custName"],'userName',1)) as cust_id,
      sha256(loan_apply.reqdata["idNo"],'idNumber',1)                                                     as user_hash_no,
      is_empty(
        datefmt(loan_apply.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
        datefmt(substring(loan_apply.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
      )                                                                                                   as birthday,
      age_birth(
        is_empty(
          datefmt(loan_apply.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
          datefmt(substring(loan_apply.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
        ),
        to_date(nvl(loan_contract.active_date,loan_apply.reqdata["loanDate"]))
      )                                                                                                   as age,
      loan_apply.reqdata["applyNo"]                                                                       as pre_apply_no,
      loan_apply.reqdata["applyNo"]                                                                       as apply_id,
      loan_contract.due_bill_no                                                                           as due_bill_no,
      cast(loan_apply.reqdata["loanDate"] as timestamp)                                                   as loan_apply_time,
      cast(loan_apply.reqdata["pactAmt"] as decimal(15,4))                                                as loan_amount_apply,
      cast(loan_apply.reqdata["loanTerm"] as decimal(3,0))                                                as loan_terms,
      is_empty(loan_apply.reqdata["appUse"])                                                              as loan_usage,
      case loan_apply.reqdata["appUse"]
        when '01' then '企业经营'
        when '02' then '购置车辆'
        when '03' then '装修'
        when '04' then '子女教育(出国留学)'
        when '05' then '购置古玩字画'
        when '06' then '股票/期货等金融工具投资'
        when '07' then '其他消费类'
        when '08' then '医美消费'
        else is_empty(loan_apply.reqdata["appUse"],'未知')
      end                                                                                                 as loan_usage_cn,
      loan_apply.reqdata["rpyMethod"]                                                                     as repay_type,
      case loan_apply.reqdata["rpyMethod"]
        when '01' then '等本等息'
        when '02' then '随借随还'
        when '03' then '等额本息'
        when '04' then '等本等费'
        else is_empty(loan_apply.reqdata["rpyMethod"])
      end                                                                                                 as repay_type_cn,
      loan_apply.reqdata["lnRate"] * 12 / 100                                                             as interest_rate,
      cast(nvl(loan_apply.reqdata["creditCoef"],loan_apply.reqdata["lnRate"])as decimal(15,8)) * 12 / 100 as credit_coef,
      0                                                                                                   as penalty_rate,
      case
        when loan_contract.loan_init_prin is not null then 1
        when loan_apply.resdata["pass"] = 'Yes' then 4
        when loan_apply.resdata["pass"] != 'Yes' then 5
        else 2
      end                                                                                                 as apply_status,
      case
        when loan_contract.loan_init_prin is not null then '放款成功'
        when loan_apply.resdata["pass"] = 'Yes' then '用信通过'
        when loan_apply.resdata["pass"] != 'Yes' then '用信失败'
        else '放款失败'
      end                                                                                                 as apply_resut_msg,
      cast(nvl(loan_contract.active_date,loan_apply.reqdata["loanDate"]) as timestamp)                    as issue_time,
      case loan_apply.resdata["pass"]
        when 'Yes' then cast(loan_apply.reqdata["pactAmt"] as decimal(15,4))
        else 0
      end                                                                                                 as loan_amount_approval,
      cast(is_empty(loan_contract.loan_init_prin,0) as decimal(15,4))                                     as loan_amount,
      loan_apply.reqdata["scoreLevel"]                                                                    as risk_level,
      null                                                                                                as risk_score,
      loan_apply.ori_request                                                                              as ori_request,
      null                                                                                                as ori_response,
      loan_apply.create_time                                                                              as create_time,
      loan_apply.update_time                                                                              as update_time,
      loan_apply.reqdata["loanDate"]                                                                      as biz_date,
      loan_apply.reqdata["proCode"]                                                                       as product_id
    from (
      select
        reqdata["loanDate"] as create_time,
        reqdata["loanDate"] as update_time,
        reqdata,
        resdata,
        concat('{"', concat_ws('","', collect_list(concat_ws('":"', k,v) ) ), '"}') as ori_request
      from stage.kafka_credit_msg
        lateral view outer explode(reqdata) kv as k,v
      where 1 > 0
        and p_type = 'WS0012200001'
        and batch_date between date_sub('${ST9}',7) and '${ST9}'
      group by reqdata["loanDate"],reqdata,resdata
    ) as loan_apply
    left join (
      select
        apply_no,
        due_bill_no,
        product_no,
        contract_amount as loan_init_prin,
        actual_loan_date as active_date
      from (
        select
          *,
          row_number() over(partition by due_bill_no order by batch_date desc) as rn
        from stage.loan_contract
        where d_date <= '${ST9}'
          and p_type = 'WS0012200001'
        ) as tmp
      where rn = 1
    ) as loan_contract
    on  loan_apply.reqdata["proCode"] = loan_contract.product_no
    and loan_apply.reqdata["applyNo"] = loan_contract.apply_no
    left join (
      select distinct
        product_id as dim_product_id,
        channel_id
      from (
        select
          max(if(col_name = 'product_id',  col_val,null)) as product_id,
          max(if(col_name = 'channel_id',  col_val,null)) as channel_id
        from dim.data_conf
        where col_type = 'ac'
        group by col_id
      ) as tmp
    ) as biz_conf
    on loan_apply.reqdata["proCode"] = biz_conf.dim_product_id
  ) as tmp
  ;
--set hivevar:ST9=2021-06-15;
--执行用时预估101s
insert overwrite table ods.loan_apply partition(biz_date, product_id)
select
distinct *
from
ods.loan_apply
where
1 > 0 
and biz_date >= date_sub('${ST9}',30)
and product_id in ('002001', '001801', '001902', '002401', '002006', '001906', '002402', '001901', '001802', '002002')
;
  
  --------------------------------------【全量跑乐信数据】--------------------------------------
-- 乐信
--执行用时预估1304s
---set hive.execution.engine=mr;
---set mapreduce.map.memory.mb=4096;
---set mapreduce.reduce.memory.mb=4096;
---set hive.exec.parallel=true;
---set hive.exec.parallel.thread.number=20;
---set hive.exec.dynamic.partition=true;
---set hive.exec.dynamic.partition.mode=nonstrict;
---set hive.exec.max.dynamic.partitions=200000;
---set hive.exec.max.dynamic.partitions.pernode=50000;
---set hive.auto.convert.join=false;
---set hive.map.aggr=true;
---set hive.merge.mapfiles=true;
---set hive.merge.mapredfiles=true;
---set hive.merge.size.per.task=1024000000;
---set hive.merge.smallfiles.avgsize=1024000000;
---set mapred.max.split.size=256000000;
---set mapred.min.split.size.per.node=100000000;
---set mapred.min.split.size.per.rack=100000000;
-----explain
---insert overwrite table ods.loan_apply partition(biz_date,product_id)
---select
---  cust_id,
---  user_hash_no,
---  birthday,
---  age,
---  pre_apply_no,
---  apply_id,
---  due_bill_no,
---  loan_apply_time,
---  loan_amount_apply,
---  loan_terms,
---  loan_usage,
---  loan_usage_cn,
---  repay_type,
---  repay_type_cn,
---  interest_rate,
---  credit_coef,
---  penalty_rate,
---  apply_status,
---  apply_resut_msg,
---  issue_time,
---  loan_amount_approval,
---  loan_amount,
---  risk_level,
---  risk_score,
---  ori_request,
---  ori_response,
---  create_time,
---  update_time,
---  biz_date,
---  product_id
---from (
---  select
---    *,
---    row_number() over(partition by apply_id,product_id order by update_time) as rn
---  from (
---    select
---      concat_ws('_','0006',sha256(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),'idNumber',1),sha256(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.custName'),'userName',1)) as cust_id,
---      sha256(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),'idNumber',1)       as user_hash_no,
---      is_empty(
---        datefmt(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.birth'),'yyyyMMdd','yyyy-MM-dd'),
---        datefmt(substring(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd')
---      )                                                                                                               as birthday,
---      age_birth(
---        is_empty(
---          datefmt(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.birth'),'yyyyMMdd','yyyy-MM-dd'),
---          datefmt(substring(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.idNo'),7,8),'yyyyMMdd','yyyy-MM-dd')
---        ),
---        is_empty(
---          ecas_loan.active_date,
---          get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanDate')
---        )
---      )                                                                                                               as age,
---      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')                         as pre_apply_no,
---      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')                         as apply_id,
---      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo')                         as due_bill_no,
---      cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanTime') as timestamp)     as loan_apply_time,
---      cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.pactAmt') as decimal(15,4))  as loan_amount_apply,
---      cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanTerm') as decimal(3,0))  as loan_terms,
---      is_empty(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.appUse'))                as loan_usage,
---      case get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.appUse')
---        when '01' then '企业经营'
---        when '02' then '购置车辆'
---        when '03' then '装修'
---        when '04' then '子女教育(出国留学)'
---        when '05' then '购置古玩字画'
---        when '06' then '股票/期货等金融工具投资'
---        when '07' then '其他消费类'
---        else is_empty(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.appUse'),'未知')
---      end                                                                                                             as loan_usage_cn,
---      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.rpyMethod')                       as repay_type,
---      case get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.rpyMethod')
---        when '01' then '等本等息'
---        when '02' then '随借随还'
---        when '03' then '等额本息'
---        else is_empty(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.rpyMethod'))
---      end                                                                                                             as repay_type_cn,
---      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.lnRate') * 12 /100                as interest_rate,
---      cast(
---        is_empty(
---          get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.creditCoef'),
---          get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.lnRate')
---        ) as decimal(15,8)
---      ) * 12 /100                                                                                                     as credit_coef,
---      0                                                                                                               as penalty_rate,
---      case
---        when ecas_loan.loan_init_prin is not null then 1
---        when get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass') = 'Yes' then 4
---        when get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass') != 'Yes' then 5
---        else 2
---      end                                                                                                             as apply_status,
---      case
---        when ecas_loan.loan_init_prin is not null then '放款成功'
---        when get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass') = 'Yes' then '用信通过'
---        when get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass') != 'Yes' then '用信失败'
---        else '放款失败'
---      end                                                                                                             as apply_resut_msg,
---      cast(nvl(ecas_loan.active_date,get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanTime')) as timestamp) as issue_time,
---      case get_json_object(loan_apply.original_msg,'$.reqContent.jsonResp.rspData.pass')
---        when 'Yes' then cast(get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.pactAmt') as decimal(15,4))
---        else 0
---      end                                                                                                             as loan_amount_approval,
---      cast(is_empty(ecas_loan.loan_init_prin,0) as decimal(15,4))                                                     as loan_amount,
---      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.scoreLevel')                      as risk_level,
---      null                                                                                                            as risk_score,
---      loan_apply.original_msg                                                                                         as ori_request,
---      null                                                                                                            as ori_response,
---      loan_apply.create_time                                                                                          as create_time,
---      loan_apply.update_time                                                                                          as update_time,
---      get_json_object(loan_apply.original_msg,'$.reqContent.jsonReq.content.reqData.loanDate')                        as biz_date,
---      loan_apply.product_id                                                                                           as product_id
---    from (
---    select
---    t.create_time
---    ,t.update_time
---    ,t.original_msg
---    ,get_json_object(t.original_msg,'$.reqContent.jsonReq.content.reqData.applyNo') as due_bill_no
---    ,get_json_object(t.original_msg,'$.reqContent.jsonReq.content.reqData.proCode') as product_id
---    from
---    (
---      select
---        datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
---        datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
---        replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\\\"\\\{','\\\{'),'\\\}\\\\\"','\\\}'),'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\\\\\\\\\"','\\\"'),'\\\\\"','\\\"'),'\\\\\\\\','\\\\'),'\\\\\"',"") as original_msg
---      from stage.ecas_msg_log
---      where 1 > 0
---        and msg_type = 'WIND_CONTROL_CREDIT'
---        and original_msg is not null
---        --${where_date}
---        ) t
---    ) as loan_apply
---    left join (
---      select distinct
---        due_bill_no,
---        loan_init_prin,
---        active_date,
---        product_code
---      from stage.ecas_loan
---      where 1 > 0
---        and p_type in ('lx','lx2','lxzt','lx3')
---        and d_date between date_sub(current_date,2) and current_date
---    ) as ecas_loan
---    on  loan_apply.due_bill_no = ecas_loan.due_bill_no
---    and loan_apply.product_id = ecas_loan.product_code
---  ) as tmp
---) as tmp
---where rn = 1
---;




----- 新核心 乐信云信
---set hive.exec.input.listing.max.threads=50;
---set tez.grouping.min-size=50000000;
---set tez.grouping.max-size=50000000;
---set hive.exec.reducers.max=500;
---
----- 设置 Container 大小
---set hive.tez.container.size=4096;
---set tez.am.resource.memory.mb=4096;
----- 合并小文件
---set hive.merge.tezfiles=true;
---set hive.merge.size.per.task=64000000;      -- 64M
---set hive.merge.smallfiles.avgsize=64000000; -- 64M
----- 设置动态分区
---set hive.exec.dynamic.partition=true;
---set hive.exec.dynamic.partition.mode=nonstrict;
---set hive.exec.max.dynamic.partitions=200000;
---set hive.exec.max.dynamic.partitions.pernode=50000;
----- 禁用 Hive 矢量执行
---set hive.vectorized.execution.enabled=false;
---set hive.vectorized.execution.reduce.enabled=false;
---set hive.vectorized.execution.reduce.groupby.enabled=false;
---
---set hive.auto.convert.join=false;
---msck repair table stage.kafka_credit_msg;
---
---insert overwrite table ods.loan_apply partition(biz_date,product_id)
---  select
---    distinct *
---  from (
---    select
---      concat_ws('_',biz_conf.channel_id,sha256(loan_apply.reqdata["idNo"],'idNumber',1),sha256(loan_apply.reqdata["custName"],'userName',1)) as cust_id,
---      sha256(loan_apply.reqdata["idNo"],'idNumber',1)                                                     as user_hash_no,
---      is_empty(
---        datefmt(loan_apply.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
---        datefmt(substring(loan_apply.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
---      )                                                                                                   as birthday,
---      age_birth(
---        is_empty(
---          datefmt(loan_apply.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
---          datefmt(substring(loan_apply.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
---        ),
---        to_date(nvl(loan_contract.active_date,loan_apply.reqdata["loanDate"]))
---      )                                                                                                   as age,
---      loan_apply.reqdata["applyNo"]                                                                       as pre_apply_no,
---      loan_apply.reqdata["applyNo"]                                                                       as apply_id,
---      loan_contract.due_bill_no                                                                           as due_bill_no,
---      cast(loan_apply.reqdata["loanDate"] as timestamp)                                                   as loan_apply_time,
---      cast(loan_apply.reqdata["pactAmt"] as decimal(15,4))                                                as loan_amount_apply,
---      cast(loan_apply.reqdata["loanTerm"] as decimal(3,0))                                                as loan_terms,
---      is_empty(loan_apply.reqdata["appUse"])                                                              as loan_usage,
---      case loan_apply.reqdata["appUse"]
---        when '01' then '企业经营'
---        when '02' then '购置车辆'
---        when '03' then '装修'
---        when '04' then '子女教育(出国留学)'
---        when '05' then '购置古玩字画'
---        when '06' then '股票/期货等金融工具投资'
---        when '07' then '其他消费类'
---        when '08' then '医美消费'
---        else is_empty(loan_apply.reqdata["appUse"],'未知')
---      end                                                                                                 as loan_usage_cn,
---      loan_apply.reqdata["rpyMethod"]                                                                     as repay_type,
---      case loan_apply.reqdata["rpyMethod"]
---        when '01' then '等本等息'
---        when '02' then '随借随还'
---        when '03' then '等额本息'
---        when '04' then '等本等费'
---        else is_empty(loan_apply.reqdata["rpyMethod"])
---      end                                                                                                 as repay_type_cn,
---      loan_apply.reqdata["lnRate"] * 12 / 100                                                             as interest_rate,
---      cast(nvl(loan_apply.reqdata["creditCoef"],loan_apply.reqdata["lnRate"])as decimal(15,8)) * 12 / 100 as credit_coef,
---      0                                                                                                   as penalty_rate,
---      case
---        when loan_contract.loan_init_prin is not null then 1
---        when loan_apply.resdata["pass"] = 'Yes' then 4
---        when loan_apply.resdata["pass"] != 'Yes' then 5
---        else 2
---      end                                                                                                 as apply_status,
---      case
---        when loan_contract.loan_init_prin is not null then '放款成功'
---        when loan_apply.resdata["pass"] = 'Yes' then '用信通过'
---        when loan_apply.resdata["pass"] != 'Yes' then '用信失败'
---        else '放款失败'
---      end                                                                                                 as apply_resut_msg,
---      cast(nvl(loan_contract.active_date,loan_apply.reqdata["loanDate"]) as timestamp)                    as issue_time,
---      case loan_apply.resdata["pass"]
---        when 'Yes' then cast(loan_apply.reqdata["pactAmt"] as decimal(15,4))
---        else 0
---      end                                                                                                 as loan_amount_approval,
---      cast(is_empty(loan_contract.loan_init_prin,0) as decimal(15,4))                                     as loan_amount,
---      loan_apply.reqdata["scoreLevel"]                                                                    as risk_level,
---      null                                                                                                as risk_score,
---      loan_apply.ori_request                                                                              as ori_request,
---      null                                                                                                as ori_response,
---      loan_apply.create_time                                                                              as create_time,
---      loan_apply.update_time                                                                              as update_time,
---      loan_apply.reqdata["loanDate"]                                                                      as biz_date,
---      loan_apply.reqdata["proCode"]                                                                       as product_id
---    from (
---      select
---        reqdata["loanDate"] as create_time,
---        reqdata["loanDate"] as update_time,
---        reqdata,
---        resdata,
---        concat('{"', concat_ws('","', collect_list(concat_ws('":"', k,v) ) ), '"}') as ori_request
---      from stage.kafka_credit_msg
---      lateral view outer explode(reqdata) kv as k,v
---      where
---        1 > 0     
---        --batch_date between date_sub('${ST9}',7) and '${ST9}'
---        and p_type = 'WS0013200001'
---        and interfacename = 'WIND_CONTROL_CREDIT'
---      group by reqdata["loanDate"],reqdata,resdata
---    ) as loan_apply
---    left join (
---      select
---        apply_no,
---        due_bill_no,
---        product_no,
---        contract_amount as loan_init_prin,
---        actual_loan_date as active_date
---      from (
---        select
---          *,
---          row_number() over(partition by due_bill_no order by batch_date desc) as rn
---        from stage.loan_contract
---        where
---         1 > 0      
---          --and d_date <= '${ST9}'
---          and p_type = 'WS0013200001'
---      ) as tmp
---      where rn = 1
---    ) as loan_contract
---    on  loan_apply.reqdata["proCode"] = loan_contract.product_no
---    and loan_apply.reqdata["applyNo"] = loan_contract.apply_no
---    left join (
---      select distinct
---        product_id as dim_product_id,
---        channel_id
---      from (
---        select
---          max(if(col_name = 'product_id',  col_val,null)) as product_id,
---          max(if(col_name = 'channel_id',  col_val,null)) as channel_id
---        from dim.data_conf
---        where col_type = 'ac'
---        group by col_id
---      ) as tmp
---    ) as biz_conf
---    on loan_apply.reqdata["proCode"] = biz_conf.dim_product_id
---  ) as tmp
---;





-- 百度医美
---insert overwrite table ods.loan_apply partition(biz_date,product_id)
---  select
---    distinct *
---  from (
---    select
---      concat_ws('_',biz_conf.channel_id,sha256(loan_apply.reqdata["idNo"],'idNumber',1),sha256(loan_apply.reqdata["custName"],'userName',1)) as cust_id,
---      sha256(loan_apply.reqdata["idNo"],'idNumber',1)                                                     as user_hash_no,
---      is_empty(
---        datefmt(loan_apply.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
---        datefmt(substring(loan_apply.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
---      )                                                                                                   as birthday,
---      age_birth(
---        is_empty(
---          datefmt(loan_apply.reqdata["birth"],'yyyyMMdd','yyyy-MM-dd'),
---          datefmt(substring(loan_apply.reqdata["idNo"],7,8),'yyyyMMdd','yyyy-MM-dd')
---        ),
---        to_date(nvl(loan_contract.active_date,loan_apply.reqdata["loanDate"]))
---      )                                                                                                   as age,
---      loan_apply.reqdata["applyNo"]                                                                       as pre_apply_no,
---      loan_apply.reqdata["applyNo"]                                                                       as apply_id,
---      loan_contract.due_bill_no                                                                           as due_bill_no,
---      cast(loan_apply.reqdata["loanDate"] as timestamp)                                                   as loan_apply_time,
---      cast(loan_apply.reqdata["pactAmt"] as decimal(15,4))                                                as loan_amount_apply,
---      cast(loan_apply.reqdata["loanTerm"] as decimal(3,0))                                                as loan_terms,
---      is_empty(loan_apply.reqdata["appUse"])                                                              as loan_usage,
---      case loan_apply.reqdata["appUse"]
---        when '01' then '企业经营'
---        when '02' then '购置车辆'
---        when '03' then '装修'
---        when '04' then '子女教育(出国留学)'
---        when '05' then '购置古玩字画'
---        when '06' then '股票/期货等金融工具投资'
---        when '07' then '其他消费类'
---        when '08' then '医美消费'
---        else is_empty(loan_apply.reqdata["appUse"],'未知')
---      end                                                                                                 as loan_usage_cn,
---      loan_apply.reqdata["rpyMethod"]                                                                     as repay_type,
---      case loan_apply.reqdata["rpyMethod"]
---        when '01' then '等本等息'
---        when '02' then '随借随还'
---        when '03' then '等额本息'
---        when '04' then '等本等费'
---        else is_empty(loan_apply.reqdata["rpyMethod"])
---      end                                                                                                 as repay_type_cn,
---      loan_apply.reqdata["lnRate"] * 12 / 100                                                             as interest_rate,
---      cast(nvl(loan_apply.reqdata["creditCoef"],loan_apply.reqdata["lnRate"])as decimal(15,8)) * 12 / 100 as credit_coef,
---      0                                                                                                   as penalty_rate,
---      case
---        when loan_contract.loan_init_prin is not null then 1
---        when loan_apply.resdata["pass"] = 'Yes' then 4
---        when loan_apply.resdata["pass"] != 'Yes' then 5
---        else 2
---      end                                                                                                 as apply_status,
---      case
---        when loan_contract.loan_init_prin is not null then '放款成功'
---        when loan_apply.resdata["pass"] = 'Yes' then '用信通过'
---        when loan_apply.resdata["pass"] != 'Yes' then '用信失败'
---        else '放款失败'
---      end                                                                                                 as apply_resut_msg,
---      cast(nvl(loan_contract.active_date,loan_apply.reqdata["loanDate"]) as timestamp)                    as issue_time,
---      case loan_apply.resdata["pass"]
---        when 'Yes' then cast(loan_apply.reqdata["pactAmt"] as decimal(15,4))
---        else 0
---      end                                                                                                 as loan_amount_approval,
---      cast(is_empty(loan_contract.loan_init_prin,0) as decimal(15,4))                                     as loan_amount,
---      loan_apply.reqdata["scoreLevel"]                                                                    as risk_level,
---      null                                                                                                as risk_score,
---      loan_apply.ori_request                                                                              as ori_request,
---      null                                                                                                as ori_response,
---      loan_apply.create_time                                                                              as create_time,
---      loan_apply.update_time                                                                              as update_time,
---      loan_apply.reqdata["loanDate"]                                                                      as biz_date,
---      loan_apply.reqdata["proCode"]                                                                       as product_id
---    from (
---      select
---        reqdata["loanDate"] as create_time,
---        reqdata["loanDate"] as update_time,
---        reqdata,
---        resdata,
---        concat('{"', concat_ws('","', collect_list(concat_ws('":"', k,v) ) ), '"}') as ori_request
---      from stage.kafka_credit_msg
---        lateral view outer explode(reqdata) kv as k,v
---      where 1 > 0
---        and p_type = 'WS0012200001'
---        --and batch_date between date_sub('${ST9}',7) and '${ST9}'
---      group by reqdata["loanDate"],reqdata,resdata
---    ) as loan_apply
---    left join (
---      select
---        apply_no,
---        due_bill_no,
---        product_no,
---        contract_amount as loan_init_prin,
---        actual_loan_date as active_date
---      from (
---        select
---          *,
---          row_number() over(partition by due_bill_no order by batch_date desc) as rn
---        from stage.loan_contract
---        where 
---          1 >0
---          --and d_date <= '${ST9}'
---          and p_type = 'WS0012200001'
---        ) as tmp
---      where rn = 1
---    ) as loan_contract
---    on  loan_apply.reqdata["proCode"] = loan_contract.product_no
---    and loan_apply.reqdata["applyNo"] = loan_contract.apply_no
---    left join (
---      select distinct
---        product_id as dim_product_id,
---        channel_id
---      from (
---        select
---          max(if(col_name = 'product_id',  col_val,null)) as product_id,
---          max(if(col_name = 'channel_id',  col_val,null)) as channel_id
---        from dim.data_conf
---        where col_type = 'ac'
---        group by col_id
---      ) as tmp
---    ) as biz_conf
---    on loan_apply.reqdata["proCode"] = biz_conf.dim_product_id
---  ) as tmp
---  ;