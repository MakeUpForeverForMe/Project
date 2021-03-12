set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=30000;

with temp_credit_apply as
(
    select
        'DIDI201908161538' as product_id,
        datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as create_time,
        datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
        original_msg
    from ods.ecas_msg_log
    where msg_type = 'CREDIT_APPLY'
      and original_msg is not null
      --and deal_date > date_sub('${ST9}',15)
      --and deal_date <= '${ST9}'
      and from_unixtime(cast(create_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') > concat(date_sub('${ST9}',15),' 07:30:00')
      and from_unixtime(cast(create_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') <= concat(date_add('${ST9}',1),' 07:30:00')
)
insert overwrite table ods_new_s.credit_apply partition(biz_date,product_id)
select
  concat_ws('_',biz_conf.channel_id,sha256(get_json_object(msg_log.original_msg,'$.userInfo.cardNo'),'idNumber',1),sha256(get_json_object(msg_log.original_msg,'$.userInfo.name'),'userName',1)) as cust_id,
  sha256(get_json_object(msg_log.original_msg,'$.userInfo.cardNo'),'idNumber',1)                                 as user_hash_no,
  get_json_object(msg_log.original_msg,'$.applicationId')                                                        as apply_id,
  cast(least(get_json_object(msg_log.original_msg,'$.creditInfo.startDate'),msg_log.create_time) as timestamp)   as credit_apply_time,
  get_json_object(msg_log.original_msg,'$.creditInfo.amount') / 100                                              as apply_amount,
  cast(case lower(get_json_object(original_msg,'$.creditResultStatus')) when 'yes' then 1 else 2 end as string)  as resp_code,
  case
  when lower(get_json_object(original_msg,'$.creditResultStatus')) = 'yes' then '授信通过'
  when lower(get_json_object(original_msg,'$.creditResultStatus')) = 'no' and get_json_object(original_msg,'$.creditResultMessage') is null then '授信失败'
  else get_json_object(original_msg,'$.creditResultMessage') end                                                 as resp_msg,
  cast(get_json_object(msg_log.original_msg,'$.creditInfo.startDate') as timestamp)                              as credit_approval_time,
  cast(get_json_object(msg_log.original_msg,'$.creditInfo.endDate') as timestamp)                                as credit_expire_date,
  get_json_object(msg_log.original_msg,'$.creditInfo.amount')  / 100                                             as credit_amount,
  cast(get_json_object(msg_log.original_msg,'$.creditInfo.interestRate') as bigint)/1000000                      as credit_interest_rate,
  cast(get_json_object(msg_log.original_msg,'$.creditInfo.interestPenaltyRate') as bigint)/1000000               as credit_interest_penalty_rate,
  null                                                                                                           as risk_assessment_time,
  null                                                                                                           as risk_type,
  null                                                                                                           as risk_result_validity,
  null                                                                                                           as risk_level,
  null                                                                                                           as risk_score,
  msg_log.original_msg                                                                                           as ori_request,
  null                                                                                                           as ori_response,
  cast(msg_log.create_time as timestamp)                                                                         as create_time,
  cast(msg_log.update_time as timestamp)                                                                         as update_time,
  from_unixtime(unix_timestamp(substring(get_json_object(original_msg,'$.applicationId'),11,8),'yyyyMMdd'),'yyyy-MM-dd')    as biz_date,
  msg_log.product_id                                                                                             as product_id
from temp_credit_apply as msg_log
left join (
  select
    product_id as dim_product_id,
    channel_id
  from dim_new.biz_conf
) as biz_conf
on msg_log.product_id = biz_conf.dim_product_id
union
select *
from ods_new_s.credit_apply
where product_id = 'DIDI201908161538'
and biz_date in
    (
        select
            from_unixtime(unix_timestamp(substring(get_json_object(original_msg,'$.applicationId'),11,8),'yyyyMMdd'),'yyyy-MM-dd')    as biz_date
        from temp_credit_apply
    )
;


--瓜子的授信
insert overwrite table ods_new_s.credit_apply partition(biz_date,product_id)
select
concat_ws('_',biz_conf.channel_id,sha256(credit_apply.idNo,'idNumber',1),sha256(credit_apply.name,'userName',1))                                              as cust_id,
sha256(credit_apply.idNo,'idNumber',1)                                                                                                                        as user_hash_no,
credit_apply.apply_id                                                                                                                                         as apply_id,
cast(credit_apply.credit_apply_time as timestamp)                                                                                                             as credit_apply_time,
credit_apply.apply_amount                                                                                                                                     as apply_amount,
if(credit_result.result_status='0','2','1')                                                                                                                   as resp_code,
if(credit_result.result_status='0','授信失败','授信通过')                                                                                                      as resp_msg,
cast(if(credit_result.result_status='0',null,credit_apply.credit_apply_time)as timestamp)                                                                     as credit_approval_time,
null                                                                                                                                                          as credit_expire_date,
cast(if(credit_result.result_status='0',0,credit_apply.apply_amount) as decimal(15,4))                                                                        as credit_amount,
cast(credit_apply.credit_interest_rate as decimal(15,8))                                                                                                      as credit_interest_rate,
cast(credit_apply.credit_interest_penalty_rate as decimal(15,8))                                                                                              as credit_interest_penalty_rate,
null                                                                                                                                                          as risk_assessment_time,
null                                                                                                                                                          as risk_type,
null                                                                                                                                                          as risk_result_validity,
null                                                                                                                                                          as risk_level,
null                                                                                                                                                          as risk_score,
credit_apply.ori_request                                                                                                                                      as ori_request,
credit_result.ori_response                                                                                                                                    as ori_response,
cast(credit_apply.create_time as timestamp)                                                                                                                   as create_time,
cast(credit_apply.update_time as timestamp)                                                                                                                   as update_time,
credit_apply.biz_date                                                                                                                                         as biz_date,
credit_apply.product_id                                                                                                                                       as product_id
from
(
select
    get_json_object(get_json_object(original_msg,'$.content'),'$.applyNo')                                                                                  as apply_id,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.rentalDate')                                                                          as credit_apply_time,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.loanAmt')                                                                             as apply_amount,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.loanRate')                                                                            as credit_interest_rate,
    null                                                                                                                                                    as risk_assessment_time,
    null                                                                                                                                                    as risk_type,
    null                                                                                                                                                    as risk_result_validity,
    null                                                                                                                                                    as risk_level,
    null                                                                                                                                                    as risk_score,
    original_msg                                                                                                                                            as ori_request,
    null                                                                                                                                                    as ori_response,
    cast(create_time as timestamp)                                                                                                                          as create_time,
    cast(update_time as timestamp)                                                                                                                          as update_time,

    get_json_object(get_json_object(original_msg,'$.data'),'$.product.loanPenaltyRate')                                                                     as credit_interest_penalty_rate,
    get_json_object(get_json_object(original_msg,'$.data'),'$.borrower.idNo')                                                                               as idNo,
    get_json_object(get_json_object(original_msg,'$.data'),'$.borrower.name')                                                                               as name,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.rentalDate')                                                                          as biz_date,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.productNo')                                                                           as product_id
from ods.ecas_msg_log
    where msg_type='GZ_CREDIT_APPLY'
    and deal_date>=date_sub('${ST9}',15)
 )credit_apply
 left join
 (
    select
    get_json_object( get_json_object(get_json_object(original_msg,'$.reqContent.jsonReq'),'$.content'),'$.applyNo')                                          as apply_id,
    cast(get_json_object(get_json_object(get_json_object(original_msg,'$.reqContent.jsonResp'),'$.rspData'),'$.status') as string)                           as result_status,
    original_msg                                                                                                                                             as ori_response
    from ods.ecas_msg_log
    where msg_type='GZ_CREDIT_RESULT'
    and deal_date>=date_sub('${ST9}',15)
)credit_result on credit_apply.apply_id=credit_result.apply_id
left join (
  select
    product_id as dim_product_id,
    channel_id
  from dim_new.biz_conf
) as biz_conf
on credit_apply.product_id = biz_conf.dim_product_id;

