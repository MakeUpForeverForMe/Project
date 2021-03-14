-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;



with temp_credit_apply as (
  select
    'DIDI201908161538'                                                                               as product_id,
    datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')                                                  as create_time,
    datefmt(update_time,'ms','yyyy-MM-dd HH:mm:ss')                                                  as update_time,
    sha256(get_json_object(original_msg,'$.userInfo.cardNo'),'idNumber',1)                           as idNo,
    sha256(get_json_object(original_msg,'$.userInfo.name'),'userName',1)                             as name,
    get_json_object(original_msg,'$.applicationId')                                                  as apply_id,
    get_json_object(original_msg,'$.creditInfo.startDate')                                           as credit_apply_time,
    get_json_object(original_msg,'$.creditInfo.amount') / 100                                        as apply_amount,
    case lower(get_json_object(original_msg,'$.creditResultStatus')) when 'yes' then 1 else 2 end    as resp_code,
    case  when lower(get_json_object(original_msg,'$.creditResultStatus')) = 'yes' then '授信通过'
          when lower(get_json_object(original_msg,'$.creditResultStatus')) = 'no' and get_json_object(original_msg,'$.creditResultMessage') is null then '授信失败'
          else get_json_object(original_msg,'$.creditResultMessage')
    end                                                                                              as resp_msg,
    get_json_object(original_msg,'$.creditInfo.startDate')                                           as credit_approval_time,
    get_json_object(original_msg,'$.creditInfo.endDate')                                             as credit_expire_date,
    get_json_object(original_msg,'$.creditInfo.amount') / 100                                        as credit_amount,
    get_json_object(original_msg,'$.creditInfo.interestRate') / 1000000                              as credit_interest_rate,
    get_json_object(original_msg,'$.creditInfo.interestPenaltyRate') / 1000000                       as credit_interest_penalty_rate,
    datefmt(substring(get_json_object(original_msg,'$.applicationId'),11,8),'yyyyMMdd','yyyy-MM-dd') as biz_date,
    original_msg
  from stage.ecas_msg_log
  where 1 > 0
    and msg_type = 'CREDIT_APPLY'
    and original_msg is not null
    -- and deal_date > date_sub('${ST9}',15)
    -- and deal_date <= '${ST9}'
    -- and from_unixtime(cast(create_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') > concat(date_sub('${ST9}',15),' 07:30:00')
    -- and from_unixtime(cast(create_time/1000 as bigint),'yyyy-MM-dd hh:mm:ss') <= concat(date_add('${ST9}',1),' 07:30:00')
    and datefmt(create_time,'ms','yyyy-MM-dd hh:mm:ss') between concat(date_sub('${ST9}',14),' 07:30:00') and concat(date_add('${ST9}',1),' 07:30:00')
)
insert overwrite table ods.credit_apply partition(biz_date,product_id)
select
  concat_ws('_',channel_id,idNo,name)                     as cust_id,
  idNo                                                    as user_hash_no,
  apply_id                                                as apply_id,
  cast(least(credit_apply_time,create_time) as timestamp) as credit_apply_time,
  apply_amount                                            as apply_amount,
  cast(resp_code as string)                               as resp_code,
  resp_msg                                                as resp_msg,
  cast(credit_approval_time as timestamp)                 as credit_approval_time,
  cast(credit_expire_date as timestamp)                   as credit_expire_date,
  credit_amount                                           as credit_amount,
  credit_interest_rate                                    as credit_interest_rate,
  credit_interest_penalty_rate                            as credit_interest_penalty_rate,
  null                                                    as risk_assessment_time,
  null                                                    as risk_type,
  null                                                    as risk_result_validity,
  null                                                    as risk_level,
  null                                                    as risk_score,
  original_msg                                            as ori_request,
  null                                                    as ori_response,
  cast(create_time as timestamp)                          as create_time,
  cast(update_time as timestamp)                          as update_time,
  biz_date                                                as biz_date,
  product_id                                              as product_id
from temp_credit_apply as msg_log
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
on msg_log.product_id = biz_conf.dim_product_id
union
select
  *
from ods.credit_apply
where 1 > 0
  and product_id = 'DIDI201908161538'
  and biz_date in (select distinct biz_date from temp_credit_apply)
-- limit 10
;






--瓜子的授信
insert overwrite table ods.credit_apply partition(biz_date,product_id)
select
  concat_ws('_',channel_id,idNo,name)                               as cust_id,
  idNo                                                              as user_hash_no,
  apply_id                                                          as apply_id,
  cast(credit_apply_time as timestamp)                              as credit_apply_time,
  cast(apply_amount as decimal(25,5))                               as apply_amount,
  if(result_status = '0','2','1')                                   as resp_code,
  if(result_status = '0','授信失败','授信通过')                     as resp_msg,
  cast(if(result_status = '0',null,credit_apply_time) as timestamp) as credit_approval_time,
  null                                                              as credit_expire_date,
  cast(if(result_status = '0',0,apply_amount) as decimal(25,5))     as credit_amount,
  cast(credit_interest_rate as decimal(30,10))                      as credit_interest_rate,
  cast(credit_interest_penalty_rate as decimal(30,10))              as credit_interest_penalty_rate,
  null                                                              as risk_assessment_time,
  null                                                              as risk_type,
  null                                                              as risk_result_validity,
  null                                                              as risk_level,
  null                                                              as risk_score,
  ori_request                                                       as ori_request,
  ori_response                                                      as ori_response,
  cast(create_time as timestamp)                                    as create_time,
  cast(update_time as timestamp)                                    as update_time,
  biz_date                                                          as biz_date,
  product_id                                                        as product_id
from (
  select
    get_json_object(get_json_object(original_msg,'$.content'),'$.applyNo')                         as apply_id,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.rentalDate')                 as credit_apply_time,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.loanAmt')                    as apply_amount,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.loanRate')                   as credit_interest_rate,
    original_msg                                                                                   as ori_request,
    create_time                                                                                    as create_time,
    update_time                                                                                    as update_time,

    get_json_object(get_json_object(original_msg,'$.data'),'$.product.loanPenaltyRate')            as credit_interest_penalty_rate,
    sha256(get_json_object(get_json_object(original_msg,'$.data'),'$.borrower.idNo'),'idNumber',1) as idNo,
    sha256(get_json_object(get_json_object(original_msg,'$.data'),'$.borrower.name'),'userName',1) as name,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.rentalDate')                 as biz_date,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.productNo')                  as product_id
  from stage.ecas_msg_log
  where msg_type = 'GZ_CREDIT_APPLY'
    and deal_date >= date_sub('${ST9}',15)
) as credit_apply
left join (
  select
    get_json_object(get_json_object(get_json_object(original_msg,'$.reqContent.jsonReq'),'$.content'),'$.applyNo')                 as apply_id_credit_result,
    cast(get_json_object(get_json_object(get_json_object(original_msg,'$.reqContent.jsonResp'),'$.rspData'),'$.status') as string) as result_status,
    original_msg                                                                                                                   as ori_response
  from stage.ecas_msg_log
  where msg_type = 'GZ_CREDIT_RESULT'
    and deal_date >= date_sub('${ST9}',15)
) as credit_result
on credit_apply.apply_id = credit_result.apply_id_credit_result
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
on credit_apply.product_id = biz_conf.dim_product_id
union
select
  credit_apply.*
from ods.credit_apply
join (
  select distinct
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.rentalDate') as biz_date,
    get_json_object(get_json_object(original_msg,'$.data'),'$.product.productNo')  as product_id
  from stage.ecas_msg_log
  where msg_type = 'GZ_CREDIT_APPLY'
    and deal_date >= date_sub('${ST9}',15)
) as msg_log
on  credit_apply.biz_date   = msg_log.biz_date
and credit_apply.product_id = msg_log.product_id
-- limit 10