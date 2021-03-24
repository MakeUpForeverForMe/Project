set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=64000000;      -- 64M
set hive.merge.smallfiles.avgsize=64000000; -- 64M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;



insert overwrite table ods.credit_apply partition(biz_date,product_id)
select
  concat('10041','_',document_num,borrower_name) as cust_id,
  document_num                                   as user_hash_no,
  credit.credit_id                               as apply_id,
  credit.assessment_date                         as credit_apply_time,
  credit.credit_amount                           as apply_amount,
  '1'                                            as resp_code,
  '授信通过'                                     as resp_msg,
  credit.assessment_date                         as credit_approval_time,
  credit.credit_validity                         as credit_expire_date,
  credit.credit_amount                           as credit_amount,
  null                                           as credit_interest_rate,
  null                                           as credit_interest_penalty_rate,
  null                                           as risk_assessment_time,
  null                                           as risk_type,
  null                                           as risk_result_validity,
  null                                           as risk_level,
  null                                           as risk_score,
  null                                           as ori_request,
  null                                           as ori_response,
  credit.create_time                             as create_time,
  credit.update_time                             as update_time,
  to_date(credit.assessment_date)                as biz_date,
  'pl00282'                                      as product_id
from (
  select * from stage.asset_t_ods_credit
  where project_id = 'pl00282'
) as credit
left join (
  select distinct
    sha256(decrypt_aes(is_empty(map_from_str(extra_info)['身份证号'],document_num), 'tencentabs123456'),'idNumber',1) as document_num,
    sha256(decrypt_aes(is_empty(map_from_str(extra_info)['客户姓名'],customer_name),'tencentabs123456'),'userName',1) as borrower_name,
    is_empty(map_from_str(extra_info)['手机号'],phone_num)                                                            as phone_no
  from stage.asset_02_t_principal_borrower_info
  where project_id = 'pl00282'
) as borrower
on credit.phone_no = borrower.phone_no
-- limit 10
;
