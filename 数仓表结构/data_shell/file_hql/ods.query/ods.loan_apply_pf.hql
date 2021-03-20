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




insert overwrite table ods.loan_apply partition(biz_date,product_id)
select
  concat('10041','_',asset_02.document_num,asset_02.borrower_name) as cust_id,
  asset_02.document_num                                            as user_hash_no,
  asset_02.birthday                                                as birthday,
  age_birth(
    asset_02.birthday,
    asset_01.loan_begin_date
  )                                                                as age,
  null                                                             as pre_apply_no,
  asset_loan_apply.credit_id                                       as apply_id,
  asset_loan_apply.loan_id                                         as due_bill_no,
  asset_loan_apply.assessment_date                                 as loan_apply_time,
  asset_loan_apply.apply_amt                                       as loan_amount_apply,
  asset_loan_apply.product_terms                                   as loan_terms,
  '9'                                                              as loan_usage,
  asset_01.loan_use                                                as loan_usage_cn,
  '2'                                                              as repay_type,
  asset_01.repay_type                                              as repay_type_cn,
  asset_01.loan_interest_rate                                      as interest_rate,
  asset_01.loan_interest_rate                                      as credit_coef,
  asset_01.loan_penalty_rate                                       as penalty_rate,
  asset_loan_apply.risk_control_result                             as apply_status,
  null                                                             as apply_resut_msg,
  to_date(asset_01.loan_begin_date)                                as issue_time,
  asset_loan_apply.approval_amt                                    as loan_amount_approval,
  asset_01.loan_total_amount                                       as loan_amount,
  null                                                             as risk_level,
  null                                                             as risk_score,
  null                                                             as ori_request,
  null                                                             as ori_response,
  asset_loan_apply.create_time                                     as create_time,
  asset_loan_apply.update_time                                     as update_time,
  to_date(asset_loan_apply.assessment_date)                        as biz_date, -- assessment_date 评估日期
  'pl00282'                                                        as product_id
from (
  select * from stage.asset_t_credit_loan
  where project_id = 'pl00282'
) as asset_loan_apply
left join (
  select distinct
    sha256(decrypt_aes(is_empty(map_from_str(extra_info)['身份证号'],document_num), 'tencentabs123456'),'idNumber',1) as document_num,
    sha256(decrypt_aes(is_empty(map_from_str(extra_info)['客户姓名'],customer_name),'tencentabs123456'),'userName',1) as borrower_name,
    datefmt(substring(decrypt_aes(is_empty(map_from_str(extra_info)['身份证号'],document_num),'tencentabs123456'),7,8),'yyyyMMdd','yyyy-MM-dd') as birthday,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                                                             as asset_id
  from stage.asset_02_t_principal_borrower_info
  where project_id = 'pl00282'
) as asset_02
on asset_loan_apply.loan_id = asset_02.asset_id
left join (
  select
    is_empty(map_from_str(extra_info)['借据号'],asset_id)                      as asset_id,
    is_empty(map_from_str(extra_info)['贷款用途'],loan_use)                    as loan_use,
    is_empty(map_from_str(extra_info)['还款方式'],repay_type)                  as repay_type,
    is_empty(map_from_str(extra_info)['总期数'],periods)                       as periods,
    is_empty(map_from_str(extra_info)['贷款总金额(元)'],loan_total_amount,0)   as loan_total_amount,
    is_empty(loan_interest_rate,map_from_str(extra_info)['贷款年利率(%)'],0)   as loan_interest_rate,
    is_empty(map_from_str(extra_info)['贷款总利息(元)'],loan_total_interest,0) as loan_total_interest,
    is_empty(map_from_str(extra_info)['贷款罚息利率(%)'],loan_penalty_rate,0)  as loan_penalty_rate,
    is_empty(
      case when length(map_from_str(extra_info)['实际放款时间']) = 19 then to_date(map_from_str(extra_info)['实际放款时间'])
      else is_empty(datefmt(map_from_str(extra_info)['实际放款时间'],'','yyyy-MM-dd')) end,
      case when length(map_from_str(extra_info)['合同开始时间']) = 19 then to_date(map_from_str(extra_info)['合同开始时间'])
      else is_empty(datefmt(map_from_str(extra_info)['合同开始时间'],'','yyyy-MM-dd')) end
    )                                                                          as loan_begin_date
  from stage.asset_01_t_loan_contract_info
  where project_id = 'pl00282'
) as asset_01
on asset_loan_apply.loan_id = asset_01.asset_id
-- limit 10
;
