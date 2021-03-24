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


-- set hivevar:db_suffix=;set hivevar:tb_suffix=_asset;
-- set hivevar:db_suffix=_cps;set hivevar:tb_suffix=;
-- set hivevar:db_suffix=;set hivevar:tb_suffix=;

-- set hivevar:ST9=2021-03-22;
-- set hivevar:product_id=
-- '001801','001802','001803','001804',
-- '001901','001902','001903','001904','001905','001906','001907',
-- '002001','002002','002003','002004','002005','002006','002007',
-- '002401','002402',
-- ''
-- ;


insert overwrite table ods${db_suffix}.loan_lending partition(product_id)
select
  lending.apply_no                              as apply_no,
  lending.contract_no                           as contract_no,
  lending.contract_term                         as contract_term,
  lending.due_bill_no                           as due_bill_no,
  nvl(
    case log.guaranty_type
    when 'CAR' then '抵押担保'
    else log.guaranty_type end,
    lending.guarantee_type
  ) as guarantee_type,
  lending.loan_usage                            as loan_usage,
  lending.loan_issue_date                       as loan_issue_date,
  lending.loan_expiry_date                      as loan_expiry_date,
  lending.loan_active_date                      as loan_active_date,
  lending.loan_expire_date                      as loan_expire_date,
  lending.cycle_day                             as cycle_day,
  lending.loan_type                             as loan_type,
  lending.loan_type_cn                          as loan_type_cn,
  0                                             as contract_daily_interest_rate_basis,
  lending.interest_rate_type                    as interest_rate_type,
  lending.loan_init_interest_rate               as loan_init_interest_rate,
  lending.loan_init_term_fee_rate               as loan_init_term_fee_rate,
  lending.loan_init_svc_fee_rate                as loan_init_svc_fee_rate,
  lending.loan_init_penalty_rate                as loan_init_penalty_rate,
  0                                             as tail_amount,
  0                                             as tail_amount_rate,
  null                                          as bus_product_id,
  null                                          as bus_product_name,
  0                                             as mortgage_rate,
  lending.biz_date                              as biz_date,
  lending.product_id                            as product_id
from (
  select
    apply_no                        as apply_no,
    contract_no                     as contract_no,
    if(
      dayofmonth(loan_expire_date) - dayofmonth(active_date) > 27,
      (year(loan_expire_date) - year(active_date)) * 12 + month(loan_expire_date) - month(active_date) + 1,
      (year(loan_expire_date) - year(active_date)) * 12 + month(loan_expire_date) - month(active_date)
    )                               as contract_term,
    due_bill_no                     as due_bill_no,
    '信用担保'                      as guarantee_type,
    purpose                         as loan_usage,
    active_date                     as loan_issue_date,
    loan_expire_date                as loan_expiry_date,
    active_date                     as loan_active_date,
    loan_expire_date                as loan_expire_date,
    cast(cycle_day as decimal(2,0)) as cycle_day,
    loan_type                       as loan_type,
    case loan_type
      when 'R'     then '消费转分期'
      when 'C'     then '现金分期'
      when 'B'     then '账单分期'
      when 'P'     then 'POS分期'
      when 'M'     then '大额分期（专项分期）'
      when 'MCAT'  then '随借随还'
      when 'MCEP'  then '等额本金'
      when 'MCEI'  then '等额本息'
      when 'STAIR' then '阶梯还款'
      else loan_type
    end                             as loan_type_cn,
    '固定利率'                      as interest_rate_type,
    interest_rate                   as loan_init_interest_rate,
    term_fee_rate                   as loan_init_term_fee_rate,
    svc_fee_rate                    as loan_init_svc_fee_rate,
    penalty_rate                    as loan_init_penalty_rate,
    active_date                     as biz_date,
    product_code                    as product_id
  from stage.ecas_loan${tb_suffix}
  where 1 > 0
    and d_date = '${ST9}'
    and product_code in (${product_id})
) as lending
left join (
  select distinct
    get_json_object(standard_req_msg,'$.product.product_no')                              as product_id,
    map_from_str(standard_req_msg)['apply_no']                                            as due_bill_no,
    json_array_to_array(map_from_str(standard_req_msg)['guaranties'])[0]['guaranty_type'] as guaranty_type
  from stage.nms_interface_resp_log
  where 1 > 0
    and sta_service_method_name = 'setupCustCredit'
  union all
  select distinct
    get_json_object(original_msg,'$.data.product.productNo') as product_id,
    get_json_object(original_msg,'$.data.applyNo')           as due_bill_no,
    json_array_to_array(get_json_object(regexp_replace(regexp_replace(regexp_replace(original_msg,'\\\"\\\{','\\\{'),'\\\}\\\"','\\\}'),'\\\\\"','\\\"'),'$.data.guaranties'))[0]['guarantyType'] as guaranty_type
  from stage.ecas_msg_log
  where 1 > 0
    and msg_type = 'GZ_CREDIT_APPLY'
) as log
on  lending.product_id  = log.product_id
and lending.due_bill_no = log.due_bill_no
-- limit 10
;
