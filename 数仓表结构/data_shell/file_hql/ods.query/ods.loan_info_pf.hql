set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.5;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;
set hive.mapjoin.optimized.hashtable=false;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;
set hive.support.quoted.identifiers=None;



-- DROP TABLE IF EXISTS `ods_new_s.loan_info_tmp_pf`;
-- CREATE TABLE IF NOT EXISTS `ods_new_s.loan_info_tmp_pf` like `ods_new_s.loan_info`;
-- CREATE TABLE IF NOT EXISTS `ods_new_s.loan_info_tmp_pf_result` like `ods_new_s.loan_info`;
truncate table ods_new_s.loan_info_tmp_pf;
insert into table ods_new_s.loan_info_tmp_pf partition(is_settled = 'no',product_id = 'pl00282')
select
  `(is_settled|product_id)?+.+`
from ods_new_s.loan_info
where 1 > 0
  and is_settled = 'no'
  and s_d_date < '${ST9}'
  and product_id ='pl00282'
  -- and to_date(effective_time) <= date_add('2019-01-01',1)
;
-- DROP TABLE IF EXISTS ods_new_s.loan_info_intsert;
-- CREATE TABLE IF NOT EXISTS `ods_new_s.loan_info_intsert` like `ods_new_s.loan_info`;


truncate table ods_new_s.loan_info_pf_temp2;
insert into table ods_new_s.loan_info_pf_temp2
  select
   today.user_hash_no
  ,today.cust_id
  ,today.age
  ,today.loan_id
  ,today.due_bill_no
  ,today.contract_no
  ,today.apply_no
  ,today.loan_usage
  ,today.register_date
  ,today.request_time
  ,cast(today.loan_active_date as string) as loan_active_date
  ,today.cycle_day
  ,today.loan_expire_date
  ,today.loan_type
  ,today.loan_type_cn
  ,today.loan_init_term
  ,today.loan_term1
  ,today.loan_term2
  ,today.should_repay_date
  ,today.loan_term_repaid
  ,today.loan_term_remain
  ,today.loan_status
  ,today.loan_status_cn
  ,today.loan_out_reason
  ,today.paid_out_type
  ,today.paid_out_type_cn
  ,today.paid_out_date
  ,today.terminal_date
  ,today.loan_init_principal
  ,today.loan_init_interest_rate
  ,today.loan_init_interest
  ,today.loan_init_term_fee_rate
  ,today.loan_init_term_fee
  ,today.loan_init_svc_fee_rate
  ,today.loan_init_svc_fee
  ,today.loan_init_penalty_rate
  ,today.paid_amount
  ,today.paid_principal
  ,today.paid_interest
  ,today.paid_penalty
  ,today.paid_svc_fee
  ,today.paid_term_fee
  ,today.paid_mult
  ,today.remain_amount
  ,today.remain_principal
  ,today.remain_interest
  ,today.remain_svc_fee
  ,today.remain_term_fee
  ,today.overdue_principal
  ,today.overdue_interest
  ,today.overdue_svc_fee
  ,today.overdue_term_fee
  ,today.overdue_penalty
  ,today.overdue_mult_amt
  ,today.overdue_date_first
  ,cast(today.overdue_date_start as string) as overdue_date_start
  ,today.overdue_days
  ,today.overdue_date
  ,today.dpd_begin_date
  ,today.dpd_days
  ,today.dpd_days_count
  ,today.dpd_days_max
  ,today.collect_out_date
  ,today.overdue_term
  ,today.overdue_terms_count
  ,today.overdue_terms_max
  ,today.overdue_principal_accumulate
  ,today.overdue_principal_max
  ,today.sync_date
  ,today.sync_date                      as s_d_date
  ,'3000-12-31'                         as e_d_date
  from (
  select
t2.user_hash_no                                                                         as user_hash_no
,t2.cust_id                                                                             as cust_id
,t2.age                                                                                 as age
,t1.asset_id                                                                            as loan_id
,t1.asset_id                                                                            as due_bill_no
,t3.contract_code                                                                       as contract_no
,null                                                                                   as apply_no
,t3.loan_use                                                                            as loan_usage
,null                                                                                   as register_date
,null                                                                                   as request_time
,to_date(t3.loan_begin_date)                                                            as loan_active_date
,null                                                                                   as cycle_day
,t4.repay_date                                                                          as loan_expire_date
,'0'                                                                                    as loan_type
,'等额本息'                                                                             as loan_type_cn
,t3.periods                                                                             as loan_init_term
,t1.curr_period                                                                         as loan_term1
,t1.curr_period                                                                         as loan_term2
,t1.next_pay_date                                                                       as should_repay_date
,t1.repayedperiod                                                                       as loan_term_repaid
,t1.remain_period                                                                       as loan_term_remain
,case t1.assets_status
 when '正常' then 'N'
 WHEN '逾期' then 'O'
 WHEN '提前结清' then 'F'
 END                                                                                    AS loan_status
,t1.assets_status                                                                       as loan_status_cn
,null                                                                                   as loan_out_reason
,case t1.settle_reason
when '正常结清' then '0'
when '逾期结清' then '1'
when '提前结清' then '2'
end                                                                                     as paid_out_type
,t1.settle_reason                                                                       as paid_out_type_cn
,t5.rel_pay_date                                                                        as paid_out_date
,null                                                                                   as terminal_date
,t1.loan_total_amount                                                                   as loan_init_principal
,t1.loan_interest_rate                                                                  as loan_init_interest_rate
,t3.loan_total_interest                                                                 as loan_init_interest
,null                                                                                   as loan_init_term_fee_rate
,null                                                                                   as loan_init_term_fee
,null                                                                                   as loan_init_svc_fee_rate
,null                                                                                   as loan_init_svc_fee
,t3.loan_penalty_rate                                                                   as loan_init_penalty_rate
,t1.total_rel_amount                                                                    as paid_amount
,t1.total_rel_principal                                                                 as paid_principal
,t1.total_rel_interest                                                                  as paid_interest
,t1.total_rel_penalty                                                                   as paid_penalty
,null                                                                                   as paid_svc_fee
,null                                                                                   as paid_term_fee
,null                                                                                   as paid_mult
,(nvl(t1.remain_principal,0) + nvl(t1.remain_interest,0) + nvl(t1.remain_othamounts,0)) as remain_amount
,t1.remain_principal                                                                    as remain_principal
,t1.remain_interest                                                                     as remain_interest
,null                                                                                   as remain_svc_fee
,null                                                                                   as remain_term_fee
,t1.current_overdue_principal                                                           as overdue_principal
,t1.current_overdue_interest                                                            as overdue_interest
,null                                                                                   as overdue_svc_fee
,null                                                                                   as overdue_term_fee
,null                                                                                   as overdue_penalty
,null                                                                                   as overdue_mult_amt
,t6.overdue_date_first                                                                  as overdue_date_first
,if(t1.current_overdue_days > 0,date_sub(t1.account_date, t1.current_overdue_days),null) as overdue_date_start
,t1.current_overdue_days                                                                as overdue_days
,cast(date_add(t7.repay_date, t1.current_overdue_days - 1) as string)                   as overdue_date
,null                                                                                   as dpd_begin_date
,null                                                                                   as dpd_days
,null                                                                                   as dpd_days_count
,null                                                                                   as dpd_days_max
,null                                                                                   as collect_out_date
,t1.current_overdue_period                                                              as overdue_term
,t1.accum_overdue_period                                                                as overdue_terms_count
,t1.his_overdue_mperiods                                                                as overdue_terms_max
,t1.accum_overdue_principal                                                             as overdue_principal_accumulate
,t1.his_overdue_mprincipal                                                              as overdue_principal_max
,account_date                                                                           as sync_date
from
(select
*
from ods.t_asset_check
where
project_id = 'pl00282'
and account_date = '${ST9}'
) t1
left join
(select distinct
concat( '10041', '_', sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) ) AS cust_id,
sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS user_hash_no,
asset_id,
age_birth(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),datefmt ( substr( decrypt_aes ( document_num, 'tencentabs123456' ), 7, 8 ), 'yyyyMMdd', 'yyyy-MM-dd' )) as age
from ods.t_principal_borrower_info
where project_id = 'pl00282') t2
on t1.asset_id = t2.asset_id
left join
(SELECT
 asset_id
,contract_code
,loan_use
,repay_type
,periods
,loan_total_interest
,loan_penalty_rate
,loan_begin_date
 from ods.t_loan_contract_info where project_id = 'pl00282') t3
on t1.asset_id = t3.asset_id
left join
(SELECT asset_id,repay_date from ods.t_repayment_schedule
where project_id = 'pl00282'
and  remainder_periods = 0) t4
on t1.asset_id = t4.asset_id
left join
(SELECT asset_id,rel_pay_date from ods.t_repayment_info
where project_id = 'pl00282'
and plan_remainder_periods = 0) t5
on
t1.asset_id = t5.asset_id
left join
(select
asset_id
,min(repay_date) overdue_date_first
from
 ods.t_repayment_info
where project_id = 'pl00282'
and
overdue_day > 0
group by
asset_id) t6
on t1.asset_id = t6.asset_id
left join
(select
asset_id
,max(repay_date) as repay_date
,period
from
 ods.t_repayment_info
where project_id = 'pl00282'
and
overdue_day > 0
group by
asset_id
,period
) t7
on t1.asset_id = t7.asset_id
and t1.curr_period = t7.period

  ) as today
  left join (
      select
t2.user_hash_no                                                                         as user_hash_no
,t2.cust_id                                                                             as cust_id
,t2.age                                                                                 as age
,t1.asset_id                                                                            as loan_id
,t1.asset_id                                                                            as due_bill_no
,t3.contract_code                                                                       as contract_no
,null                                                                                   as apply_no
,t3.loan_use                                                                            as loan_usage
,null                                                                                   as register_date
,null                                                                                   as request_time
,to_date(t3.loan_begin_date)                                                            as loan_active_date
,null                                                                                   as cycle_day
,t4.repay_date                                                                          as loan_expire_date
,'0'                                                                                    as loan_type
,'等额本息'                                                                             as loan_type_cn
,t3.periods                                                                             as loan_init_term
,t1.curr_period                                                                         as loan_term1
,t1.curr_period                                                                         as loan_term2
,t1.next_pay_date                                                                       as should_repay_date
,t1.repayedperiod                                                                       as loan_term_repaid
,t1.remain_period                                                                       as loan_term_remain
,case t1.assets_status
 when '正常' then 'N'
 WHEN '逾期' then 'O'
 WHEN '提前结清' then 'F'
 END                                                                                    AS loan_status
,t1.assets_status                                                                       as loan_status_cn
,null                                                                                   as loan_out_reason
,case t1.settle_reason
when '正常结清' then '0'
when '逾期结清' then '1'
when '提前结清' then '2'
end                                                                                     as paid_out_type
,t1.settle_reason                                                                       as paid_out_type_cn
,t5.rel_pay_date                                                                        as paid_out_date
,null                                                                                   as terminal_date
,t1.loan_total_amount                                                                   as loan_init_principal
,t1.loan_interest_rate                                                                  as loan_init_interest_rate
,t3.loan_total_interest                                                                 as loan_init_interest
,null                                                                                   as loan_init_term_fee_rate
,null                                                                                   as loan_init_term_fee
,null                                                                                   as loan_init_svc_fee_rate
,null                                                                                   as loan_init_svc_fee
,t3.loan_penalty_rate                                                                   as loan_init_penalty_rate
,t1.total_rel_amount                                                                    as paid_amount
,t1.total_rel_principal                                                                 as paid_principal
,t1.total_rel_interest                                                                  as paid_interest
,t1.total_rel_penalty                                                                   as paid_penalty
,null                                                                                   as paid_svc_fee
,null                                                                                   as paid_term_fee
,null                                                                                   as paid_mult
,(nvl(t1.remain_principal,0) + nvl(t1.remain_interest,0) + nvl(t1.remain_othamounts,0)) as remain_amount
,t1.remain_principal                                                                    as remain_principal
,t1.remain_interest                                                                     as remain_interest
,null                                                                                   as remain_svc_fee
,null                                                                                   as remain_term_fee
,t1.current_overdue_principal                                                           as overdue_principal
,t1.current_overdue_interest                                                            as overdue_interest
,null                                                                                   as overdue_svc_fee
,null                                                                                   as overdue_term_fee
,null                                                                                   as overdue_penalty
,null                                                                                   as overdue_mult_amt
,t6.overdue_date_first                                                                  as overdue_date_first
,if(t1.current_overdue_days > 0,date_sub(t1.account_date, t1.current_overdue_days),null) as overdue_date_start
,t1.current_overdue_days                                                                as overdue_days
,cast(date_add(t7.repay_date, t1.current_overdue_days - 1) as string)                   as overdue_date
,null                                                                                   as dpd_begin_date
,null                                                                                   as dpd_days
,null                                                                                   as dpd_days_count
,null                                                                                   as dpd_days_max
,null                                                                                   as collect_out_date
,t1.current_overdue_period                                                              as overdue_term
,t1.accum_overdue_period                                                                as overdue_terms_count
,t1.his_overdue_mperiods                                                                as overdue_terms_max
,t1.accum_overdue_principal                                                             as overdue_principal_accumulate
,t1.his_overdue_mprincipal                                                              as overdue_principal_max
,account_date                                                                           as sync_date
from
(select
*
from ods.t_asset_check
where
project_id = 'pl00282'
and account_date = date_sub('${ST9}',1)
) t1
left join
(select distinct
concat( '10041', '_', sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) ) AS cust_id,
sha256 ( decrypt_aes ( document_num, 'tencentabs123456' ), 'idNumber', 1 ) AS user_hash_no,
asset_id,
age_birth(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),datefmt ( substr( decrypt_aes ( document_num, 'tencentabs123456' ), 7, 8 ), 'yyyyMMdd', 'yyyy-MM-dd' )) as age
from ods.t_principal_borrower_info
where project_id = 'pl00282') t2
on t1.asset_id = t2.asset_id
left join
(SELECT
 asset_id
,contract_code
,loan_use
,repay_type
,periods
,loan_total_interest
,loan_penalty_rate
,loan_begin_date
 from ods.t_loan_contract_info where project_id = 'pl00282') t3
on t1.asset_id = t3.asset_id
left join
(SELECT asset_id,repay_date from ods.t_repayment_schedule
where project_id = 'pl00282'
and  remainder_periods = 0) t4
on t1.asset_id = t4.asset_id
left join
(SELECT asset_id,rel_pay_date from ods.t_repayment_info
where project_id = 'pl00282'
and plan_remainder_periods = 0) t5
on
t1.asset_id = t5.asset_id
left join
(select
asset_id
,min(repay_date) overdue_date_first
from
 ods.t_repayment_info
where project_id = 'pl00282'
and
overdue_day > 0
group by
asset_id) t6
on t1.asset_id = t6.asset_id
left join
(select
asset_id
,max(repay_date) as repay_date
,period
from
 ods.t_repayment_info
where project_id = 'pl00282'
and
overdue_day > 0
group by
asset_id
,period
) t7
on t1.asset_id = t7.asset_id
and t1.curr_period = t7.period
  ) as yesterday
    on
    is_empty(today.user_hash_no                   , 'a') =  is_empty(yesterday.user_hash_no                   , 'a')
and is_empty(today.cust_id                        , 'a') =  is_empty(yesterday.cust_id                        , 'a')
and is_empty(today.age                            , 'a') =  is_empty(yesterday.age                            , 'a')
and is_empty(today.loan_id                        , 'a') =  is_empty(yesterday.loan_id                        , 'a')
and is_empty(today.due_bill_no                    , 'a') =  is_empty(yesterday.due_bill_no                    , 'a')
and is_empty(today.contract_no                    , 'a') =  is_empty(yesterday.contract_no                    , 'a')
and is_empty(today.apply_no                       , 'a') =  is_empty(yesterday.apply_no                       , 'a')
and is_empty(today.loan_usage                     , 'a') =  is_empty(yesterday.loan_usage                     , 'a')
and is_empty(today.register_date                  , 'a') =  is_empty(yesterday.register_date                  , 'a')
and is_empty(today.request_time                   , 'a') =  is_empty(yesterday.request_time                   , 'a')
and is_empty(today.loan_active_date               , 'a') =  is_empty(yesterday.loan_active_date               , 'a')
and is_empty(today.cycle_day                      , 'a') =  is_empty(yesterday.cycle_day                      , 'a')
and is_empty(today.loan_expire_date               , 'a') =  is_empty(yesterday.loan_expire_date               , 'a')
and is_empty(today.loan_type                      , 'a') =  is_empty(yesterday.loan_type                      , 'a')
and is_empty(today.loan_type_cn                   , 'a') =  is_empty(yesterday.loan_type_cn                   , 'a')
and is_empty(today.loan_init_term                 , 'a') =  is_empty(yesterday.loan_init_term                 , 'a')
and is_empty(today.loan_term1                     , 'a') =  is_empty(yesterday.loan_term1                     , 'a')
and is_empty(today.loan_term2                     , 'a') =  is_empty(yesterday.loan_term2                     , 'a')
and is_empty(today.should_repay_date              , 'a') =  is_empty(yesterday.should_repay_date              , 'a')
and is_empty(today.loan_term_repaid               , 'a') =  is_empty(yesterday.loan_term_repaid               , 'a')
and is_empty(today.loan_term_remain               , 'a') =  is_empty(yesterday.loan_term_remain               , 'a')
and is_empty(today.loan_status                    , 'a') =  is_empty(yesterday.loan_status                    , 'a')
and is_empty(today.loan_status_cn                 , 'a') =  is_empty(yesterday.loan_status_cn                 , 'a')
and is_empty(today.loan_out_reason                , 'a') =  is_empty(yesterday.loan_out_reason                , 'a')
and is_empty(today.paid_out_type                  , 'a') =  is_empty(yesterday.paid_out_type                  , 'a')
and is_empty(today.paid_out_type_cn               , 'a') =  is_empty(yesterday.paid_out_type_cn               , 'a')
and is_empty(today.paid_out_date                  , 'a') =  is_empty(yesterday.paid_out_date                  , 'a')
and is_empty(today.terminal_date                  , 'a') =  is_empty(yesterday.terminal_date                  , 'a')
and is_empty(today.loan_init_principal            , 'a') =  is_empty(yesterday.loan_init_principal            , 'a')
and is_empty(today.loan_init_interest_rate        , 'a') =  is_empty(yesterday.loan_init_interest_rate        , 'a')
and is_empty(today.loan_init_interest             , 'a') =  is_empty(yesterday.loan_init_interest             , 'a')
and is_empty(today.loan_init_term_fee_rate        , 'a') =  is_empty(yesterday.loan_init_term_fee_rate        , 'a')
and is_empty(today.loan_init_term_fee             , 'a') =  is_empty(yesterday.loan_init_term_fee             , 'a')
and is_empty(today.loan_init_svc_fee_rate         , 'a') =  is_empty(yesterday.loan_init_svc_fee_rate         , 'a')
and is_empty(today.loan_init_svc_fee              , 'a') =  is_empty(yesterday.loan_init_svc_fee              , 'a')
and is_empty(today.loan_init_penalty_rate         , 'a') =  is_empty(yesterday.loan_init_penalty_rate         , 'a')
and is_empty(today.paid_amount                    , 'a') =  is_empty(yesterday.paid_amount                    , 'a')
and is_empty(today.paid_principal                 , 'a') =  is_empty(yesterday.paid_principal                 , 'a')
and is_empty(today.paid_interest                  , 'a') =  is_empty(yesterday.paid_interest                  , 'a')
and is_empty(today.paid_penalty                   , 'a') =  is_empty(yesterday.paid_penalty                   , 'a')
and is_empty(today.paid_svc_fee                   , 'a') =  is_empty(yesterday.paid_svc_fee                   , 'a')
and is_empty(today.paid_term_fee                  , 'a') =  is_empty(yesterday.paid_term_fee                  , 'a')
and is_empty(today.paid_mult                      , 'a') =  is_empty(yesterday.paid_mult                      , 'a')
and is_empty(today.remain_amount                  , 'a') =  is_empty(yesterday.remain_amount                  , 'a')
and is_empty(today.remain_principal               , 'a') =  is_empty(yesterday.remain_principal               , 'a')
and is_empty(today.remain_interest                , 'a') =  is_empty(yesterday.remain_interest                , 'a')
and is_empty(today.remain_svc_fee                 , 'a') =  is_empty(yesterday.remain_svc_fee                 , 'a')
and is_empty(today.remain_term_fee                , 'a') =  is_empty(yesterday.remain_term_fee                , 'a')
and is_empty(today.overdue_principal              , 'a') =  is_empty(yesterday.overdue_principal              , 'a')
and is_empty(today.overdue_interest               , 'a') =  is_empty(yesterday.overdue_interest               , 'a')
and is_empty(today.overdue_svc_fee                , 'a') =  is_empty(yesterday.overdue_svc_fee                , 'a')
and is_empty(today.overdue_term_fee               , 'a') =  is_empty(yesterday.overdue_term_fee               , 'a')
and is_empty(today.overdue_penalty                , 'a') =  is_empty(yesterday.overdue_penalty                , 'a')
and is_empty(today.overdue_mult_amt               , 'a') =  is_empty(yesterday.overdue_mult_amt               , 'a')
and is_empty(today.overdue_date_first             , 'a') =  is_empty(yesterday.overdue_date_first             , 'a')
and is_empty(today.overdue_date_start             , 'a') =  is_empty(yesterday.overdue_date_start             , 'a')
and is_empty(today.overdue_days                   , 'a') =  is_empty(yesterday.overdue_days                   , 'a')
and is_empty(today.overdue_date                   , 'a') =  is_empty(yesterday.overdue_date                   , 'a')
and is_empty(today.dpd_begin_date                 , 'a') =  is_empty(yesterday.dpd_begin_date                 , 'a')
and is_empty(today.dpd_days                       , 'a') =  is_empty(yesterday.dpd_days                       , 'a')
and is_empty(today.dpd_days_count                 , 'a') =  is_empty(yesterday.dpd_days_count                 , 'a')
and is_empty(today.dpd_days_max                   , 'a') =  is_empty(yesterday.dpd_days_max                   , 'a')
and is_empty(today.collect_out_date               , 'a') =  is_empty(yesterday.collect_out_date               , 'a')
and is_empty(today.overdue_term                   , 'a') =  is_empty(yesterday.overdue_term                   , 'a')
and is_empty(today.overdue_terms_count            , 'a') =  is_empty(yesterday.overdue_terms_count            , 'a')
and is_empty(today.overdue_terms_max              , 'a') =  is_empty(yesterday.overdue_terms_max              , 'a')
and is_empty(today.overdue_principal_accumulate   , 'a') =  is_empty(yesterday.overdue_principal_accumulate   , 'a')
and is_empty(today.overdue_principal_max          , 'a') =  is_empty(yesterday.overdue_principal_max          , 'a')
  where yesterday.due_bill_no is null
;
-- insert overwrite table ods_new_s.loan_info_intsert partition(is_settled = 'no',product_id)
insert overwrite table ods_new_s.loan_info partition(is_settled = 'no',product_id = 'pl00282')
select
  user_hash_no,
  cust_id,
  age,
  loan_id,
  due_bill_no,
  contract_no,
  apply_no,
  loan_usage,
  register_date,
  request_time,
  loan_active_date,
  cycle_day,
  loan_expire_date,
  loan_type,
  loan_type_cn,
  loan_init_term,
  loan_term1,
  loan_term2,
  should_repay_date,
  loan_term_repaid,
  loan_term_remain,
  loan_status,
  loan_status_cn,
  loan_out_reason,
  paid_out_type,
  paid_out_type_cn,
  paid_out_date,
  terminal_date,
  loan_init_principal,
  loan_init_interest_rate,
  loan_init_interest,
  loan_init_term_fee_rate,
  loan_init_term_fee,
  loan_init_svc_fee_rate,
  loan_init_svc_fee,
  loan_init_penalty_rate,
  paid_amount,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_svc_fee,
  paid_term_fee,
  paid_mult,
  remain_amount,
  remain_principal,
  remain_interest,
  remain_svc_fee,
  remain_term_fee,
  overdue_principal,
  overdue_interest,
  overdue_svc_fee,
  overdue_term_fee,
  overdue_penalty,
  overdue_mult_amt,
  overdue_date_first,
  overdue_date_start,
  overdue_days,
  overdue_date,
  dpd_begin_date,
  dpd_days,
  0 as dpd_days_count,
  max(overdue_days) over(partition by due_bill_no order by s_d_date) as dpd_days_max,
  collect_out_date,
  overdue_term,
  overdue_terms_count,
  overdue_terms_max,
  overdue_principal_accumulate,
  overdue_principal_max,
  sync_date,
  s_d_date,
  e_d_date,
  null as effective_time,
  null as expire_time
  --'pl00282' as product_id
from (
  select
    loan_info.user_hash_no
    ,loan_info.cust_id
    ,loan_info.age
    ,loan_info.loan_id
    ,loan_info.due_bill_no
    ,loan_info.contract_no
    ,loan_info.apply_no
    ,loan_info.loan_usage
    ,loan_info.register_date
    ,loan_info.request_time
    ,loan_info.loan_active_date
    ,loan_info.cycle_day
    ,loan_info.loan_expire_date
    ,loan_info.loan_type
    ,loan_info.loan_type_cn
    ,loan_info.loan_init_term
    ,loan_info.loan_term1
    ,loan_info.loan_term2
    ,loan_info.should_repay_date
    ,loan_info.loan_term_repaid
    ,loan_info.loan_term_remain
    ,loan_info.loan_status
    ,loan_info.loan_status_cn
    ,loan_info.loan_out_reason
    ,loan_info.paid_out_type
    ,loan_info.paid_out_type_cn
    ,loan_info.paid_out_date
    ,loan_info.terminal_date
    ,loan_info.loan_init_principal
    ,loan_info.loan_init_interest_rate
    ,loan_info.loan_init_interest
    ,loan_info.loan_init_term_fee_rate
    ,loan_info.loan_init_term_fee
    ,loan_info.loan_init_svc_fee_rate
    ,loan_info.loan_init_svc_fee
    ,loan_info.loan_init_penalty_rate
    ,loan_info.paid_amount
    ,loan_info.paid_principal
    ,loan_info.paid_interest
    ,loan_info.paid_penalty
    ,loan_info.paid_svc_fee
    ,loan_info.paid_term_fee
    ,loan_info.paid_mult
    ,loan_info.remain_amount
    ,loan_info.remain_principal
    ,loan_info.remain_interest
    ,loan_info.remain_svc_fee
    ,loan_info.remain_term_fee
    ,loan_info.overdue_principal
    ,loan_info.overdue_interest
    ,loan_info.overdue_svc_fee
    ,loan_info.overdue_term_fee
    ,loan_info.overdue_penalty
    ,loan_info.overdue_mult_amt
    ,loan_info.overdue_date_first
    ,loan_info.overdue_date_start
    ,loan_info.overdue_days
    ,loan_info.overdue_date
    ,loan_info.dpd_begin_date
    ,loan_info.dpd_days
    ,loan_info.dpd_days_count
    ,loan_info.dpd_days_max
    ,loan_info.collect_out_date
    ,loan_info.overdue_term
    ,loan_info.overdue_terms_count
    ,loan_info.overdue_terms_max
    ,loan_info.overdue_principal_accumulate
    ,loan_info.overdue_principal_max
    ,loan_info.sync_date
    ,loan_info.s_d_date
    ,if(loan_info.e_d_date > '${ST9}' and ods_new_s_loan.due_bill_no is not null,ods_new_s_loan.sync_date,loan_info.e_d_date) as e_d_date
  from (select * from ods_new_s.loan_info_tmp_pf where 1 > 0 and product_id = 'pl00282') as loan_info
  left join ods_new_s.loan_info_pf_temp2 ods_new_s_loan
  on loan_info.due_bill_no = ods_new_s_loan.due_bill_no
  union all
  select *
  from ods_new_s.loan_info_pf_temp2
) as tmp
-- where 1 > 0
--   and due_bill_no = '1120061421344483293354'
-- limit 1
;

-- DROP TABLE IF EXISTS `ods_new_s.loan_info`;
-- ALTER TABLE ods_new_s.loan_info_intsert RENAME TO ods_new_s.loan_info;
