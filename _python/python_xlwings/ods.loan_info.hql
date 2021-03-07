set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set hive.auto.convert.join=false;

truncate table ods_new_s.loan_info_tmp;

insert overwrite table ods_new_s.loan_info_tmp partition(is_settled = 'no',product_id)
select distinct
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
  loan_term,
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
  dpd_days_count,
  dpd_days_max,
  collect_out_date,
  overdue_term,
  overdue_terms_count,
  overdue_terms_max,
  overdue_principal_accumulate,
  overdue_principal_max,
  mob,
  sync_date,
  s_d_date,
  e_d_date,
  effective_time,
  expire_time,
  product_id
from ods_new_s.loan_info
where is_settled = 'no'
  -- and to_date(effective_time) <= date_add('${compute_date}',1)
  and s_d_date < '${compute_date}'
;

DROP TABLE IF EXISTS ods_new_s.loan_info_intsert;
CREATE TABLE IF NOT EXISTS `ods_new_s.loan_info_intsert` like `ods_new_s.loan_info`;

with ods_new_s_loan as (
  select distinct
    ecas_loan.product_id                      as product_id,
    loan_apply.user_hash_no                   as user_hash_no,
    loan_apply.cust_id                        as cust_id,
    loan_apply.age                            as age,
    ecas_loan.loan_id                         as loan_id,
    ecas_loan.due_bill_no                     as due_bill_no,
    ecas_loan.contract_no                     as contract_no,
    ecas_loan.apply_no                        as apply_no,
    ecas_loan.loan_usage                      as loan_usage,
    ecas_loan.register_date                   as register_date,
    cast(datefmt(ecas_loan.request_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)        as request_time,
    ecas_loan.loan_active_date                as loan_active_date,
    cast(ecas_loan.cycle_day as decimal(2,0)) as cycle_day,
    ecas_loan.loan_expire_date                as loan_expire_date,
    ecas_loan.loan_type                       as loan_type,
    ecas_loan.loan_type_cn                    as loan_type_cn,
    ecas_loan.loan_init_term                  as loan_init_term,
    ecas_loan.loan_term                       as loan_term,
    ecas_loan.loan_term_repaid                as loan_term_repaid,
    ecas_loan.loan_term_remain                as loan_term_remain,
    ecas_loan.loan_status                     as loan_status,
    ecas_loan.loan_status_cn                  as loan_status_cn,
    ecas_loan.loan_out_reason                 as loan_out_reason,
    ecas_loan.paid_out_type                   as paid_out_type,
    ecas_loan.paid_out_type_cn                as paid_out_type_cn,
    ecas_loan.paid_out_date                   as paid_out_date,
    ecas_loan.terminal_date                   as terminal_date,
    ecas_loan.loan_init_principal             as loan_init_principal,
    ecas_loan.loan_init_interest_rate         as loan_init_interest_rate,
    ecas_loan.loan_init_interest              as loan_init_interest,
    ecas_loan.loan_init_term_fee_rate         as loan_init_term_fee_rate,
    ecas_loan.loan_init_term_fee              as loan_init_term_fee,
    ecas_loan.loan_init_svc_fee_rate          as loan_init_svc_fee_rate,
    ecas_loan.loan_init_svc_fee               as loan_init_svc_fee,
    ecas_loan.loan_init_penalty_rate          as loan_init_penalty_rate,
    ecas_loan.paid_amount                     as paid_amount,
    ecas_loan.paid_principal                  as paid_principal,
    ecas_loan.paid_interest                   as paid_interest,
    ecas_loan.paid_penalty                    as paid_penalty,
    ecas_loan.paid_svc_fee                    as paid_svc_fee,
    ecas_loan.paid_term_fee                   as paid_term_fee,
    ecas_loan.paid_mult                       as paid_mult,
    ecas_loan.remain_amount                   as remain_amount,
    ecas_loan.remain_principal                as remain_principal,
    ecas_loan.remain_interest                 as remain_interest,
    ecas_loan.overdue_principal               as overdue_principal,
    ecas_loan.overdue_interest                as overdue_interest,
    ecas_loan.overdue_svc_fee                 as overdue_svc_fee,
    ecas_loan.overdue_term_fee                as overdue_term_fee,
    ecas_loan.overdue_penalty                 as overdue_penalty,
    ecas_loan.overdue_mult_amt                as overdue_mult_amt,
    ecas_loan.overdue_date                    as overdue_date_start,
    ecas_loan.overdue_days                    as overdue_days,
    cast(date_add(ecas_loan.overdue_date,cast(ecas_loan.overdue_days as int) - 1) as string) as overdue_date,
    ecas_loan.dpd_begin_date                  as dpd_begin_date,
    ecas_loan.dpd_days                        as dpd_days,
    ecas_loan.dpd_days_count                  as dpd_days_count,
    ecas_loan.dpd_days_max                    as dpd_days_max,
    ecas_loan.collect_out_date                as collect_out_date,
    ecas_loan.overdue_term                    as overdue_term,
    ecas_loan.overdue_terms_count             as overdue_terms_count,
    ecas_loan.overdue_terms_max               as overdue_terms_max,
    ecas_loan.overdue_principal_accumulate    as overdue_principal_accumulate,
    ecas_loan.overdue_principal_max           as overdue_principal_max,
    ecas_loan.mob                             as mob,
    ecas_loan.sync_date                       as sync_date,
    ecas_loan.d_date                          as d_date,
    '3000-12-31'                              as e_d_date,
    ecas_loan.create_time                     as create_time,
    ecas_loan.update_time                     as update_time,
    cast('3000-12-31 00:00:00' as timestamp)  as expire_time
  from (
    select distinct
      product_code                      as product_id,
      loan_id                           as loan_id,
      due_bill_no                       as due_bill_no,
      contract_no                       as contract_no,
      apply_no                          as apply_no,
      purpose                           as loan_usage,
      register_date                     as register_date,
      request_time                      as request_time,
      active_date                       as loan_active_date,
      cycle_day                         as cycle_day,
      loan_expire_date                  as loan_expire_date,
      loan_type                         as loan_type,
      case loan_type
      when 'R'    then '消费转分期'
      when 'C'    then '现金分期'
      when 'B'    then '账单分期'
      when 'P'    then 'POS分期'
      when 'M'    then '大额分期（专项分期）'
      when 'MCAT' then '随借随还'
      when 'MCEP' then '等额本金'
      when 'MCEI' then '等额本息'
      else loan_type
      end                               as loan_type_cn,
      loan_init_term                    as loan_init_term,
      curr_term                         as loan_term,
      repay_term                        as loan_term_repaid,
      remain_term                       as loan_term_remain,
      loan_status                       as loan_status,
      case loan_status
      when 'N' then '正常'
      when 'O' then '逾期'
      when 'F' then '已还清'
      else loan_status
      end                               as loan_status_cn,
      terminal_reason_cd                as loan_out_reason,
      loan_settle_reason                as paid_out_type,
      case loan_settle_reason
      when 'NORMAL_SETTLE'  then '正常结清'
      when 'OVERDUE_SETTLE' then '逾期结清'
      when 'PRE_SETTLE'     then '提前结清'
      when 'REFUND'         then '退车'
      when 'REDEMPTION'     then '赎回'
      else loan_settle_reason
      end                               as paid_out_type_cn,
      paid_out_date                     as paid_out_date,
      terminal_date                     as terminal_date,
      loan_init_prin                    as loan_init_principal,
      interest_rate                     as loan_init_interest_rate,
      totle_int                         as loan_init_interest,
      term_fee_rate                     as loan_init_term_fee_rate,
      totle_term_fee                    as loan_init_term_fee,
      svc_fee_rate                      as loan_init_svc_fee_rate,
      totle_svc_fee                     as loan_init_svc_fee,
      penalty_rate                      as loan_init_penalty_rate,
      (paid_principal + paid_interest + paid_penalty + paid_svc_fee + paid_term_fee + paid_mult) as paid_amount,
      paid_principal                    as paid_principal,
      paid_interest                     as paid_interest,
      paid_penalty                      as paid_penalty,
      paid_svc_fee                      as paid_svc_fee,
      paid_term_fee                     as paid_term_fee,
      paid_mult                         as paid_mult,
      ((loan_init_prin + totle_int + totle_term_fee + totle_svc_fee) - (paid_principal + paid_interest + paid_svc_fee + paid_term_fee)) as remain_amount,
      (loan_init_prin - paid_principal) as remain_principal,
      (totle_int - paid_interest)       as remain_interest,
      overdue_prin                      as overdue_principal,
      overdue_interest                  as overdue_interest,
      overdue_svc_fee                   as overdue_svc_fee,
      overdue_term_fee                  as overdue_term_fee,
      overdue_penalty                   as overdue_penalty,
      overdue_mult_amt                  as overdue_mult_amt,
      overdue_date                      as overdue_date,
      overdue_days                      as overdue_days,
      null                              as dpd_begin_date,
      0                                 as dpd_days,
      0                                 as dpd_days_count,
      max_dpd                           as dpd_days_max,
      collect_out_date                  as collect_out_date,
      overdue_term                      as overdue_term,
      count_overdue_term                as overdue_terms_count,
      max_overdue_term                  as overdue_terms_max,
      0                                 as overdue_principal_accumulate,
      max_overdue_prin                  as overdue_principal_max,
      null                              as mob,
      case
      when d_date = '2020-02-21' and sync_date = 'ZhongHang' then capital_plan_no
      when d_date = '2020-02-27' and capital_type = '2020-02-28' then capital_type
      when d_date = '2020-02-28' and capital_type = '2020-02-29' then capital_type
      when d_date = '2020-02-29' and capital_type = '2020-03-01' then capital_type
      else sync_date end                as sync_date,
      cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as create_time,
      cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
      d_date                            as d_date
    from ods.ecas_loan
    where d_date != 'bak'
      and d_date = '${compute_date}'
      -- and due_bill_no = '1000000106'
  ) as ecas_loan
  left join (
    select distinct
      user_hash_no,
      cust_id,
      age_birth(birthday,to_date(issue_time)) as age,
      due_bill_no,
      product_id
    from ods_new_s.loan_apply
  ) as loan_apply
  on ecas_loan.product_id = loan_apply.product_id and ecas_loan.due_bill_no = loan_apply.due_bill_no
  left join (
    select distinct
      product_code                      as product_id,
      loan_id                           as loan_id,
      due_bill_no                       as due_bill_no,
      contract_no                       as contract_no,
      apply_no                          as apply_no,
      purpose                           as loan_usage,
      register_date                     as register_date,
      request_time                      as request_time,
      active_date                       as loan_active_date,
      cycle_day                         as cycle_day,
      loan_expire_date                  as loan_expire_date,
      loan_type                         as loan_type,
      loan_init_term                    as loan_init_term,
      curr_term                         as loan_term,
      repay_term                        as loan_term_repaid,
      remain_term                       as loan_term_remain,
      loan_status                       as loan_status,
      terminal_reason_cd                as loan_out_reason,
      loan_settle_reason                as paid_out_type,
      paid_out_date                     as paid_out_date,
      terminal_date                     as terminal_date,
      loan_init_prin                    as loan_init_principal,
      interest_rate                     as loan_init_interest_rate,
      totle_int                         as loan_init_interest,
      term_fee_rate                     as loan_init_term_fee_rate,
      totle_term_fee                    as loan_init_term_fee,
      svc_fee_rate                      as loan_init_svc_fee_rate,
      totle_svc_fee                     as loan_init_svc_fee,
      penalty_rate                      as loan_init_penalty_rate,
      paid_principal                    as paid_principal,
      paid_interest                     as paid_interest,
      paid_penalty                      as paid_penalty,
      paid_svc_fee                      as paid_svc_fee,
      paid_term_fee                     as paid_term_fee,
      paid_mult                         as paid_mult,
      overdue_prin                      as overdue_principal,
      overdue_interest                  as overdue_interest,
      overdue_svc_fee                   as overdue_svc_fee,
      overdue_term_fee                  as overdue_term_fee,
      overdue_penalty                   as overdue_penalty,
      overdue_mult_amt                  as overdue_mult_amt,
      overdue_date                      as overdue_date,
      overdue_days                      as overdue_days,
      max_dpd                           as dpd_days_max,
      collect_out_date                  as collect_out_date,
      overdue_term                      as overdue_term,
      count_overdue_term                as overdue_terms_count,
      max_overdue_term                  as overdue_terms_max,
      max_overdue_prin                  as overdue_principal_max,
      d_date                            as d_date
    from ods.ecas_loan
    where d_date != 'bak'
      and d_date = date_sub('${compute_date}',1)
      -- and due_bill_no = '1000000106'
  ) as ecas_loan_tmp
  on  is_empty(ecas_loan.product_id             ,'a') = is_empty(ecas_loan_tmp.product_id             ,'a')
  and is_empty(ecas_loan.loan_id                ,'a') = is_empty(ecas_loan_tmp.loan_id                ,'a')
  and is_empty(ecas_loan.due_bill_no            ,'a') = is_empty(ecas_loan_tmp.due_bill_no            ,'a')
  and is_empty(ecas_loan.contract_no            ,'a') = is_empty(ecas_loan_tmp.contract_no            ,'a')
  and is_empty(ecas_loan.apply_no               ,'a') = is_empty(ecas_loan_tmp.apply_no               ,'a')
  and is_empty(ecas_loan.loan_usage             ,'a') = is_empty(ecas_loan_tmp.loan_usage             ,'a')
  and is_empty(ecas_loan.register_date          ,'a') = is_empty(ecas_loan_tmp.register_date          ,'a')
  and is_empty(ecas_loan.request_time           ,'a') = is_empty(ecas_loan_tmp.request_time           ,'a')
  and is_empty(ecas_loan.loan_active_date       ,'a') = is_empty(ecas_loan_tmp.loan_active_date       ,'a')
  and is_empty(ecas_loan.cycle_day              ,'a') = is_empty(ecas_loan_tmp.cycle_day              ,'a')
  and is_empty(ecas_loan.loan_expire_date       ,'a') = is_empty(ecas_loan_tmp.loan_expire_date       ,'a')
  and is_empty(ecas_loan.loan_type              ,'a') = is_empty(ecas_loan_tmp.loan_type              ,'a')
  and is_empty(ecas_loan.loan_init_term         ,'a') = is_empty(ecas_loan_tmp.loan_init_term         ,'a')
  and is_empty(ecas_loan.loan_term              ,'a') = is_empty(ecas_loan_tmp.loan_term              ,'a')
  and is_empty(ecas_loan.loan_term_repaid       ,'a') = is_empty(ecas_loan_tmp.loan_term_repaid       ,'a')
  and is_empty(ecas_loan.loan_term_remain       ,'a') = is_empty(ecas_loan_tmp.loan_term_remain       ,'a')
  and is_empty(ecas_loan.loan_status            ,'a') = is_empty(ecas_loan_tmp.loan_status            ,'a')
  and is_empty(ecas_loan.loan_out_reason        ,'a') = is_empty(ecas_loan_tmp.loan_out_reason        ,'a')
  and is_empty(ecas_loan.paid_out_type          ,'a') = is_empty(ecas_loan_tmp.paid_out_type          ,'a')
  and is_empty(ecas_loan.paid_out_date          ,'a') = is_empty(ecas_loan_tmp.paid_out_date          ,'a')
  and is_empty(ecas_loan.terminal_date          ,'a') = is_empty(ecas_loan_tmp.terminal_date          ,'a')
  and is_empty(ecas_loan.loan_init_principal    ,'a') = is_empty(ecas_loan_tmp.loan_init_principal    ,'a')
  and is_empty(ecas_loan.loan_init_interest_rate,'a') = is_empty(ecas_loan_tmp.loan_init_interest_rate,'a')
  and is_empty(ecas_loan.loan_init_interest     ,'a') = is_empty(ecas_loan_tmp.loan_init_interest     ,'a')
  and is_empty(ecas_loan.loan_init_term_fee_rate,'a') = is_empty(ecas_loan_tmp.loan_init_term_fee_rate,'a')
  and is_empty(ecas_loan.loan_init_term_fee     ,'a') = is_empty(ecas_loan_tmp.loan_init_term_fee     ,'a')
  and is_empty(ecas_loan.loan_init_svc_fee_rate ,'a') = is_empty(ecas_loan_tmp.loan_init_svc_fee_rate ,'a')
  and is_empty(ecas_loan.loan_init_svc_fee      ,'a') = is_empty(ecas_loan_tmp.loan_init_svc_fee      ,'a')
  and is_empty(ecas_loan.loan_init_penalty_rate ,'a') = is_empty(ecas_loan_tmp.loan_init_penalty_rate ,'a')
  and is_empty(ecas_loan.paid_principal         ,'a') = is_empty(ecas_loan_tmp.paid_principal         ,'a')
  and is_empty(ecas_loan.paid_interest          ,'a') = is_empty(ecas_loan_tmp.paid_interest          ,'a')
  and is_empty(ecas_loan.paid_penalty           ,'a') = is_empty(ecas_loan_tmp.paid_penalty           ,'a')
  and is_empty(ecas_loan.paid_svc_fee           ,'a') = is_empty(ecas_loan_tmp.paid_svc_fee           ,'a')
  and is_empty(ecas_loan.paid_term_fee          ,'a') = is_empty(ecas_loan_tmp.paid_term_fee          ,'a')
  and is_empty(ecas_loan.paid_mult              ,'a') = is_empty(ecas_loan_tmp.paid_mult              ,'a')
  and is_empty(ecas_loan.overdue_principal      ,'a') = is_empty(ecas_loan_tmp.overdue_principal      ,'a')
  and is_empty(ecas_loan.overdue_interest       ,'a') = is_empty(ecas_loan_tmp.overdue_interest       ,'a')
  and is_empty(ecas_loan.overdue_svc_fee        ,'a') = is_empty(ecas_loan_tmp.overdue_svc_fee        ,'a')
  and is_empty(ecas_loan.overdue_term_fee       ,'a') = is_empty(ecas_loan_tmp.overdue_term_fee       ,'a')
  and is_empty(ecas_loan.overdue_penalty        ,'a') = is_empty(ecas_loan_tmp.overdue_penalty        ,'a')
  and is_empty(ecas_loan.overdue_mult_amt       ,'a') = is_empty(ecas_loan_tmp.overdue_mult_amt       ,'a')
  and is_empty(ecas_loan.overdue_date           ,'a') = is_empty(ecas_loan_tmp.overdue_date           ,'a')
  and is_empty(ecas_loan.overdue_days           ,'a') = is_empty(ecas_loan_tmp.overdue_days           ,'a')
  and is_empty(ecas_loan.dpd_days_max           ,'a') = is_empty(ecas_loan_tmp.dpd_days_max           ,'a')
  and is_empty(ecas_loan.collect_out_date       ,'a') = is_empty(ecas_loan_tmp.collect_out_date       ,'a')
  and is_empty(ecas_loan.overdue_term           ,'a') = is_empty(ecas_loan_tmp.overdue_term           ,'a')
  and is_empty(ecas_loan.overdue_terms_count    ,'a') = is_empty(ecas_loan_tmp.overdue_terms_count    ,'a')
  and is_empty(ecas_loan.overdue_terms_max      ,'a') = is_empty(ecas_loan_tmp.overdue_terms_max      ,'a')
  and is_empty(ecas_loan.overdue_principal_max  ,'a') = is_empty(ecas_loan_tmp.overdue_principal_max  ,'a')
  where ecas_loan_tmp.due_bill_no is null
  distribute by product_id
)
insert overwrite table ods_new_s.loan_info_intsert partition(is_settled = 'no',product_id)
select distinct
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
  loan_term,
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
  overdue_principal,
  overdue_interest,
  overdue_svc_fee,
  overdue_term_fee,
  overdue_penalty,
  overdue_mult_amt,
  min(overdue_date_start) over(partition by due_bill_no order by effective_time) as overdue_date_first,
  overdue_date_start,
  overdue_days,
  overdue_date,
  dpd_begin_date,
  dpd_days,
  dpd_days_count,
  dpd_days_max,
  collect_out_date,
  overdue_term,
  overdue_terms_count,
  overdue_terms_max,
  overdue_principal_accumulate,
  overdue_principal_max,
  mob,
  sync_date,
  s_d_date,
  e_d_date,
  effective_time,
  expire_time,
  product_id
from (
  select
    loan_info.user_hash_no,
    loan_info.cust_id,
    loan_info.age,
    loan_info.loan_id,
    loan_info.due_bill_no,
    loan_info.contract_no,
    loan_info.apply_no,
    loan_info.loan_usage,
    loan_info.register_date,
    loan_info.request_time,
    loan_info.loan_active_date,
    loan_info.cycle_day,
    loan_info.loan_expire_date,
    loan_info.loan_type,
    loan_info.loan_type_cn,
    loan_info.loan_init_term,
    loan_info.loan_term,
    loan_info.loan_term_repaid,
    loan_info.loan_term_remain,
    loan_info.loan_status,
    loan_info.loan_status_cn,
    loan_info.loan_out_reason,
    loan_info.paid_out_type,
    loan_info.paid_out_type_cn,
    loan_info.paid_out_date,
    loan_info.terminal_date,
    loan_info.loan_init_principal,
    loan_info.loan_init_interest_rate,
    loan_info.loan_init_interest,
    loan_info.loan_init_term_fee_rate,
    loan_info.loan_init_term_fee,
    loan_info.loan_init_svc_fee_rate,
    loan_info.loan_init_svc_fee,
    loan_info.loan_init_penalty_rate,
    loan_info.paid_amount,
    loan_info.paid_principal,
    loan_info.paid_interest,
    loan_info.paid_penalty,
    loan_info.paid_svc_fee,
    loan_info.paid_term_fee,
    loan_info.paid_mult,
    loan_info.remain_amount,
    loan_info.remain_principal,
    loan_info.remain_interest,
    loan_info.overdue_principal,
    loan_info.overdue_interest,
    loan_info.overdue_svc_fee,
    loan_info.overdue_term_fee,
    loan_info.overdue_penalty,
    loan_info.overdue_mult_amt,
    loan_info.overdue_date_start,
    loan_info.overdue_days,
    loan_info.overdue_date,
    loan_info.dpd_begin_date,
    loan_info.dpd_days,
    loan_info.dpd_days_count,
    loan_info.dpd_days_max,
    loan_info.collect_out_date,
    loan_info.overdue_term,
    loan_info.overdue_terms_count,
    loan_info.overdue_terms_max,
    loan_info.overdue_principal_accumulate,
    loan_info.overdue_principal_max,
    loan_info.mob,
    loan_info.sync_date,
    loan_info.s_d_date,
    if(loan_info.e_d_date > '${compute_date}' and ods_new_s_loan.due_bill_no is not null,
      ods_new_s_loan.d_date,loan_info.e_d_date) as e_d_date,
    loan_info.effective_time,
    if(to_date(loan_info.expire_time) > '${compute_date}' and ods_new_s_loan.due_bill_no is not null,
      ods_new_s_loan.update_time,loan_info.expire_time) as expire_time,
    loan_info.product_id
  from ods_new_s.loan_info_tmp as loan_info
  left join ods_new_s_loan
  on loan_info.due_bill_no = ods_new_s_loan.due_bill_no
  union all
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
    loan_term,
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
    overdue_principal,
    overdue_interest,
    overdue_svc_fee,
    overdue_term_fee,
    overdue_penalty,
    overdue_mult_amt,
    overdue_date_start,
    overdue_days,
    overdue_date,
    dpd_begin_date,
    dpd_days,
    dpd_days_count,
    dpd_days_max,
    collect_out_date,
    overdue_term,
    overdue_terms_count,
    overdue_terms_max,
    overdue_principal_accumulate,
    overdue_principal_max,
    mob,
    sync_date,
    d_date      as s_d_date,
    e_d_date,
    update_time as effective_time,
    expire_time,
    product_id
  from ods_new_s_loan
) as tmp
distribute by product_id
-- limit 1
;


DROP TABLE IF EXISTS `ods_new_s.loan_info`;
ALTER TABLE ods_new_s.loan_info_intsert RENAME TO ods_new_s.loan_info;











-- 凤金
-- select distinct
--   concat('00',loan_code)                                              as product_id,
--   loan_id                                                             as loan_id,
--   due_bill_no                                                         as due_bill_no,
--   contr_nbr                                                           as contract_no,
--   due_bill_no                                                         as apply_no,
--   purpose                                                             as loan_usage,
--   register_date                                                       as register_date,
--   request_time                                                        as request_time,
--   active_date                                                         as loan_active_date,
--   cast(cycle_day as decimal(2,0))                                     as cycle_day,
--   loan_expire_date                                                    as loan_expire_date,
--   loan_type                                                           as loan_type,
--   case loan_type
--   when 'R'    then '消费转分期'
--   when 'C'    then '现金分期'
--   when 'B'    then '账单分期'
--   when 'P'    then 'POS分期'
--   when 'M'    then '大额分期（专项分期）'
--   when 'MCAT' then '随借随还'
--   when 'MCEP' then '等额本金'
--   when 'MCEI' then '等额本息'
--   else loan_type
--   end                                                                 as loan_type_cn,
--   loan_init_term                                                      as loan_init_term,
--   curr_term                                                           as loan_term,
--   0                                                                   as loan_term_repaid,
--   remain_term                                                         as loan_term_remain,
--   loan_status                                                         as loan_status,
--   case loan_status
--   when 'N' then '正常'
--   when 'O' then '逾期'
--   when 'F' then '已还清'
--   else loan_status
--   end                                                                 as loan_status_cn,
--   terminal_reason_cd                                                  as loan_out_reason,
--   force_flag                                                          as paid_out_type,
--   paid_out_date                                                       as paid_out_date,
--   terminal_date                                                       as terminal_date,
--   loan_init_prin                                                      as loan_init_principal,
--   interest_rate                                                       as loan_init_interest_rate,
--   pay_interest                                                        as loan_init_interest,
--   fee_rate                                                            as loan_init_term_fee_rate,
--   loan_init_fee                                                       as loan_init_term_fee,
--   installment_fee_rate                                                as loan_init_svc_fee_rate,
--   tol_svc_fee                                                         as loan_init_svc_fee,
--   penalty_rate                                                        as loan_init_penalty_rate,
--   paid_fee                                                            as paid_amount,
--   paid_principal                                                      as paid_principal,
--   paid_interest                                                       as paid_interest,
--   paid_penalty                                                        as paid_penalty,
--   paid_svc_fee                                                        as paid_svc_fee,
--   0                                                                   as paid_term_fee,
--   0                                                                   as paid_mult,
--   remain_amount                                                       as remain_amount,
--   remain_principal                                                    as remain_principal,
--   remain_interest                                                     as remain_interest,
--   prin                                                                as overdue_principal,
--   interest                                                            as overdue_interest,
--   0                                                                   as overdue_svc_fee,
--   0                                                                   as overdue_term_fee,
--   penalty                                                             as overdue_penalty,
--   0                                                                   as overdue_mult_amt,
--   overdue_date                                                        as overdue_date,
--   overdue_days                                                        as overdue_days,
--   if(overdue_date is null,null,first_value(overdue_date) over(partition by due_bill_no order by cast(overdue_date as timestamp))) as first_overdue_date,
--   null                                                                as dpd_begin_date,
--   0                                                                   as dpd_days,
--   0                                                                   as dpd_days_count,
--   max_dpd                                                             as dpd_days_max,
--   collect_out_date                                                    as collect_out_date,
--   0                                                                   as overdue_term,
--   0                                                                   as overdue_terms_count,
--   0                                                                   as overdue_terms_max,
--   0                                                                   as overdue_principal_accumulate,
--   0                                                                   as overdue_principal_max,
--   null                                                                as mob,
--   null                                                                as sync_date,
--   create_time                                                         as create_time,
--   update_time                                                         as update_time
-- from (
--   select distinct
--     ref_nbr,
--     d_date,
--     loan_code,
--     loan_id,
--     contr_nbr,
--     due_bill_no,
--     purpose,
--     register_date,
--     cast(datefmt(request_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as request_time,
--     active_date,
--     loan_expire_date,
--     loan_type,
--     loan_init_term,
--     curr_term,
--     remain_term,
--     loan_status,
--     terminal_reason_cd,
--     force_flag,
--     paid_out_date,
--     terminal_date,
--     loan_init_prin,
--     interest_rate,
--     pay_interest,
--     fee_rate,
--     loan_init_fee,
--     installment_fee_rate,
--     tol_svc_fee,
--     penalty_rate,
--     paid_fee,
--     paid_principal,
--     paid_interest,
--     paid_penalty,
--     paid_svc_fee,
--     (loan_init_prin + pay_interest + loan_init_fee + tol_svc_fee - paid_fee) as remain_amount,
--     (loan_init_prin - paid_principal)                                        as remain_principal,
--     (pay_interest - paid_interest)                                           as remain_interest,
--     overdue_date,
--     if(overdue_date is null,0,datediff(current_date,overdue_date))           as overdue_days,
--     max_dpd,
--     collect_out_date,
--     cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)       as create_time,
--     cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as update_time
--   from ods.ccs_loan
--   where d_date = '${compute_date}'
-- ) as ccs_loan
-- left join (
--   select distinct
--     ref_nbr,
--     day(loan_pmt_due_date) as cycle_day
--   from ods.ccs_repay_schedule
-- ) as ccs_repay_schedule
-- on ccs_loan.ref_nbr = ccs_repay_schedule.ref_nbr
-- left join (
--   select
--     sum(past_principal + ctd_principal)            as prin,
--     sum(past_interest + ctd_interest)              as interest,
--     sum(penalty_acru + past_penalty + ctd_penalty) as penalty,
--     ref_nbr,
--     d_date as ccs_plan_d_date
--   from ods.ccs_plan
--   group by ref_nbr,d_date
-- ) as ccs_plan
-- on ccs_loan.ref_nbr = ccs_plan.ref_nbr and ccs_loan.d_date = ccs_plan.ccs_plan_d_date
