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
set hivevar:p_types =and p_type in ('lx','lx2','lx3','lxzt');
--set hivevar:db_suffix=;
--set hivevar:tb_suffix=_asset;
--set hivevar:product_id='001801';

insert overwrite table ods${db_suffix}.loan_info_inter partition(biz_date,product_id)
select
  today.due_bill_no                                   as due_bill_no,
  today.apply_no                                      as apply_no,
  today.loan_active_date                              as loan_active_date,
  today.loan_init_term                                as loan_init_term,
  nvl(today.loan_init_principal,0)                    as loan_init_principal,
  nvl(today.loan_init_interest,0)                     as loan_init_interest,
  nvl(today.loan_init_term_fee,0)                     as loan_init_term_fee,
  nvl(today.loan_init_svc_fee,0)                      as loan_init_svc_fee,
  today.loan_term                                     as loan_term,
  today.loan_term                                     as account_age,
  today.should_repay_date                             as should_repay_date,
  today.loan_term_repaid                              as loan_term_repaid,
  today.loan_term_remain                              as loan_term_remain,

  today.loan_status                                   as loan_status,
  today.loan_status_cn                                as loan_status_cn,

  today.loan_out_reason                               as loan_out_reason,
  today.paid_out_type                                 as paid_out_type,
  today.paid_out_type_cn                              as paid_out_type_cn,
  today.paid_out_date                                 as paid_out_date,
  today.terminal_date                                 as terminal_date,
  nvl(today.paid_amount,0)                            as paid_amount,
  nvl(today.paid_principal,0)                         as paid_principal,
  nvl(today.paid_interest,0)                          as paid_interest,
  nvl(today.paid_penalty,0)                           as paid_penalty,
  nvl(today.paid_svc_fee,0)                           as paid_svc_fee,
  nvl(today.paid_term_fee,0)                          as paid_term_fee,
  nvl(today.paid_mult,0)                              as paid_mult,
  nvl(today.remain_amount,0)                          as remain_amount,
  nvl(today.remain_principal,0)                       as remain_principal,
  nvl(today.remain_interest,0)                        as remain_interest,
  nvl(today.remain_svc_fee,0)                         as remain_svc_fee,
  nvl(today.remain_term_fee,0)                        as remain_term_fee,
  0                                                   as remain_othAmounts,
  nvl(today.overdue_principal,0)                      as overdue_principal,
  nvl(today.overdue_interest,0)                       as overdue_interest,
  nvl(today.overdue_svc_fee,0)                        as overdue_svc_fee,
  nvl(today.overdue_term_fee,0)                       as overdue_term_fee,
  nvl(today.overdue_penalty,0)                        as overdue_penalty,
  nvl(today.overdue_mult_amt,0)                       as overdue_mult_amt,
  today.overdue_date                                  as overdue_date_start,
  today.overdue_days                                  as overdue_days,
  date_add(today.overdue_date,today.overdue_days - 1) as overdue_date,
  today.collect_out_date                              as collect_out_date,
  0                                                   as dpd_days_max,
  today.overdue_term                                  as overdue_term,
  0                                                   as overdue_terms_count,
  today.overdue_terms_max                             as overdue_terms_max,
  0                                                   as overdue_principal_accumulate,
  0                                                   as overdue_principal_max,
  today.create_time                                   as create_time,
  today.update_time                                   as update_time,
  today.d_date                                        as biz_date,
  today.product_id                                    as product_id
from (
  select
    ecas_loan.product_id,
    ecas_loan.due_bill_no,
    ecas_loan.apply_no,
    ecas_loan.loan_active_date,
    ecas_loan.loan_init_term,
    --2021.05.09修改  
    schedule.loan_term as loan_term,
    schedule.should_repay_date as should_repay_date ,
    schedule.repaid_terms as loan_term_repaid,
    (ecas_loan.loan_init_term - schedule.repaid_terms) as  loan_term_remain,
    if(coalesce(ecas_loan.paid_out_date,schedule.loan_paid_out_date) is not null, 'F',        ecas_loan.loan_status)     AS loan_status,
    if(coalesce(ecas_loan.paid_out_date,schedule.loan_paid_out_date) is not null, '已还清',   ecas_loan.loan_status_cn)  as loan_status_cn,

    ecas_loan.loan_out_reason,
    ecas_loan.paid_out_type,
    ecas_loan.paid_out_type_cn,
    --2021.05.09修改 max_paid_out_date
    coalesce(ecas_loan.paid_out_date,schedule.loan_paid_out_date) as paid_out_date,
    
    ecas_loan.terminal_date,
    ecas_loan.loan_init_principal,
    ecas_loan.loan_init_interest_rate,
    ecas_loan.loan_init_interest,
    ecas_loan.loan_init_term_fee_rate,
    ecas_loan.loan_init_term_fee,
    ecas_loan.loan_init_svc_fee_rate,
    ecas_loan.loan_init_svc_fee,
    ecas_loan.loan_init_penalty_rate,
    coalesce(repay_detail.paid_principal,ecas_loan.paid_principal,0) +
    coalesce(repay_detail.paid_interest, ecas_loan.paid_interest, 0) +
    coalesce(repay_detail.paid_penalty,  ecas_loan.paid_penalty,  0) +
    coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) +
    coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0) +
    coalesce(repay_detail.paid_mult,     ecas_loan.paid_mult,     0) as paid_amount,
    coalesce(repay_detail.paid_principal,ecas_loan.paid_principal,0) as paid_principal,
    coalesce(repay_detail.paid_interest, ecas_loan.paid_interest, 0) as paid_interest,
    coalesce(repay_detail.paid_penalty,  ecas_loan.paid_penalty,  0) as paid_penalty,
    coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) as paid_svc_fee,
    coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0) as paid_term_fee,
    coalesce(repay_detail.paid_mult,     ecas_loan.paid_mult,     0) as paid_mult,
    (
      nvl(ecas_loan.loan_init_principal,0) +
      nvl(ecas_loan.loan_init_interest, 0) +
      nvl(ecas_loan.loan_init_term_fee, 0) +
      nvl(ecas_loan.loan_init_svc_fee,  0)
    ) - (
      coalesce(repay_detail.paid_principal,ecas_loan.paid_principal,0) +
      coalesce(repay_detail.paid_interest, ecas_loan.paid_interest, 0) +
      coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) +
      coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0)
    ) as remain_amount,
    ecas_loan.loan_init_principal - coalesce(repay_detail.paid_principal,ecas_loan.paid_principal,0) as remain_principal,
    ecas_loan.loan_init_interest  - coalesce(repay_detail.paid_interest, ecas_loan.paid_interest, 0) as remain_interest,
    ecas_loan.loan_init_svc_fee   - coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) as remain_svc_fee,
    ecas_loan.loan_init_term_fee  - coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0) as remain_term_fee,
    ecas_loan.overdue_principal,
    ecas_loan.overdue_interest,
    ecas_loan.overdue_svc_fee,
    ecas_loan.overdue_term_fee,
    ecas_loan.overdue_penalty,
    ecas_loan.overdue_mult_amt,
    ecas_loan.overdue_date,
    ecas_loan.overdue_days,
    ecas_loan.collect_out_date,
    overdue_term.overdue_term,
    nvl(overdue_term.overdue_terms_max,0) as overdue_terms_max,
    ecas_loan.create_time,
    ecas_loan.update_time,
    ecas_loan.d_date
  from (
    select
      product_code                      as product_id,
      due_bill_no                       as due_bill_no,
      apply_no                          as apply_no,
      active_date                       as loan_active_date,
      loan_init_term                    as loan_init_term,
      curr_term                         as loan_term,
      case
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 6
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      else repay_term end               as loan_term_repaid,
      case
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      else remain_term end              as loan_term_remain,
      case
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      else loan_status end              as loan_status,
      case
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        else loan_status end
      when 'N' then '正常'
      when 'O' then '逾期'
      when 'F' then '已还清'
      end                               as loan_status_cn,
      terminal_reason_cd                as loan_out_reason,
      case
      --2021-05-07修改
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      --2021-05-07修改
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      else loan_settle_reason  end      as paid_out_type,
      case
      --2021-05-07修改
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '提前结清'
      --2021-05-07修改
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then '提前结清'
      when loan_settle_reason = 'NORMAL_SETTLE'  then '正常结清'
      when loan_settle_reason = 'OVERDUE_SETTLE' then '逾期结清'
      when loan_settle_reason = 'PRE_SETTLE'     then '提前结清'
      when loan_settle_reason = 'REFUND'         then '退车'
      when loan_settle_reason = 'REDEMPTION'     then '赎回'
      when loan_settle_reason = 'BANK_REF'       then '退票结清'
      when loan_settle_reason = 'BUY_BACK'       then '资产回购'
      when loan_settle_reason = 'CAPITAL_VERI'   then '资产核销'
      when loan_settle_reason = 'DISPOSAL'       then '处置结束'
      when loan_settle_reason = 'OVER_COMP'      then '逾期代偿'
      else loan_settle_reason end as paid_out_type_cn,
      case
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-03'
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-03'
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-04'
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      else paid_out_date end            as paid_out_date,
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
      paid_term_fee                     as paid_term_fee,
      paid_svc_fee                      as paid_svc_fee,
      paid_penalty                      as paid_penalty,
      paid_mult                         as paid_mult,
      case
      when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
      when product_code = '001802' and due_bill_no = '1120061211013462742786' and d_date between '2020-07-08' and '2020-07-09' then 0
      else overdue_prin end             as overdue_principal,
      case
      when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
      when product_code = '001802' and due_bill_no = '1120061211013462742786' and d_date between '2020-07-08' and '2020-07-09' then 0
      else overdue_interest end         as overdue_interest,
      overdue_svc_fee                   as overdue_svc_fee,
      overdue_term_fee                  as overdue_term_fee,
      overdue_penalty                   as overdue_penalty,
      overdue_mult_amt                  as overdue_mult_amt,
      case
      when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
      when product_code = '001802' and due_bill_no = '1120061211013462742786' and d_date between '2020-07-08' and '2020-07-09' then null
      else overdue_date end             as overdue_date,
      case
      when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
      when product_code = '001802' and due_bill_no = '1120061211013462742786' and d_date between '2020-07-08' and '2020-07-09' then 0
      else overdue_days end             as overdue_days,
      collect_out_date                  as collect_out_date,
      cast(datefmt(create_time, 'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as create_time,
      cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
      d_date                            as d_date
    from stage.ecas_loan${tb_suffix}
    where 1 > 0
      and d_date = '${ST9}'
      and product_code in (${product_id})
      ${p_types}
  ) as ecas_loan
  left join (
    select
      due_bill_no,
      sum(if(bnp_type = 'Pricinpal',        repay_amt,0))
      - case when due_bill_no = '1120070912093993172613' and d_date between '2020-07-10' and '2020-07-13' then if('${tb_suffix}' = '_asset',2500,0) else 0 end
                                                          as paid_principal,
      sum(if(bnp_type = 'Interest',         repay_amt,0)) as paid_interest,
      sum(if(bnp_type = 'TERMFee',          repay_amt,0)) as paid_term_fee,
      sum(if(bnp_type = 'SVCFee',           repay_amt,0)) as paid_svc_fee,
      sum(if(bnp_type = 'Penalty',          repay_amt,0)) as paid_penalty,
      sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0)) as paid_mult
      
      --sum(if(bnp_type = 'Pricinpal',1, 0))                as repaid_terms    --已还期数
    from stage.ecas_repay_hst${tb_suffix}
    where 1 > 0
      and d_date = '${ST9}'
      and if('${tb_suffix}' = '_asset',true,
        payment_id not in ( -- 代偿后  在 2020-07-10 到 2020-07-13 这四天有六笔，最新分区只有一笔。
          '000015945007161admin000083000002',
          '000015945007161admin000083000003',
          '000015945007161admin000083000004',
          '000015945007161admin000083000005',
          '000015945007161admin000083000006'
        )
      )
      ${p_types}
      -- and d_date = date_sub(current_date(),2)
      -- and txn_date <= '${ST9}'
    group by due_bill_no,d_date
  ) as repay_detail
  on ecas_loan.due_bill_no = repay_detail.due_bill_no
  left join
  --2021-05-09修改，由还款计划出已还期数，结清日期，当前期数，应还日 四个字段
  (
  select
     due_bill_no
     ,product_code
     ,max(repaid_terms) as repaid_terms
     ,max(loan_paid_out_date) as loan_paid_out_date
     ,if(max(loan_paid_out_date) is null
         ,max(no_settle_loan_term)   --如果没有结清
         ,if(max(loan_paid_out_date) >= max(split(term_pmt_due_date, '~~')[1]), max(loan_init_term), min(if(split(term_pmt_due_date,'~~')[1] >= loan_paid_out_date, split(term_pmt_due_date,'~~')[0], null )))) 
     as loan_term
     ,if(max(loan_paid_out_date) is null
         ,max(no_settle_should_repay_date)  --如果没有结清
         ,if(max(loan_paid_out_date) >= max(split(term_pmt_due_date, '~~')[1]), max(split(term_pmt_due_date, '~~')[1]), min(if(split(term_pmt_due_date,'~~')[1] >= loan_paid_out_date, split(term_pmt_due_date,'~~')[1],null))))
     as should_repay_date
from
(
    select
    due_bill_no,
    max(loan_init_term) as loan_init_term,    
    sum(if(schedule_status = 'F', 1, 0)) as repaid_terms,    --已还期数  
    max(if(loan_init_term=curr_term and paid_out_date is not null, paid_out_date, null)) as loan_paid_out_date, --结清日期    
    if(max(if(loan_init_term=curr_term and paid_out_date is not null, paid_out_date,null)) is null
        ,if('${ST9}' >= max(pmt_due_date), max(loan_init_term), min(case when '${ST9}' < pmt_due_date then curr_term end))  --如果未结清
        ,null
        )    --如果已结清   
    as no_settle_loan_term                        --未结清当前期数        
    ,if(max(if(loan_init_term=curr_term and paid_out_date is not null, paid_out_date,null)) is null
        ,if('${ST9}' >= max(pmt_due_date), max(pmt_due_date),min(case when '${ST9}' < pmt_due_date then pmt_due_date end)) --如果未结清
        ,null)
    as no_settle_should_repay_date    --未结清应还日    
    ,collect_list(concat(curr_term, '~~', pmt_due_date))  as term_pmt_due_date_list    
    ,product_code   
    from
    stage.ecas_repay_schedule${tb_suffix}
      where 1 > 0
      and d_date = '${ST9}'
      and curr_term > 0
      and product_code in (${product_id})
      ${p_types}
      group by due_bill_no,product_code
    )  t
    LATERAL VIEW explode(term_pmt_due_date_list) pmt as term_pmt_due_date
    group by t.due_bill_no,product_code
    )  schedule 
    on ecas_loan.due_bill_no = schedule.due_bill_no
    and ecas_loan.product_id = schedule.product_code
  left join (
    select
      due_bill_no,
      max(curr_term)   as overdue_term,
      count(curr_term) as overdue_terms_max,
      d_date
    from stage.ecas_repay_schedule${tb_suffix}
    where 1 > 0
      and d_date = '${ST9}'
      and pmt_due_date <= d_date
      and product_code in (${product_id})
      and case
      --2021-05-07修改
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        --2021-05-07修改
        when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and to_date(current_timestamp()) then 'F'
        else schedule_status end = 'O'
        ${p_types}
    group by due_bill_no,d_date
  ) as overdue_term
  on  ecas_loan.d_date      = overdue_term.d_date
  and ecas_loan.due_bill_no = overdue_term.due_bill_no
) as today
left join (
  select
    ecas_loan.product_id,
    ecas_loan.due_bill_no,
    ecas_loan.apply_no,
    ecas_loan.loan_active_date,
    ecas_loan.loan_init_term,
    --2021-05-09 修改---
    schedule.loan_term                                              as  loan_term,
    schedule.should_repay_date                                      as  should_repay_date ,                                            
    schedule.repaid_terms                                           as loan_term_repaid,
    (ecas_loan.loan_init_term - schedule.repaid_terms)              as  loan_term_remain,
    if(coalesce(ecas_loan.paid_out_date,schedule.loan_paid_out_date) is not null,'F',ecas_loan.loan_status) AS loan_status,
    
    ecas_loan.loan_out_reason,
    ecas_loan.paid_out_type,
    
    --2021.05.09修改 paid_out_date
    coalesce(ecas_loan.paid_out_date,schedule.loan_paid_out_date) as paid_out_date,
    
    ecas_loan.terminal_date,
    ecas_loan.loan_init_principal,
    ecas_loan.loan_init_interest_rate,
    ecas_loan.loan_init_interest,
    ecas_loan.loan_init_term_fee_rate,
    ecas_loan.loan_init_term_fee,
    ecas_loan.loan_init_svc_fee_rate,
    ecas_loan.loan_init_svc_fee,
    ecas_loan.loan_init_penalty_rate,
    coalesce(repay_detail.paid_principal,ecas_loan.paid_principal,0) as paid_principal,
    ecas_loan.overdue_principal,
    ecas_loan.overdue_interest,
    ecas_loan.overdue_svc_fee,
    ecas_loan.overdue_term_fee,
    ecas_loan.overdue_penalty,
    ecas_loan.overdue_mult_amt,
    ecas_loan.overdue_date,
    ecas_loan.overdue_days
  from (
    select
      product_code                      as product_id,
      due_bill_no                       as due_bill_no,
      contract_no                       as contract_no,
      apply_no                          as apply_no,
      active_date                       as loan_active_date,
      loan_init_term                    as loan_init_term,
      curr_term                         as loan_term,
      case
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 6
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 1
      
      else repay_term end               as loan_term_repaid,
      case
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 0
      else remain_term end              as loan_term_remain,
      case
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'F'
      else loan_status end              as loan_status,
      terminal_reason_cd                as loan_out_reason,
      case
     --2021-05-07修改
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'PRE_SETTLE'
      --2021-05-07修改
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then 'PRE_SETTLE'
      else loan_settle_reason end       as paid_out_type,
      case
      when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-03'
      when product_code = '001801' and due_bill_no = '1120060216004289090275' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-03'
      when product_code = '001801' and due_bill_no = '1120060420501158464265' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-04'
      when product_code = '001801' and due_bill_no = '1120060510291219831303' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510292928006247' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510293236264396' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510300702357685' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510303029749221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510313484801510' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510314443303533' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510314637234204' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510324386931800' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510455040992322' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510455756944221' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510462216533625' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510470989937022' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-05'
      when product_code = '001801' and due_bill_no = '1120060602322140630323' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060602332594205233' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060602323631368511' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060602324557511307' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060511173419906330' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060511325696044931' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060510300944421982' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060511131974539713' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      when product_code = '001801' and due_bill_no = '1120060511184385512833' and d_date between '2020-06-06' and to_date(current_timestamp()) then '2020-06-06'
      else paid_out_date end            as paid_out_date,
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
      case
      when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
      when product_code = '001802' and due_bill_no = '1120061211013462742786' and d_date between '2020-07-08' and '2020-07-09' then 0
      else overdue_prin end             as overdue_principal,
      case
      when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
      when product_code = '001802' and due_bill_no = '1120061211013462742786' and d_date between '2020-07-08' and '2020-07-09' then 0
      else overdue_interest end         as overdue_interest,
      overdue_svc_fee                   as overdue_svc_fee,
      overdue_term_fee                  as overdue_term_fee,
      overdue_penalty                   as overdue_penalty,
      overdue_mult_amt                  as overdue_mult_amt,
      case
      when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
      when product_code = '001802' and due_bill_no = '1120061211013462742786' and d_date between '2020-07-08' and '2020-07-09' then null
      else overdue_date end             as overdue_date,
      case
      when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
      when product_code = '001802' and due_bill_no = '1120061211013462742786' and d_date between '2020-07-08' and '2020-07-09' then 0
      else overdue_days end             as overdue_days,
      d_date as d_date
    from stage.ecas_loan${tb_suffix}
    where 1 > 0
      and d_date = date_sub('${ST9}',1)
      and product_code in (${product_id})
      ${p_types}
  ) as ecas_loan
  left join (
    select
      due_bill_no,
      sum(if(bnp_type = 'Pricinpal',        repay_amt,0))
      - case when due_bill_no = '1120070912093993172613' and d_date between '2020-07-10' and '2020-07-13' then if('${tb_suffix}' = '_asset',2500,0) else 0 end
                                                          as paid_principal,
      sum(if(bnp_type = 'Interest',         repay_amt,0)) as paid_interest,
      sum(if(bnp_type = 'TERMFee',          repay_amt,0)) as paid_term_fee,
      sum(if(bnp_type = 'SVCFee',           repay_amt,0)) as paid_svc_fee,
      sum(if(bnp_type = 'Penalty',          repay_amt,0)) as paid_penalty,
      sum(if(bnp_type = 'LatePaymentCharge',repay_amt,0)) as paid_mult
    from stage.ecas_repay_hst${tb_suffix}
    where 1 > 0
      and d_date = date_sub('${ST9}',1)
      and if('${tb_suffix}' = '_asset',true,
        payment_id not in ( -- 代偿后  在 2020-07-10 到 2020-07-13 这四天有六笔，最新分区只有一笔。
          '000015945007161admin000083000002',
          '000015945007161admin000083000003',
          '000015945007161admin000083000004',
          '000015945007161admin000083000005',
          '000015945007161admin000083000006'
        )
      )
      ${p_types}
      -- and d_date = date_sub(date_sub(current_date(),2),1)
      -- and txn_date <= date_sub('${ST9}',1)
    group by due_bill_no,d_date
  ) as repay_detail
  on ecas_loan.due_bill_no = repay_detail.due_bill_no
  left join 
  --2021-05-09修改，由还款计划出已还期数，结清日期，当前期数，应还日 四个字段
  (
  select
     due_bill_no
     ,product_code
     ,max(repaid_terms) as repaid_terms
     ,max(loan_paid_out_date) as loan_paid_out_date
     ,if(max(loan_paid_out_date) is null
         ,max(no_settle_loan_term)   --如果没有结清
         ,if(max(loan_paid_out_date) >= max(split(term_pmt_due_date, '~~')[1]), max(loan_init_term), min(if(split(term_pmt_due_date,'~~')[1] >= loan_paid_out_date, split(term_pmt_due_date,'~~')[0], null )))) 
     as loan_term
     ,if(max(loan_paid_out_date) is null
         ,max(no_settle_should_repay_date)  --如果没有结清
         ,if(max(loan_paid_out_date) >= max(split(term_pmt_due_date, '~~')[1]), max(split(term_pmt_due_date, '~~')[1]), min(if(split(term_pmt_due_date,'~~')[1] >= loan_paid_out_date, split(term_pmt_due_date,'~~')[1],null))))
     as should_repay_date
from
(
    select
    due_bill_no,
    max(loan_init_term) as loan_init_term,    
    sum(if(schedule_status = 'F', 1, 0)) as repaid_terms,    --已还期数  
    max(if(loan_init_term=curr_term and paid_out_date is not null, paid_out_date, null)) as loan_paid_out_date, --结清日期    
    if(max(if(loan_init_term=curr_term and paid_out_date is not null, paid_out_date,null)) is null
        ,if(date_sub('${ST9}',1) >= max(pmt_due_date), max(loan_init_term), min(case when date_sub('${ST9}',1) < pmt_due_date then curr_term end))  --如果未结清
        ,null
        )    --如果已结清   
    as no_settle_loan_term                        --未结清当前期数        
    ,if(max(if(loan_init_term=curr_term and paid_out_date is not null, paid_out_date,null)) is null
        ,if(date_sub('${ST9}',1) >= max(pmt_due_date), max(pmt_due_date),min(case when date_sub('${ST9}',1) < pmt_due_date then pmt_due_date end)) --如果未结清
        ,null)
    as no_settle_should_repay_date    --未结清应还日    
    ,collect_list(concat(curr_term, '~~', pmt_due_date))  as term_pmt_due_date_list    
    ,product_code   
    from
    stage.ecas_repay_schedule${tb_suffix}
      where 1 > 0
      and d_date = date_sub('${ST9}',1)
      and curr_term > 0
      and product_code in (${product_id})
      ${p_types}
      group by due_bill_no,product_code
    )  t
    LATERAL VIEW explode(term_pmt_due_date_list) pmt as term_pmt_due_date
    group by t.due_bill_no,product_code
    )  schedule 
  on ecas_loan.due_bill_no = schedule.due_bill_no
  and ecas_loan.product_id = schedule.product_code
) as yesterday
on  is_empty(today.product_id             ,'a') = is_empty(yesterday.product_id             ,'a')
and is_empty(today.due_bill_no            ,'a') = is_empty(yesterday.due_bill_no            ,'a')
and is_empty(today.apply_no               ,'a') = is_empty(yesterday.apply_no               ,'a')
and is_empty(today.loan_active_date       ,'a') = is_empty(yesterday.loan_active_date       ,'a')
and is_empty(today.loan_init_term         ,'a') = is_empty(yesterday.loan_init_term         ,'a')
and is_empty(today.loan_term              ,'a') = is_empty(yesterday.loan_term              ,'a')
and is_empty(today.should_repay_date      ,'a') = is_empty(yesterday.should_repay_date      ,'a')
and is_empty(today.loan_term_repaid       ,'a') = is_empty(yesterday.loan_term_repaid       ,'a')
and is_empty(today.loan_term_remain       ,'a') = is_empty(yesterday.loan_term_remain       ,'a')
and is_empty(today.loan_status            ,'a') = is_empty(yesterday.loan_status            ,'a')
and is_empty(today.loan_out_reason        ,'a') = is_empty(yesterday.loan_out_reason        ,'a')
and is_empty(today.paid_out_type          ,'a') = is_empty(yesterday.paid_out_type          ,'a')
and is_empty(today.paid_out_date          ,'a') = is_empty(yesterday.paid_out_date          ,'a')
and is_empty(today.terminal_date          ,'a') = is_empty(yesterday.terminal_date          ,'a')
and is_empty(today.loan_init_principal    ,  0) = is_empty(yesterday.loan_init_principal    ,  0)
and is_empty(today.loan_init_interest_rate,  0) = is_empty(yesterday.loan_init_interest_rate,  0)
and is_empty(today.loan_init_interest     ,  0) = is_empty(yesterday.loan_init_interest     ,  0)
and is_empty(today.loan_init_term_fee_rate,  0) = is_empty(yesterday.loan_init_term_fee_rate,  0)
and is_empty(today.loan_init_term_fee     ,  0) = is_empty(yesterday.loan_init_term_fee     ,  0)
and is_empty(today.loan_init_svc_fee_rate ,  0) = is_empty(yesterday.loan_init_svc_fee_rate ,  0)
and is_empty(today.loan_init_svc_fee      ,  0) = is_empty(yesterday.loan_init_svc_fee      ,  0)
and is_empty(today.loan_init_penalty_rate ,  0) = is_empty(yesterday.loan_init_penalty_rate ,  0)
and is_empty(today.paid_principal         ,  0) = is_empty(yesterday.paid_principal         ,  0)
and is_empty(today.overdue_principal      ,  0) = is_empty(yesterday.overdue_principal      ,  0)
and is_empty(today.overdue_interest       ,  0) = is_empty(yesterday.overdue_interest       ,  0)
and is_empty(today.overdue_svc_fee        ,  0) = is_empty(yesterday.overdue_svc_fee        ,  0)
and is_empty(today.overdue_term_fee       ,  0) = is_empty(yesterday.overdue_term_fee       ,  0)
and is_empty(today.overdue_penalty        ,  0) = is_empty(yesterday.overdue_penalty        ,  0)
and is_empty(today.overdue_mult_amt       ,  0) = is_empty(yesterday.overdue_mult_amt       ,  0)
and is_empty(today.overdue_date           ,'a') = is_empty(yesterday.overdue_date           ,'a')
and is_empty(today.overdue_days           ,'a') = is_empty(yesterday.overdue_days           ,'a')
where yesterday.due_bill_no is null
-- limit 10
;
