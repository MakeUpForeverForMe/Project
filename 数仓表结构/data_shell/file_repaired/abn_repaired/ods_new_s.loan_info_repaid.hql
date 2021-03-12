set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=false;
set hive.mapjoin.optimized.hashtable=false;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;
set hive.support.quoted.identifiers=None;


-- DROP TABLE IF EXISTS `ods_new_s${db_suffix}.his_loan_info_repaired`;
CREATE TABLE IF NOT EXISTS `ods_new_s${db_suffix}.his_loan_info_repaired` like `ods_new_s${db_suffix}.loan_info`;

DROP TABLE IF EXISTS `ods_new_s${db_suffix}.his_loan_info_tmp_repaired`;
CREATE TABLE IF NOT EXISTS `ods_new_s${db_suffix}.his_loan_info_tmp_repaired` like `ods_new_s${db_suffix}.loan_info`;


insert overwrite table ods_new_s${db_suffix}.his_loan_info_tmp_repaired partition(is_settled = 'no',product_id)
select
  `(is_settled)?+.+`
from ods_new_s${db_suffix}.his_loan_info_repaired
where 1 > 0
  and is_settled = 'no'
  and product_id in (${product_id})
  and s_d_date < '${ST9}'
  ${due_bill_no}
  -- and to_date(effective_time) <= date_add('${ST9}',1)
-- distribute by product_id,loan_active_date
;



with ods_new_s_loan as (
  select
    today.product_id                                                                 as product_id,
    today.due_bill_no                                                                as due_bill_no,
    today.apply_no                                                                   as apply_no,
    today.loan_active_date                                                           as loan_active_date,
    today.loan_init_principal                                                        as loan_init_principal,
    today.loan_init_term                                                             as loan_init_term,
    today.loan_term                                                                  as loan_term,
    today.should_repay_date                                                          as should_repay_date,
    today.loan_term_repaid                                                           as loan_term_repaid,
    today.loan_term_remain                                                           as loan_term_remain,
    today.loan_status                                                                as loan_status,
    today.loan_status_cn                                                             as loan_status_cn,
    today.loan_out_reason                                                            as loan_out_reason,
    today.paid_out_type                                                              as paid_out_type,
    today.paid_out_type_cn                                                           as paid_out_type_cn,
    today.paid_out_date                                                              as paid_out_date,
    today.terminal_date                                                              as terminal_date,
    today.loan_init_interest                                                         as loan_init_interest,
    today.loan_init_term_fee                                                         as loan_init_term_fee,
    today.loan_init_svc_fee                                                          as loan_init_svc_fee,
    today.paid_amount                                                                as paid_amount,
    today.paid_principal                                                             as paid_principal,
    today.paid_interest                                                              as paid_interest,
    today.paid_penalty                                                               as paid_penalty,
    today.paid_svc_fee                                                               as paid_svc_fee,
    today.paid_term_fee                                                              as paid_term_fee,
    today.paid_mult                                                                  as paid_mult,
    today.remain_amount                                                              as remain_amount,
    today.remain_principal                                                           as remain_principal,
    today.remain_interest                                                            as remain_interest,
    today.remain_svc_fee                                                             as remain_svc_fee,
    today.remain_term_fee                                                            as remain_term_fee,
    today.overdue_principal                                                          as overdue_principal,
    today.overdue_interest                                                           as overdue_interest,
    today.overdue_svc_fee                                                            as overdue_svc_fee,
    today.overdue_term_fee                                                           as overdue_term_fee,
    today.overdue_penalty                                                            as overdue_penalty,
    today.overdue_mult_amt                                                           as overdue_mult_amt,
    today.overdue_date                                                               as overdue_date_start,
    today.overdue_days                                                               as overdue_days,
    cast(date_add(today.overdue_date,cast(today.overdue_days as int) - 1) as string) as overdue_date,
    today.collect_out_date                                                           as collect_out_date,
    today.overdue_term                                                               as overdue_term,
    today.overdue_terms_max                                                          as overdue_terms_max,
    today.d_date                                                                     as d_date,
    '3000-12-31'                                                                     as e_d_date,
    today.create_time                                                                as create_time,
    today.update_time                                                                as update_time,
    cast('3000-12-31 00:00:00' as timestamp)                                         as expire_time
  from (
    select
      ecas_loan.product_id,
      ecas_loan.due_bill_no,
      ecas_loan.apply_no,
      ecas_loan.loan_active_date,
      ecas_loan.loan_init_term,
      case
      when ecas_loan.paid_out_date = ecas_loan.loan_active_date then 1
      when ecas_loan.paid_out_date is null                      then repay_schedule.loan_term2
      when '${ST9}' <= ecas_loan.paid_out_date                  then repay_schedule.loan_term2
      else null end as loan_term,
      if(
        (ecas_loan.paid_out_date = ecas_loan.loan_active_date and ecas_loan.loan_term = 1) or ecas_loan.paid_out_date is null or '${ST9}' <= ecas_loan.paid_out_date,
        repay_schedule.should_repay_date,
        null
      ) as should_repay_date,
      ecas_loan.loan_term_repaid,
      ecas_loan.loan_term_remain,
      ecas_loan.loan_status,
      ecas_loan.loan_status_cn,
      ecas_loan.loan_out_reason,
      ecas_loan.paid_out_type,
      ecas_loan.paid_out_type_cn,
      ecas_loan.paid_out_date,
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
        when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
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
          else loan_status end
        when 'N' then '正常'
        when 'O' then '逾期'
        when 'F' then '已还清'
        else
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
          else loan_status end
        end                               as loan_status_cn,
        terminal_reason_cd                as loan_out_reason,
        loan_settle_reason                as paid_out_type,
        case loan_settle_reason
        when 'NORMAL_SETTLE'  then '正常结清'
        when 'OVERDUE_SETTLE' then '逾期结清'
        when 'PRE_SETTLE'     then '提前结清'
        when 'REFUND'         then '退车'
        when 'REDEMPTION'     then '赎回'
        when 'BANK_REF'       then '退票结清'
        when 'BUY_BACK'       then '资产回购'
        when 'CAPITAL_VERI'   then '资产核销'
        when 'DISPOSAL'       then '处置结束'
        when 'OVER_COMP'      then '逾期代偿'
        else loan_settle_reason
        end                               as paid_out_type_cn,
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
      from ods.ecas_loan${tb_suffix}
      where 1 > 0
        and d_date = '${ST9}'
        and product_code in (${product_id})
        ${due_bill_no}
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
      from ods.ecas_repay_hst${tb_suffix}
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
        ${due_bill_no}
        -- and d_date = date_sub(current_date(),2)
        -- and txn_date <= '${ST9}'
      group by due_bill_no,d_date
    ) as repay_detail
    on ecas_loan.due_bill_no = repay_detail.due_bill_no
    left join (
      select
        due_bill_no,
        min(curr_term)    as loan_term2,
        min(pmt_due_date) as should_repay_date,
        product_code      as product_id
      from ods.ecas_repay_schedule${tb_suffix}
      where 1 > 0
        and d_date = '${ST9}'                               -- 取快照日当天的所有还款计划数据
        and d_date <= nvl(origin_pmt_due_date,pmt_due_date) -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0                                   -- 过滤掉汇通的第 0 期的情况
        and product_code in (${product_id})
        ${due_bill_no}
      group by due_bill_no,product_code
    ) as repay_schedule
    on ecas_loan.due_bill_no = repay_schedule.due_bill_no
    left join (
      select
        due_bill_no,
        max(curr_term)   as overdue_term,
        count(curr_term) as overdue_terms_max,
        d_date
      from ods.ecas_repay_schedule${tb_suffix}
      where 1 > 0
        and d_date = '${ST9}'
        and pmt_due_date <= d_date
        and product_code in (${product_id})
        and case
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
          else schedule_status
        end = 'O'
        ${due_bill_no}
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
      case
      when ecas_loan.paid_out_date = ecas_loan.loan_active_date then 1
      when ecas_loan.paid_out_date is null                      then repay_schedule.loan_term2
      when '${ST9}' <= ecas_loan.paid_out_date                  then repay_schedule.loan_term2
      else null end as loan_term,
      if(
        (ecas_loan.paid_out_date = ecas_loan.loan_active_date and ecas_loan.loan_term = 1) or ecas_loan.paid_out_date is null or '${ST9}' <= ecas_loan.paid_out_date,
        repay_schedule.should_repay_date,
        null
      ) as should_repay_date,
      ecas_loan.loan_term_repaid,
      ecas_loan.loan_term_remain,
      ecas_loan.loan_status,
      ecas_loan.loan_out_reason,
      ecas_loan.paid_out_type,
      ecas_loan.paid_out_date,
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
        when product_code = '001801' and due_bill_no = '1120060510474405236124' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060510483166645034' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060510483511719117' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511110961602615' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511132383103016' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511174298184833' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511312194364430' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
        when product_code = '001801' and due_bill_no = '1120060511315016692632' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 1
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
        else loan_status end              as loan_status,
        terminal_reason_cd                as loan_out_reason,
        loan_settle_reason                as paid_out_type,
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
        else overdue_days end             as overdue_days
      from ods.ecas_loan${tb_suffix}
      where 1 > 0
        and d_date = date_sub('${ST9}',1)
        and product_code in (${product_id})
        ${due_bill_no}
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
      from ods.ecas_repay_hst${tb_suffix}
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
        ${due_bill_no}
        -- and d_date = date_sub(date_sub(current_date(),2),1)
        -- and txn_date <= date_sub('${ST9}',1)
      group by due_bill_no,d_date
    ) as repay_detail
    on ecas_loan.due_bill_no = repay_detail.due_bill_no
    left join (
      select
        due_bill_no,
        min(curr_term)    as loan_term2,
        min(pmt_due_date) as should_repay_date,
        product_code      as product_id
      from ods.ecas_repay_schedule${tb_suffix}
      where 1 > 0
        and d_date = date_sub('${ST9}',1)                   -- 取快照日当天的所有还款计划数据
        and d_date <= nvl(origin_pmt_due_date,pmt_due_date) -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0                                   -- 过滤掉汇通的第 0 期的情况
        and product_code in (${product_id})
        ${due_bill_no}
      group by due_bill_no,product_code
    ) as repay_schedule
    on ecas_loan.due_bill_no = repay_schedule.due_bill_no
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
  and is_empty(today.loan_init_principal    ,'a') = is_empty(yesterday.loan_init_principal    ,'a')
  and is_empty(today.loan_init_interest_rate,'a') = is_empty(yesterday.loan_init_interest_rate,'a')
  and is_empty(today.loan_init_interest     ,'a') = is_empty(yesterday.loan_init_interest     ,'a')
  and is_empty(today.loan_init_term_fee_rate,'a') = is_empty(yesterday.loan_init_term_fee_rate,'a')
  and is_empty(today.loan_init_term_fee     ,'a') = is_empty(yesterday.loan_init_term_fee     ,'a')
  and is_empty(today.loan_init_svc_fee_rate ,'a') = is_empty(yesterday.loan_init_svc_fee_rate ,'a')
  and is_empty(today.loan_init_svc_fee      ,'a') = is_empty(yesterday.loan_init_svc_fee      ,'a')
  and is_empty(today.loan_init_penalty_rate ,'a') = is_empty(yesterday.loan_init_penalty_rate ,'a')
  and is_empty(today.paid_principal         ,'a') = is_empty(yesterday.paid_principal         ,'a')
  and is_empty(today.overdue_principal      ,'a') = is_empty(yesterday.overdue_principal      ,'a')
  and is_empty(today.overdue_interest       ,'a') = is_empty(yesterday.overdue_interest       ,'a')
  and is_empty(today.overdue_svc_fee        ,'a') = is_empty(yesterday.overdue_svc_fee        ,'a')
  and is_empty(today.overdue_term_fee       ,'a') = is_empty(yesterday.overdue_term_fee       ,'a')
  and is_empty(today.overdue_penalty        ,'a') = is_empty(yesterday.overdue_penalty        ,'a')
  and is_empty(today.overdue_mult_amt       ,'a') = is_empty(yesterday.overdue_mult_amt       ,'a')
  and is_empty(today.overdue_date           ,'a') = is_empty(yesterday.overdue_date           ,'a')
  and is_empty(today.overdue_days           ,'a') = is_empty(yesterday.overdue_days           ,'a')
  where yesterday.due_bill_no is null
)
insert overwrite table ods_new_s${db_suffix}.his_loan_info_repaired partition(is_settled = 'no',product_id)
select
  due_bill_no,
  apply_no,
  loan_active_date,
  loan_init_principal,
  loan_init_term,
  max(loan_term)         over(partition by due_bill_no order by s_d_date) as loan_term,
  max(should_repay_date) over(partition by due_bill_no order by s_d_date) as should_repay_date,
  loan_term_repaid,
  loan_term_remain,
  loan_init_interest,
  loan_init_term_fee,
  loan_init_svc_fee,
  loan_status,
  loan_status_cn,
  loan_out_reason,
  paid_out_type,
  paid_out_type_cn,
  paid_out_date,
  terminal_date,
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
  min(overdue_date_start) over(partition by due_bill_no order by s_d_date) as overdue_date_first,
  overdue_date_start,
  overdue_days,
  overdue_date,
  overdue_date_start as dpd_begin_date,
  overdue_days as dpd_days,
  0 as dpd_days_count,
  max(overdue_days) over(partition by due_bill_no order by s_d_date) as dpd_days_max,
  collect_out_date as collect_out_date,
  overdue_term,
  count(distinct if(overdue_days > 0,overdue_term,null)) over(partition by due_bill_no order by s_d_date)    as overdue_terms_count,
  max(overdue_terms_max)                                 over(partition by due_bill_no order by s_d_date)    as overdue_terms_max,
  nvl(sum(distinct overdue_principal)                    over(partition by due_bill_no order by s_d_date),0) as overdue_principal_accumulate,
  nvl(max(overdue_principal)                             over(partition by due_bill_no order by s_d_date),0) as overdue_principal_max,
  s_d_date,
  e_d_date,
  effective_time,
  expire_time,
  product_id
from (
  select
    loan_info.due_bill_no,
    loan_info.apply_no,
    loan_info.loan_active_date,
    loan_info.loan_init_principal,
    loan_info.loan_init_term,
    loan_info.loan_term,
    loan_info.should_repay_date,
    loan_info.loan_term_repaid,
    loan_info.loan_term_remain,
    loan_info.loan_status,
    loan_info.loan_status_cn,
    loan_info.loan_out_reason,
    loan_info.paid_out_type,
    loan_info.paid_out_type_cn,
    loan_info.paid_out_date,
    loan_info.terminal_date,
    loan_info.loan_init_interest,
    loan_info.loan_init_term_fee,
    loan_info.loan_init_svc_fee,
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
    loan_info.remain_svc_fee,
    loan_info.remain_term_fee,
    loan_info.overdue_principal,
    loan_info.overdue_interest,
    loan_info.overdue_svc_fee,
    loan_info.overdue_term_fee,
    loan_info.overdue_penalty,
    loan_info.overdue_mult_amt,
    loan_info.overdue_date_start,
    loan_info.overdue_days,
    loan_info.overdue_date,
    loan_info.collect_out_date,
    loan_info.overdue_term,
    loan_info.overdue_terms_max,
    loan_info.s_d_date,
    if(loan_info.e_d_date > '${ST9}' and ods_new_s_loan.due_bill_no is not null,ods_new_s_loan.d_date,if(loan_info.e_d_date = '${ST9}' and ods_new_s_loan.due_bill_no is null,'3000-12-31',loan_info.e_d_date)) as e_d_date,
    loan_info.effective_time,
    if(to_date(loan_info.expire_time) > '${ST9}' and ods_new_s_loan.due_bill_no is not null,ods_new_s_loan.update_time,loan_info.expire_time) as expire_time,
    loan_info.product_id
  from (select * from ods_new_s${db_suffix}.his_loan_info_tmp_repaired where 1 > 0 and product_id in (${product_id})) as loan_info
  left join ods_new_s_loan
  on loan_info.due_bill_no = ods_new_s_loan.due_bill_no
  union all
  select
    ods_new_s_loan.due_bill_no,
    ods_new_s_loan.apply_no,
    ods_new_s_loan.loan_active_date,
    ods_new_s_loan.loan_init_principal,
    ods_new_s_loan.loan_init_term,
    ods_new_s_loan.loan_term,
    ods_new_s_loan.should_repay_date,
    ods_new_s_loan.loan_term_repaid,
    ods_new_s_loan.loan_term_remain,
    ods_new_s_loan.loan_status,
    ods_new_s_loan.loan_status_cn,
    ods_new_s_loan.loan_out_reason,
    ods_new_s_loan.paid_out_type,
    ods_new_s_loan.paid_out_type_cn,
    ods_new_s_loan.paid_out_date,
    ods_new_s_loan.terminal_date,
    ods_new_s_loan.loan_init_interest,
    ods_new_s_loan.loan_init_term_fee,
    ods_new_s_loan.loan_init_svc_fee,
    ods_new_s_loan.paid_amount,
    ods_new_s_loan.paid_principal,
    ods_new_s_loan.paid_interest,
    ods_new_s_loan.paid_penalty,
    ods_new_s_loan.paid_svc_fee,
    ods_new_s_loan.paid_term_fee,
    ods_new_s_loan.paid_mult,
    ods_new_s_loan.remain_amount,
    ods_new_s_loan.remain_principal,
    ods_new_s_loan.remain_interest,
    ods_new_s_loan.remain_svc_fee,
    ods_new_s_loan.remain_term_fee,
    ods_new_s_loan.overdue_principal,
    ods_new_s_loan.overdue_interest,
    ods_new_s_loan.overdue_svc_fee,
    ods_new_s_loan.overdue_term_fee,
    ods_new_s_loan.overdue_penalty,
    ods_new_s_loan.overdue_mult_amt,
    ods_new_s_loan.overdue_date_start,
    ods_new_s_loan.overdue_days,
    ods_new_s_loan.overdue_date,
    ods_new_s_loan.collect_out_date,
    ods_new_s_loan.overdue_term,
    ods_new_s_loan.overdue_terms_max,
    ods_new_s_loan.d_date as s_d_date,
    ods_new_s_loan.e_d_date,
    ods_new_s_loan.update_time as effective_time,
    ods_new_s_loan.expire_time,
    ods_new_s_loan.product_id
  from ods_new_s_loan
) as tmp
-- where 1 > 0
--   and due_bill_no = '1120061421344483293354'
-- limit 1
;
