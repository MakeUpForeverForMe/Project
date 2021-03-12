set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
 

insert overwrite table dw${db_suffix}.dw_loan_base_stat_repay_detail_day partition(biz_date = '${ST9}',product_id)
select
  is_empty(loan_init_term,loan_init_term_count) as loan_terms,
  nvl(repaid_num,0)                             as repaid_num,
  nvl(repaid_num_count,0)                       as repaid_num_count,
  nvl(repaid_amount,0)                          as repaid_amount,
  nvl(repaid_principal,0)                       as repaid_principal,
  nvl(repaid_interest_penalty_svc_fee,0)        as repaid_interest_penalty_svc_fee,
  nvl(repaid_interest,0)                        as repaid_interest,
  nvl(repaid_repay_svc_term,0)                  as repaid_repay_svc_term,
  nvl(repaid_repay_term_fee,0)                  as repaid_repay_term_fee,
  nvl(repaid_repay_svc_fee,0)                   as repaid_repay_svc_fee,
  nvl(repaid_penalty,0)                         as repaid_penalty,
  nvl(repaid_amount_count,0)                    as repaid_amount_count,
  nvl(repaid_principal_count,0)                 as repaid_principal_count,
  nvl(repaid_interest_penalty_svc_fee_count,0)  as repaid_interest_penalty_svc_fee_count,
  nvl(repaid_interest_count,0)                  as repaid_interest_count,
  nvl(repaid_repay_svc_term_count,0)            as repaid_repay_svc_term_count,
  nvl(repaid_repay_term_fee_count,0)            as repaid_repay_term_fee_count,
  nvl(repaid_repay_svc_fee_count,0)             as repaid_repay_svc_fee_count,
  nvl(repaid_penalty_count,0)                   as repaid_penalty_count,
  is_empty(product_id,product_id_count)         as product_id
from (
  select
    loan_init_term                                      as loan_init_term,
    repaid_num                                          as repaid_num,
    pricinpal + interest + term_fee + svc_fee + penalty as repaid_amount,
    pricinpal                                           as repaid_principal,
    interest + term_fee + svc_fee + penalty             as repaid_interest_penalty_svc_fee,
    interest                                            as repaid_interest,
    term_fee + svc_fee                                  as repaid_repay_svc_term,
    term_fee                                            as repaid_repay_term_fee,
    svc_fee                                             as repaid_repay_svc_fee,
    penalty                                             as repaid_penalty,
    product_id                                          as product_id
  from (
    select
      loan_init_term,
      count(distinct due_bill_no)                                      as repaid_num,
      sum(case bnp_type when 'Pricinpal' then repay_amount else 0 end) as pricinpal,
      sum(case bnp_type when 'Interest'  then repay_amount else 0 end) as interest,
      sum(case bnp_type when 'TERMFee'   then repay_amount else 0 end) as term_fee,
      sum(case bnp_type when 'SVCFee'    then repay_amount else 0 end) as svc_fee,
      sum(case bnp_type when 'Penalty'   then repay_amount else 0 end) as penalty,
      product_id
    from ods${db_suffix}.repay_detail
    where 1 > 0
      and biz_date = '${ST9}'  ${hive_param_str}
    group by loan_init_term,product_id
  ) as tmp
) as loan_active_num
full join (
  select
    loan_init_term                                      as loan_init_term_count,
    repaid_num                                          as repaid_num_count,
    pricinpal + interest + term_fee + svc_fee + penalty as repaid_amount_count,
    pricinpal                                           as repaid_principal_count,
    interest + term_fee + svc_fee + penalty             as repaid_interest_penalty_svc_fee_count,
    interest                                            as repaid_interest_count,
    term_fee + svc_fee                                  as repaid_repay_svc_term_count,
    term_fee                                            as repaid_repay_term_fee_count,
    svc_fee                                             as repaid_repay_svc_fee_count,
    penalty                                             as repaid_penalty_count,
    product_id                                          as product_id_count
  from (
    select
      loan_init_term,
      count(distinct due_bill_no)                                      as repaid_num,
      sum(case bnp_type when 'Pricinpal' then repay_amount else 0 end) as pricinpal,
      sum(case bnp_type when 'Interest'  then repay_amount else 0 end) as interest,
      sum(case bnp_type when 'TERMFee'   then repay_amount else 0 end) as term_fee,
      sum(case bnp_type when 'SVCFee'    then repay_amount else 0 end) as svc_fee,
      sum(case bnp_type when 'Penalty'   then repay_amount else 0 end) as penalty,
      product_id
    from ods${db_suffix}.repay_detail
    where 1 > 0
      and biz_date <= '${ST9}'  ${hive_param_str}
    group by loan_init_term,product_id
  ) as tmp
) as loan_active_count
on  product_id     = product_id_count
and loan_init_term = loan_init_term_count
;
