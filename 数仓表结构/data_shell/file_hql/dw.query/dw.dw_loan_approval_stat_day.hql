set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
 

insert overwrite table dw.dw_loan_approval_stat_day partition(biz_date,product_id)
select
  nvl(loan_terms,loan_terms_count)                                           as loan_terms,
  '${ST9}'                                                                   as loan_approval_date,
  nvl(loan_approval_num,0)                                                   as loan_approval_num,
  nvl(loan_approval_num_count,0)                                             as loan_approval_num_count,
  nvl(loan_approval_num_person,0)                                            as loan_approval_num_person,
  nvl(loan_approval_num_person_count,0)                                      as loan_approval_num_person_count,
  nvl(loan_approval_amount,0)                                                as loan_approval_amount,
  nvl(loan_approval_amount_count,0)                                          as loan_approval_amount_count,
  '${ST9}'                                                                   as biz_date,
  nvl(product_id,product_id_count)                                           as product_id
from (
  select
    product_id                   as product_id,
    loan_terms                   as loan_terms,
    count(distinct due_bill_no)  as loan_approval_num,
    count(distinct user_hash_no) as loan_approval_num_person,
    sum(loan_amount_approval)    as loan_approval_amount
  from (
    select
      user_hash_no,
      due_bill_no,
      loan_terms,
      nvl(loan_amount_approval,0) as loan_amount_approval,
      product_id
    from ods.loan_apply
    where 1 > 0
      and apply_status in (1,4)
      and to_date(issue_time) = '${ST9}'
      ${hive_param_str}
  ) as tmp
  group by product_id,loan_terms
) as loan_apply
full join (
  select
    product_id                   as product_id_count,
    loan_terms                   as loan_terms_count,
    count(distinct due_bill_no)  as loan_approval_num_count,
    count(distinct user_hash_no) as loan_approval_num_person_count,
    sum(loan_amount_approval)    as loan_approval_amount_count
  from (
    select
      user_hash_no,
      due_bill_no,
      loan_terms,
      nvl(loan_amount_approval,0) as loan_amount_approval,
      product_id
    from ods.loan_apply
    where 1 > 0
      and apply_status in (1,4)
      and to_date(issue_time) <= '${ST9}'
      ${hive_param_str}
  ) as tmp
  group by product_id,loan_terms
) as ods_count
on  product_id = product_id_count
and loan_terms = loan_terms_count
-- limit 10
;
