set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
 

insert overwrite table dw.dw_loan_apply_stat_day partition(biz_date,product_id)
select
  loan_terms,
  loan_apply_date                                             as loan_apply_date,
  count(due_bill_no)                                          as loan_apply_num,
  count(distinct user_hash_no)                                as loan_apply_num_person,
  sum(loan_amount_apply)                                      as loan_apply_amount,
  loan_approval_date                                          as loan_approval_date,
  count(if(apply_status in (1,4),due_bill_no,null))           as loan_approval_num,
  count(distinct if(apply_status in (1,4),user_hash_no,null)) as loan_approval_num_person,
  sum(if(apply_status in (1,4),loan_amount_approval,0))       as loan_approval_amount,
  loan_apply_date                                          as biz_date,
  product_id
from (
  select
    user_hash_no,
    apply_id,
    due_bill_no,
    loan_terms,
    to_date(loan_apply_time) as loan_apply_date,
    loan_amount_apply,
    apply_status,
    to_date(issue_time)      as loan_approval_date,
    loan_amount_approval,
    product_id
  from ods.loan_apply
  where 1 > 0
    and biz_date = '${ST9}'
    ${hive_param_str}
) as loan_apply
group by
  loan_terms,
  loan_apply_date,
  loan_approval_date,
  product_id
-- order by product_id,loan_apply_date
-- limit 10
;
