set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;


insert overwrite table dw${db_suffix}.dw_loan_base_stat_loan_num_day partition(biz_date = '${ST9}',product_id)
select
  is_empty(loan_init_term,loan_init_term_count)                    as loan_terms,
  is_empty(loan_num,0)                                             as loan_num,
  is_empty(loan_num_count,0)                                       as loan_num_count,
  is_empty(loan_principal,0)                                       as loan_principal,
  is_empty(loan_principal_count,0)                                 as loan_principal_count,
  is_empty(product_id,product_id_count)                            as product_id
from (
  select
    loan_init_term           as loan_init_term,
    count(due_bill_no)       as loan_num,
    sum(loan_init_principal) as loan_principal,
    product_id               as product_id
  from (
    select distinct
      due_bill_no,
      loan_init_term,
      loan_init_principal,
      loan_active_date,
      product_id
    from ods${db_suffix}.loan_lending
    where 1 > 0
      and biz_date = '${ST9}'
      ${hive_param_str}
  ) as tmp
  group by loan_init_term,product_id
) as loan_active_num
full join (
  select
    loan_init_term           as loan_init_term_count,
    count(due_bill_no)       as loan_num_count,
    sum(loan_init_principal) as loan_principal_count,
    product_id               as product_id_count
  from (
    select distinct
      due_bill_no,
      loan_init_term,
      loan_init_principal,
      product_id
    from ods${db_suffix}.loan_lending
    where 1 > 0
      and biz_date <= '${ST9}'
      ${hive_param_str}
  ) as tmp
  group by loan_init_term,product_id
) as loan_active_count
on  product_id     = product_id_count
and loan_init_term = loan_init_term_count
-- limit 1
;
