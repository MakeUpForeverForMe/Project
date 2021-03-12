set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;        -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;            -- 关闭自动 MapJoin
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;


insert overwrite table dm_eagle${db_suffix}.eagle_repay_schedule partition(product_id)
select
  due_bill_no               as due_bill_no,
  loan_active_date          as loan_active_date,
  loan_init_principal       as loan_init_principal,
  loan_init_term            as loan_init_term,
  loan_term                 as loan_term,
  start_interest_date       as start_interest_date,
  should_repay_date         as should_repay_date,
  should_repay_date_history as should_repay_date_history,
  grace_date                as grace_date,
  should_repay_amount       as should_repay_amount,
  should_repay_principal    as should_repay_principal,
  should_repay_interest     as should_repay_interest,
  should_repay_term_fee     as should_repay_term_fee,
  should_repay_svc_fee      as should_repay_svc_fee,
  should_repay_penalty      as should_repay_penalty,
  should_repay_mult_amt     as should_repay_mult_amt,
  schedule_status           as schedule_status,
  null                      as repaid_num,
  paid_amount               as paid_amount,
  paid_principal            as paid_principal,
  paid_interest             as paid_interest,
  paid_term_fee             as paid_term_fee,
  paid_svc_fee              as paid_svc_fee,
  paid_penalty              as paid_penalty,
  paid_mult                 as paid_mult,
  reduce_amount             as reduce_amount,
  reduce_principal          as reduce_principal,
  reduce_interest           as reduce_interest,
  reduce_term_fee           as reduce_term_fee,
  reduce_svc_fee            as reduce_svc_fee,
  reduce_penalty            as reduce_penalty,
  reduce_mult_amt           as reduce_mult_amt,
  null                      as schedule_id,
  s_d_date                  as s_d_date,
  e_d_date                  as e_d_date,
  product_id                as product_id
from ods_new_s.repay_schedule
  where 1 > 0 ${hive_param_str}
--    and product_id ='${product_id}'
;
