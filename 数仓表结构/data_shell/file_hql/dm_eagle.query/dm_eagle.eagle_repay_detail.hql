set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;


insert overwrite table dm_eagle${db_suffix}.eagle_repay_detail partition(biz_date,product_id)
select
  due_bill_no,
  loan_active_date,
  loan_init_term,
  repay_term,
  order_id,
  loan_status,
  loan_status_cn,
  overdue_days,
  payment_id,
  txn_time,
  post_time,
  bnp_type,
  bnp_type_cn,
  repay_amount,
  batch_date,
  create_time,
  update_time,
  biz_date,
  product_id
from ods_new_s${db_suffix}.repay_detail
where 1 > 0
  and biz_date = '${ST9}' ${hive_param_str}
;
