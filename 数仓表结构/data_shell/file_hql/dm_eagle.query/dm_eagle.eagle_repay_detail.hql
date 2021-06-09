set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;
set hive.groupby.orderby.position.alias=true;
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
-- 设置可以使用正则表达式查找字段
set hive.support.quoted.identifiers=None;


insert overwrite table dm_eagle${db_suffix}.eagle_repay_detail partition(biz_date,product_id)
select
  repay.due_bill_no,
  loan.loan_active_date,
  loan.loan_init_term,
  repay.repay_term,
  repay.order_id,
  loan.loan_status,
  loan.loan_status_cn,
  loan.overdue_days,
  repay.payment_id,
  repay.txn_time,
  repay.post_time,
  repay.bnp_type,
  repay.bnp_type_cn,
  repay.repay_amount,
  repay.batch_date,
  repay.create_time,
  repay.update_time,
  repay.biz_date,
  repay.product_id
from ods${db_suffix}.repay_detail repay
left join (
    select `(product_id)?+.+` from ods${db_suffix}.loan_info
    where 1 > 0
    and '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ${hive_param_str}
) loan
on repay.due_bill_no = loan.due_bill_no
where 1 > 0
  and repay.biz_date = '${ST9}' ${hive_param_str}
;
