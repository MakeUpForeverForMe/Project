-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
-- 设置可以使用正则匹配 `(a|b)?+.+`
set hive.support.quoted.identifiers=None;




-- insert overwrite table ods${db_suffix}.repay_schedule partition(is_settled = 'no',product_id)
select
  `(biz_date|product_id)?+.+`,
  biz_date as s_d_date,
  lead(biz_date) over(partition by product_id,due_bill_no,loan_term order by biz_date) as e_d_date,
  product_id
from ods${db_suffix}.repay_schedule_inter
limit 10
;
