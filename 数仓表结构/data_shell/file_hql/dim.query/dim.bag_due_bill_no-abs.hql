-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


insert overwrite table dim.bag_due_bill_no partition(bag_id)
select
  project_id                  as project_id,
  dues.serialNumber           as due_bill_no,
  dues.packageRemainPrincipal as package_remain_principal,
  dues.packageRemainPeriods   as package_remain_periods,
  bag_id                      as import_id
from dim.bag_due_bill_no_json
lateral view explode(due_bill_no) due_bill as dues
where 1 > 0
  and bag_id = '${exe_id}'
  and row_type != 'delete'
-- limit 10
;
