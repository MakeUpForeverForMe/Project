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
select distinct
  case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
  if(size(split(serial_number,'_')) = 2,split(serial_number,'_')[1],split(serial_number,'_')[0]) as due_bill_no,
  package_remain_principal,
  package_remain_periods,
  asset_bag_id
from stage.abs_t_basic_asset
where asset_bag_id is not null;
