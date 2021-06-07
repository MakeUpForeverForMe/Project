-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


insert overwrite table dim.bag_info partition(bag_id)
select
  case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
  asset_bag_name            as bag_name,
  status                    as bag_status,
  package_principal_balance as bag_remain_principal,
  bag_date                  as bag_date,
  to_date(update_time)      as insert_date,
  asset_bag_id              as bag_id
from stage.abs_t_asset_bag
where 1 > 0
  and status != 1
-- limit 100
;
