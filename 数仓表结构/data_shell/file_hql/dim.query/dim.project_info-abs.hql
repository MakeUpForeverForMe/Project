-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;

insert overwrite table dim.project_info partition(project_id)
select
  project_name,
  project_stage,
  asset_side,
  fund_side,
  year,
  term,
  remarks,
  project_full_name,
  asset_type,
  project_type,
  mode,
  project_time,
  project_begin_date,
  project_end_date,
  asset_pool_type,
  public_offer,
  data_source,
  create_user,
  create_time,
  update_time,
  project_id
from dim.project_info_json
where 1 > 0
  and project_id = '${exe_id}'
  and row_type != 'delete'
-- limit 10
;
