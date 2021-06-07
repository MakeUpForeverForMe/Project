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
  project_name       as project_name,
  project_stage      as project_stage,
  null               as asset_side,
  null               as fund_side,
  null               as year,
  null               as term,
  null               as remarks,
  project_full_name  as project_full_name,
  asset_type         as asset_type,
  project_type       as project_type,
  mode               as mode,
  project_time       as project_time,
  project_begin_date as project_begin_date,
  project_end_date   as project_end_date,
  asset_pool_type    as asset_pool_type,
  public_offer       as public_offer,
  data_source        as data_source,
  create_user        as create_user,
  create_time        as create_time,
  update_time        as update_time,
  case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id
from stage.abs_t_project
where 1 > 0
  and project_id not in (
    'PL202102010096', -- 1-1-1-1年第1期
    'PL201907050063', -- WY-中航-消费分期-2019年第1期
    'PL201908220067', -- 东亚中国-银登-车位分期-2019年第1期-te
    ''
  )
-- limit 100
;
