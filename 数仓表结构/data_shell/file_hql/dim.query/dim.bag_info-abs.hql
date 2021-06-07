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
  project_id,
  bag_name,
  case bag_status
    when '未封包'   then '1'
    when '已封包'   then '2'
    when '已解包'   then '3'
    when '封包中'   then '4'
    when '封包失败' then '5'
    when '已发行'   then '6'
    else bag_status
  end             as bag_status,
  bag_remain_principal,
  bag_date,
  insert_date,
  bag_id
from dim.bag_info_json
where 1 > 0
  and bag_id = '${exe_id}'
  and row_type != 'delete'
-- limit 10
;
