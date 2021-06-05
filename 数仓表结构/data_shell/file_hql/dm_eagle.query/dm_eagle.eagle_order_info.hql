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

-- set hivevar:db_suffix=;
-- set hivevar:db_suffix=_cps;

-- set hivevar:ST9=2020-09-27;
-- set hivevar:ET9=2020-11-08;

insert overwrite table dm_eagle${db_suffix}.eagle_order_info partition(biz_date,product_id)
select
  `(biz_date|product_id)?+.+`,
  biz_date,
  product_id
from ods${db_suffix}.order_info
where 1 > 0
  and biz_date = '${ST9}' ${hive_param_str}
  -- and biz_date between '${ST9}' and '${ET9}'
;
