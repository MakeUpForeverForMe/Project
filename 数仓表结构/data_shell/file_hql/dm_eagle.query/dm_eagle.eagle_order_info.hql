set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.maxRemoteBlockSizeFetchToMem=4G;
set hive.auto.convert.join=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.support.quoted.identifiers=None;     -- 设置可以使用正则表达式查找字段

-- set hivevar:db_suffix=;
-- set hivevar:db_suffix=_cps;

-- set hivevar:ST9=2020-09-27;
-- set hivevar:ET9=2020-11-08;

insert overwrite table dm_eagle${db_suffix}.eagle_order_info partition(biz_date,product_id)
select
  `(biz_date|product_id)?+.+`,
  biz_date,
  product_id
from ods_new_s${db_suffix}.order_info
where 1 > 0
  and biz_date = '${ST9}' ${hive_param_str}
  -- and biz_date between '${ST9}' and '${ET9}'
;
