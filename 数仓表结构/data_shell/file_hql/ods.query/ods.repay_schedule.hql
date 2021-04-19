set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

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

-- 设置可以使用正则匹配 `(a|b)?+.+`
set hive.support.quoted.identifiers=None;


-- 代偿前
-- set hivevar:db_suffix=;
-- 代称后
-- set hivevar:db_suffix=_cps;

set hivevar:product_id=;

-- set hivevar:product_id=and product_id not in (
--   '001801','001802','001803','001804',
--   '001901','001902','001903','001904','001905','001906','001907',
--   '002001','002002','002003','002004','002005','002006','002007',
--   '002401','002402',
--   ''
-- );
-- set hivevar:product_id=and product_id in ('DIDI201908161538','001601','001602','001603','001701','001702','002201','002202','002203');


insert overwrite table ods${db_suffix}.repay_schedule partition(is_settled = 'no',product_id)
select
  `(biz_date|product_id)?+.+`,
  biz_date as s_d_date,
  nvl(lead(biz_date) over(partition by product_id,due_bill_no,loan_term order by biz_date),'3000-12-31') as e_d_date,
  product_id
from ods${db_suffix}.repay_schedule_inter
where 1 > 0
  ${product_id}
-- limit 10
;
