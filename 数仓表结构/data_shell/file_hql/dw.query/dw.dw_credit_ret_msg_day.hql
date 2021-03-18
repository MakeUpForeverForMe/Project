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



insert overwrite table dw.dw_credit_ret_msg_day partition(biz_date,product_id)
select distinct
  resp_msg,
  count(apply_id) over(partition by biz_date,product_id,resp_msg) as ret_msg_num,
  nvl(count(apply_id) over(partition by biz_date,product_id,resp_msg) / count(apply_id) over(partition by biz_date,product_id) * 100,0) as ret_msg_rate,
  biz_date,
  product_id
from (
  select
    apply_id,
    resp_msg,
    biz_date,
    product_id
  from ods.credit_apply
  where 1 > 0
    and resp_code != 1
    and biz_date = '${ST9}'
    ${hive_param_str}
) as credit_apply
-- limit 10
;
