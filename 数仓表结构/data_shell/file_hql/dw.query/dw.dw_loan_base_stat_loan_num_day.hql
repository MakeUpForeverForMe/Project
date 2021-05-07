--set hivevar:db_suffix=;
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

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;


insert overwrite table dw${db_suffix}.dw_loan_base_stat_loan_num_day partition(biz_date = '${ST9}',product_id)
select
  loan_init_term                                                                       as loan_terms,
  sum(if(loan_active_date = '${ST9}',1,0))                                             as loan_num,
  count(due_bill_no)                                                                   as loan_num_count,
  sum(if(loan_active_date = '${ST9}',loan_init_principal,0))                           as loan_principal,
  sum(loan_init_principal)                                                             as loan_principal_count,
  product_id                                                                           as product_id
from
   ods${db_suffix}.repay_schedule 
   where 
   '${ST9}' between s_d_date and date_sub(e_d_date, 1)
   and loan_term = 1
   and loan_active_date <= '${ST9}'
   ${hive_param_str}
   group by 
   loan_init_term
   ,product_id
;

