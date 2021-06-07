--放款统计 月表
--create table if not exists eagle.eagle_loan_base_stat_loan_num_m(
--     project_id                           string             comment '项目ID'
--    ,loan_terms                           decimal(4,0)       comment '贷款期数'
--    ,loan_principal                       decimal(15,4)      comment '当月贷款金额-从月初到月末' 
--) comment '放款统计-月表'
--partitioned by (biz_date string comment '观察日',product_id string comment '产品号')
--stored as parquet;
--set hivevar:ST9=2021-01-01;
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
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=512000000;
set mapred.min.split.size.per.rack=512000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.reducers.bytes.per.reducer=1024000000; --| |#每个节点的reduce 默认是处理1G大小的数据
set hive.exec.reducers.max=50;

set hivevar:product_id_list='001601','001602','001603','001802','002001','001906','002006','001901','001801','002002','001902','DIDI201908161538','002501','002601','002602';

insert overwrite table eagle.eagle_loan_base_stat_loan_num_m partition(biz_date,product_id)
select
    t2.project_id
    ,t1.loan_terms
    ,sum(t1.loan_principal) as loan_principal
    ,last_day('${ST9}')     as biz_date
    ,t1.product_id
from
    dw.dw_loan_base_stat_loan_num_day t1
join
     (
  select distinct
    project_id,
    product_id
  from (
    select
      max(if(col_name = 'project_id',   col_val,null)) as project_id,
      max(if(col_name = 'product_id',   col_val,null)) as product_id
    from dim.data_conf
    where col_type = 'ac'
    group by col_id
  ) as tmp
  where product_id is not null
) as t2
on t1.product_id = t2.product_id
where
    substr(biz_date,0,7) = substr('${ST9}',0,7)
    and t1.product_id in (${product_id_list})
group by 
    t1.product_id
    ,t2.project_id
    ,t1.loan_terms
;