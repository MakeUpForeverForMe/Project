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
-- 关闭自动 MapJoin
set hive.auto.convert.join=false;
--set hivevar:ST9=2020-10-13;
--set hivevar:suffix=;
--set hivevar:ST9='2020-10-03';
--放款日报
--create table if not exists dm_eagle.operation_lending_daily_agg (
--   channel_id              string                 COMMENT '合作渠道方'
--  ,project_id              string                 COMMENT '项目名称'
--  ,biz_date                string                 COMMENT '放款日期'
--  ,loan_num                decimal(15,4)          COMMENT '当期放款笔数'
--  ,loan_principal          decimal(15,4)          COMMENT '当日放款金额'
--  ,loan_num_count          decimal(10,0)          COMMENT '累计放款笔数'
--  ,loan_principal_count    decimal(15,4)          COMMENT '累计放款金额'
--  ,execution_date          string                 COMMENT '跑批日期'
--) COMMENT '放款日报'
--PARTITIONED BY (`product_id` string COMMENT '产品编号')
--STORED AS PARQUET;
insert overwrite table dm_eagle${suffix}.operation_lending_daily_agg partition(product_id)
SELECT
     channel_id                             --'合作渠道方'
    ,project_id                             --'项目名称'
    ,loan_active_date as biz_date           --'放款日期'
    ,loan_num                               --'当期放款笔数'
    ,loan_principal --'当日放款金额'
    ,sum( loan_num )      over( PARTITION BY product_id, project_id, channel_id ORDER BY loan_active_date ) as loan_num_count                          --'累计放款笔数'
    ,sum( loan_principal) over( PARTITION BY product_id, project_id, channel_id ORDER BY loan_active_date ) as loan_principal_count                    --'累计放款金额'
    ,current_date as execution_date                                                                                                                                         --'跑批日期'
    ,product_id                                                                                                                                                             --'产品编号'
FROM
(
SELECT
    t1.product_id,
    t2.project_id,
    t2.channel_id,
    t1.loan_active_date,
    count( t1.due_bill_no ) as loan_num,
    sum( nvl ( t1.loan_original_principal, 0 ) ) as loan_principal
FROM
    ods_cps${suffix}.loan_lending t1
     JOIN
     (
        select
          distinct
          channel_id,
          project_id,
          product_id
          from
          (
            select
               max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
               max(if(col_name = 'project_id',   col_val,null)) as project_id,
               max(if(col_name = 'product_id',   col_val,null)) as product_id
             from dim.data_conf
             where col_type = 'ac'
             group by col_id
          ) tmp
          where  project_id in ('WS0012200001','WS0009200001','WS0006200001','WS0006200002','WS0006200003','WS0013200001')
    ) t2
    ON t1.product_id = t2.product_id
GROUP BY
    t1.product_id,
    t1.loan_active_date,
    t2.project_id,
    t2.channel_id
) t3
;
