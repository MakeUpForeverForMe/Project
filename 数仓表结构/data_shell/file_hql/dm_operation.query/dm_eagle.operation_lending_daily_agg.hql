set mapred.job.name=dm_eagle.operation_lending_daily_agg;
set hive.execution.engine=mr;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=50000000;
set hive.map.aggr=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=1024000000;
set hive.merge.smallfiles.avgsize=1024000000;
set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
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
insert overwrite table dm_eagle.operation_lending_daily_agg partition(product_id)
SELECT
     channel_id                                                                                                                                       --'合作渠道方'
    ,project_id                                                                                                                                       --'项目名称'
    ,loan_active_date AS biz_date                                                                                                                     --'放款日期'
    ,loan_num                                                                                                                                         --'当期放款笔数'
    ,loan_principal                                                                                                                                   --'当日放款金额'
    ,sum( loan_num )      over( PARTITION BY product_id, project_id, channel_id ORDER BY loan_active_date ) AS loan_num_count                         --'累计放款笔数'
    ,sum( loan_principal) over( PARTITION BY product_id, project_id, channel_id ORDER BY loan_active_date ) AS loan_principal_count                   --'累计放款金额'
    ,current_date AS execution_date                                                                                                                   --'跑批日期'
    ,product_id                                                                                                                                       --'产品编号'
FROM
    (
SELECT
    t1.product_id,
    t2.project_id,
    t2.channel_id,
    t1.loan_active_date,
    count( t1.due_bill_no ) AS loan_num,
    sum( nvl ( t3.loan_init_principal, 0 ) ) AS loan_principal
FROM
    ods_new_s_cps.loan_lending t1
     JOIN (SELECT 
           DISTINCT 
           product_id, 
           channel_id, 
           project_id
           FROM 
           dim_new.biz_conf
           where 
           project_id in ('WS0009200001','WS0006200001','WS0006200002','WS0006200003')
    ) t2 
    ON t1.product_id = t2.product_id
    left join
    (select
        due_bill_no,
        loan_init_principal
     from ods_new_s_cps.loan_info where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ) t3
    on t1.due_bill_no = t3.due_bill_no
GROUP BY
    t1.product_id,
    t1.loan_active_date,
    t2.project_id,
    t2.channel_id 
    ) t3
union all
SELECT
     channel_id                                                                                                                                       --'合作渠道方'
    ,project_id                                                                                                                                       --'项目名称'
    ,loan_active_date AS biz_date                                                                                                                     --'放款日期'
    ,loan_num                                                                                                                                         --'当期放款笔数'
    ,loan_principal                                                                                                                                   --'当日放款金额'
    ,sum( loan_num )      over( PARTITION BY product_id, project_id, channel_id ORDER BY loan_active_date ) AS loan_num_count                         --'累计放款笔数'
    ,sum( loan_principal) over( PARTITION BY product_id, project_id, channel_id ORDER BY loan_active_date ) AS loan_principal_count                   --'累计放款金额'
    ,current_date AS execution_date                                                                                                                   --'跑批日期'
    ,product_id                                                                                                                                       --'产品编号'
FROM
    (
SELECT
    t1.product_id,
    t2.project_id,
    t2.channel_id,
    t1.loan_active_date,
    count( t1.due_bill_no ) AS loan_num,
    sum( nvl ( t3.loan_init_principal, 0 ) ) AS loan_principal
FROM
    ods_new_s.loan_lending t1
     JOIN (SELECT
           DISTINCT
           product_id,
           channel_id,
           project_id
           FROM
           dim_new.biz_conf
           where
           project_id in ('bd')
    ) t2
    ON t1.product_id = t2.product_id
    left join
    (select
        due_bill_no,
        loan_init_principal
     from ods_new_s.loan_info where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ) t3
    on t1.due_bill_no = t3.due_bill_no
GROUP BY
    t1.product_id,
    t1.loan_active_date,
    t2.project_id,
    t2.channel_id
    ) t3
;
