set mapred.job.name=dm_eagle.operation_return_ticket_agg;
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
--set hivevar:ST9=2020-10-13;
--退票报表
--create table if not exists dm_eagle.operation_return_ticket_agg(
--   product_id                  string              COMMENT '产品编号'
--  ,channel_id                  string              COMMENT '合同渠道方'
--  ,project_id                  string              COMMENT '项目名称'
--  ,txn_date                    string              COMMENT '发生日期'
--  ,refund_amount               decimal(15,4)       COMMENT '退票/退车金额'
--  ,refund_num                  decimal(10,0)       COMMENT '退票/退车笔数'
--  ,refund_amount_accumul       decimal(15,4)       COMMENT '累计退票/退车金额'
--  ,refund_num_accumul          decimal(10,0)       COMMENT '累计退票/退车笔数'
--) COMMENT '退票报表'
--PARTITIONED BY (`product_id` string COMMENT '产品编号')
--STORED AS PARQUET;
insert overwrite table dm_eagle.operation_return_ticket_agg partition(product_id)
select
     channel_id                                                                                                        --'合同渠道方 '
    ,project_id                                                                                                        --'项目名称'
    ,txn_date                                                                                                          --'发生日期'
    ,refund_amount                                                                                                     --'退票 /退车金额 '
    ,refund_num                                                                                                        --'退票 /退车笔数 '
    ,refund_amount_accumul                                                                                             --'累计退票 /退车金额 '
    ,refund_num_accumul                                                                                                --'累计退票 /退车笔数 '
    ,current_date() as execution_date                                                                                  --'跑批日期'
    ,product_id                                                                                                        --'产品编号'
from                            
(SELECT
     channel_id                                                                                                        
    ,project_id                                                                                                        
    ,txn_date                                                                                                          
    ,refund_amount                                                                                                     
    ,refund_num                                                                                                        
    ,sum(refund_amount) over(partition by project_id, channel_id,product_id order by txn_date)as refund_amount_accumul 
    ,sum(refund_num) over(partition by project_id, channel_id,product_id order by txn_date)   as refund_num_accumul                                                                               
    ,product_id                                                                                                        
from
(
select
    t3.product_id
    ,t3.channel_id
    ,t3.project_id
    ,t3.txn_date
    ,count(t3.due_bill_no) as  refund_num
    ,sum(t3.success_amt)   as  refund_amount
from
    (
        select
            t1.due_bill_no
            ,t1.product_id
            ,t1.txn_date
            ,t1.success_amt
            ,t2.channel_id
            ,t2.project_id
        from
        (
                SELECT
                    product_id,
                    due_bill_no,
                    txn_date,
                    success_amt
                    FROM ods_cps.order_info
                WHERE
                    loan_usage = 'T'
                union all
                SELECT
                    product_id,
                    due_bill_no,
                    txn_date,
                    success_amt
                    FROM ods.order_info
                WHERE
                    loan_usage = 'T' and product_id in ('bd_product')
        ) t1
        left join
        (
                select distinct
                 channel_id,
                 project_id,
                 product_id
                 from (
                   select
                     max(if(col_name = 'channel_id',   col_val,null)) as channel_id,
                     max(if(col_name = 'project_id',   col_val,null)) as project_id,
                     max(if(col_name = 'product_id',   col_val,null)) as product_id
                   from dim.data_conf
                   where col_type = 'ac'
                   group by col_id
               )tmp
        ) t2
        ON t1.product_id = t2.product_id
    ) t3
    group by
    t3.product_id
    ,t3.channel_id
    ,t3.project_id
    ,t3.txn_date
) t4
) t5;