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
--set hivevar:ST9=2020-10-13;
--set hivevar:suffix=;
insert overwrite table dm_eagle${suffix}.operation_return_ticket_agg partition(product_id)
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
(
  SELECT
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
            select
                product_id,
                due_bill_no,
                txn_date,
                success_amt
            from ods_cps${suffix}.order_info
            where product_id <> '002501' and loan_usage = 'T'
        ) t1
        left join
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
    ) t3
    group by
    t3.product_id
    ,t3.channel_id
    ,t3.project_id
    ,t3.txn_date
) t4
) t5;