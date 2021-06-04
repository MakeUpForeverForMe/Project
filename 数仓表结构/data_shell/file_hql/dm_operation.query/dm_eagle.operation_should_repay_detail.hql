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

--set hivevar:ST9=2020-10-01;
--应还款明细报表 （取最新的数据）
--create table if not exists dm_eagle.operation_should_repay_detail(
-- channel_id                string            COMMENT          '合作渠道方'
--,project_id                string            COMMENT          '项目名称'
--,contract_no               string            COMMENT          '合同编号'
--,due_bill_no               string            COMMENT          '借据编号'
--,loan_active_date          string            COMMENT          '借据生效日'
--,customer_name             string            COMMENT          '客户姓名'
--,loan_init_term            decimal(3,0)      COMMENT          '总期数'
--,should_repay_date         string            COMMENT          '应还日期'
--,loan_term                 decimal(3,0)      COMMENT          '期次'
--,remain_amount             decimal(15,4)     COMMENT          '剩余金额'
--,remain_principal          decimal(15,4)     COMMENT          '剩余本金'
--,remain_interest           decimal(15,4)     COMMENT          '剩余利息'
--,remain_fee                decimal(15,4)     COMMENT          '剩余费用'
--,remain_penalty            decimal(15,4)     COMMENT          '剩余罚息'
--,should_repay_amount       decimal(15,4)     COMMENT          '应还金额'
--,should_repay_principal    decimal(15,4)     COMMENT          '应还本金'
--,should_repay_interest     decimal(15,4)     COMMENT          '应还利息'
--,should_repay_fee          decimal(15,4)     COMMENT          '应还费用'
--,should_repay_penalty      decimal(15,4)     COMMENT          '应还罚息'
--,paid_amount               decimal(15,4)     COMMENT          '已还金额'
--,paid_principal            decimal(15,4)     COMMENT          '已还本金'
--,paid_interest             decimal(15,4)     COMMENT          '已还利息'
--,paid_fee                  decimal(15,4)     COMMENT          '已还费用'
--,paid_penalty              decimal(15,4)     COMMENT          '已还罚息'
--,reduce_amount             decimal(15,4)     COMMENT          '减免金额'
--,reduce_principal          decimal(15,4)     COMMENT          '减免本金'
--,reduce_interest           decimal(15,4)     COMMENT          '减免利息'
--,reduce_fee                decimal(15,4)     COMMENT          '减免费用'
--,reduce_penalty            decimal(15,4)     COMMENT          '减免罚息'
--,execution_date            string            COMMENT          '跑批日期'
--) COMMENT '应还款明细报表'
--PARTITIONED BY (product_id string COMMENT '产品编号')
--STORED AS PARQUET;
--set hivevar:ST9=2020-10-13;
--set hivevar:suffix=;
with due_bill_no_name as
(
select
a.due_bill_no
,b.dim_decrypt
from(
select
distinct
due_bill_no
,cust_id
from
ods${suffix}.loan_apply) a
left join
(
select
 cust_id,
 name,
 dim_decrypt
from (
select
distinct
cust_id,
name
from
ods${suffix}.customer_info) c
left join dim.dim_encrypt_info d
on c.name = d.dim_encrypt
) b
on a.cust_id = b.cust_id
)


insert overwrite table dm_eagle${suffix}.operation_should_repay_detail partition(product_id)
SELECT
 distinct
t3.channel_id                                                                                       ,--'合作渠道方'
t3.project_id                                                                                       ,--'项目名称'
t2.contract_no                                                                                      ,--'合同编号'
t1.due_bill_no                                                                                      ,--'借据编号'
t1.loan_active_date                                                                                 ,--'借据生效日'
t4.dim_decrypt AS customer_name                                                                     ,--'客户姓名'
t1.loan_init_term                                                                                   ,--'总期数'
t1.should_repay_date                                                                                ,--'应还日期'
t1.loan_term                                                                                        ,--'期次'
t1.should_repay_amount - t1.paid_amount - t1.reduce_amount                      as remain_amount    ,--'剩余金额'
t1.should_repay_principal  - t1.paid_principal  - t1.reduce_principal           as remain_principal ,--'剩余本金'
t1.should_repay_interest - t1.paid_interest - t1.reduce_interest                as remain_interest  ,--'剩余利息'
t1.should_repay_term_fee + t1.should_repay_svc_fee - t1.paid_term_fee - t1.paid_svc_fee - t1.reduce_term_fee - t1.reduce_svc_fee as remain_fee, --'剩余费用'
t1.should_repay_penalty - t1.paid_penalty - t1.reduce_penalty                   as remain_penalty   ,--'剩余罚息'
t1.should_repay_amount                                                                              ,--'应还金额'
t1.should_repay_principal                                                                           ,--'应还本金'
t1.should_repay_interest                                                                            ,--'应还利息'
( nvl ( t1.should_repay_term_fee, 0 ) + nvl ( t1.should_repay_svc_fee, 0 ) )    as should_repay_fee ,--'应还费用'
t1.should_repay_penalty                                                                             ,--'应还罚息'
t1.paid_amount                                                                                      ,--'已还金额'
t1.paid_principal                                                                                   ,--'已还本金'
t1.paid_interest                                                                                    ,--'已还利息'
( nvl ( t1.paid_term_fee, 0 ) + nvl ( t1.paid_svc_fee, 0 ) )                    as paid_fee         ,--'已还费用'
t1.paid_penalty                                                                                     ,--'已还罚息'
t1.reduce_amount                                                                                    ,--'减免金额'
t1.reduce_principal                                                                                 ,--'减免本金'
t1.reduce_interest                                                                                  ,--'减免利息'
( nvl ( t1.reduce_term_fee, 0 ) + nvl ( t1.reduce_svc_fee, 0 ) )                as reduce_fee       ,--'减免费用'
t1.reduce_penalty                                                                                   ,--'减免罚息'
current_date() as execution_date                                                                    ,--'跑批日期'
t1.product_id                                                                                        --'产品名称'
FROM
(
    SELECT
    *
    FROM ods_cps${suffix}.repay_schedule
    WHERE '${ST9}' BETWEEN s_d_date AND date_sub(e_d_date, 1)
) t1
LEFT JOIN
(
    select
      due_bill_no,
      contract_no
    from
    ods_cps${suffix}.loan_lending
) t2
ON t1.due_bill_no = t2.due_bill_no
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
) t3
ON t1.product_id = t3.product_id
LEFT JOIN due_bill_no_name t4
ON t1.due_bill_no = t4.due_bill_no
;