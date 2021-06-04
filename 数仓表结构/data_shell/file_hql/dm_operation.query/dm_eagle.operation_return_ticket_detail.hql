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

--set ST9='2020-10-01';
--退车退票明细表
--create table if not exists dm_eagle.operation_return_ticket_detail(
--    channel_id                string             COMMENT '合同渠道方'
--   ,project_id                string             COMMENT '项目名称'
--   ,due_bill_no               string             COMMENT '借据编号'
--   ,contract_no               string             COMMENT '合同编号'
--   ,loan_active_date          string             COMMENT '借据生效日期'
--   ,debt_conversion_date      string             COMMENT '债转日期'
--   ,txn_date                  string             COMMENT '发生日期'
--   ,information_flow_date     string             COMMENT '信息流日期'
--   ,cash_flow_date            string             COMMENT '资金流日期'
--   ,cash_flow_status          string             COMMENT '资金流状态'
--   ,customer_name             string             COMMENT '客户姓名'
--   ,loan_init_principal       decimal(15,4)      COMMENT '放款金额'
--   ,success_amt               decimal(15,4)      COMMENT '退票/退车金额'
--   ,loan_usage                string             COMMENT '退票/退车状态'
--   ,execution_date            string             COMMENT '跑批日期'
--) COMMENT '退车退票明细表'
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


insert overwrite table dm_eagle${suffix}.operation_return_ticket_detail partition(product_id)
select
 t1.channel_id                                 --合同渠道方
,t1.project_id                                 --项目名称
,t1.due_bill_no                                --借据编号
,t1.contract_no                                --合同编号
,t1.loan_active_date                           --借据生效日期
,null as debt_conversion_date                  --债转日期
,t1.txn_date                                   --发生日期
,null as information_flow_date                 --信息流日期
,null as cash_flow_date                        --资金流日期
,null as cash_flow_status                      --资金流状态
,t2.dim_decrypt as customer_name               --客户姓名
,t1.loan_init_principal                        --放款金额
,t1.success_amt                                --退票/退车金额
,'T' AS loan_usage                             --退票/退车状态
,current_date() as execution_date              --跑批日期
,t1.product_id                                 --产品编号
FROM
(
  select
    distinct
    due_bill_no
    ,contract_no
    ,product_id
    ,txn_date
    ,success_amt
    ,channel_id
    ,project_id
    ,loan_active_date
    ,loan_init_principal
  from
  (
    SELECT
      ord.product_id,
      ord.due_bill_no,
      ord.txn_date,
      ord.success_amt,
      biz.channel_id,
      biz.project_id,
      loan.loan_active_date,
      loan.loan_init_principal,
      lending.contract_no
    FROM
    (select * from ods_cps${suffix}.order_info where loan_usage = 'T') ord
    inner join
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
    ) biz
    on ord.product_id = biz.product_id
    left join
    (
      SELECT
        due_bill_no,
        loan_active_date,
        loan_init_principal
      FROM ods_cps${suffix}.loan_info
      where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ) loan
    on ord.due_bill_no = loan.due_bill_no
    left join ods_cps${suffix}.loan_lending lending
    on ord.due_bill_no = lending.due_bill_no
    ) a
) t1
left join
due_bill_no_name t2
on t1.due_bill_no = t2.due_bill_no
;
