set mapred.job.name=dm_eagle.operation_return_ticket_detail;
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
ods.loan_apply) a
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
ods.customer_info) c
left join
(select  
   dim_encrypt
  ,dim_decrypt  
  from dim.dim_encrypt_info
  where dim_type = 'userName'
group by  
dim_encrypt
,dim_decrypt) d
on c.name = d.dim_encrypt
) b
on a.cust_id = b.cust_id
)

 
insert overwrite table dm_eagle.operation_return_ticket_detail partition(product_id)
select
 t1.channel_id                                 --合同渠道方
,t1.project_id                                 --项目名称
,t1.due_bill_no                                --借据编号
,null as contract_no                           --合同编号
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
(select
distinct
    due_bill_no
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
    loan.loan_init_principal
    FROM
    (select * from ods_cps.order_info where loan_usage = 'T') ord
    inner join
    (select distinct
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
    where project_id in ('WS0009200001','WS0006200001','WS0006200002','WS0006200003')
    ) biz
    on ord.product_id = biz.product_id
    left join
    (SELECT
        due_bill_no,
        loan_active_date,
        loan_init_principal
    FROM ods_cps.loan_info
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ) loan
    on ord.due_bill_no = loan.due_bill_no
union all
SELECT 
    ord.product_id,
    ord.due_bill_no,
    ord.txn_date,
    ord.success_amt,
    biz.channel_id,
    biz.project_id,
    loan.loan_active_date,
    loan.loan_init_principal
    FROM
    (select * from ods.order_info where loan_usage = 'T') ord
    inner join
    (select distinct
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
    where project_id in ('bd')
    ) biz
    on ord.product_id = biz.product_id
    left join
    (SELECT
        due_bill_no,
        loan_active_date,
        loan_init_principal
    FROM ods.loan_info
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ) loan
    on ord.due_bill_no = loan.due_bill_no
    ) a
) t1
left join
due_bill_no_name t2
on t1.due_bill_no = t2.due_bill_no
;    
