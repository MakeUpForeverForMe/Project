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
--create table if not exists dm_eagle.operation_loan_ledger_detail(                                       
--    channel_id                 string               COMMENT '渠道编号'                     
--   ,project_id                 string               COMMENT '项目编号'                     
--   ,contract_no                string               COMMENT '合同编号'                     
--   ,due_bill_no                string               COMMENT '借据编号'                     
--   ,customer_name              string               COMMENT '客户姓名'       
--   ,loan_init_principal        decimal(15,4)        COMMENT '合同金额'                     
--   ,loan_init_contract_amount  decimal(15,4)        COMMENT '初始合同金额'
--   ,loan_active_date           string               COMMENT '借款生效日'                   
--   ,debt_conversion_date       string               COMMENT '债转日期'  --取值逻辑待定     
--   ,cycle_day                  decimal(2,0)         COMMENT '还款日'                       
--   ,loan_type                  string               COMMENT '还款方式'                     
--   ,loan_init_interest_rate    decimal(15,8)        COMMENT '利率'                         
--   ,loan_init_penalty_rate     decimal(15,8)        COMMENT '罚息率'                       
--   ,loan_init_fee_rate         decimal(15,8)        COMMENT '费率'   
--   ,loan_expire_date           string               COMMENT '借款到期日'                   
--   ,loan_status                string               COMMENT '借据状态'                     
--   ,paid_out_date              string               COMMENT '借款结清日'                   
--   ,loan_init_term             decimal(3,0)         COMMENT '借款期数'                     
--   ,loan_term_remain           decimal(3,0)         COMMENT '剩余期数'                     
--   ,overdue_days               decimal(5,0)         COMMENT '逾期天数'                     
--   ,remain_amount              decimal(15,4)        COMMENT '剩余未还金额'                 
--   ,remain_principal           decimal(15,4)        COMMENT '剩余未还本金'                 
--   ,remain_interest            decimal(15,4)        COMMENT '剩余未还利息'                 
--   ,remain_fee                 decimal(15,4)        COMMENT '剩余未还费用'  --取值逻辑待定 
--   ,remain_penalty_interest    decimal(15,4)        COMMENT '剩余未还罚息'  --取值逻辑待定 
--   ,overdue_amount             decimal(15,4)        COMMENT '逾期金额'                     
--   ,overdue_principal          decimal(15,4)        COMMENT '逾期本金'                     
--   ,overdue_interest           decimal(15,4)        COMMENT '逾期利息'                     
--   ,overdue_fee                decimal(15,4)        COMMENT '逾期费用'                     
--   ,overdue_penalty            decimal(15,4)        COMMENT '逾期罚息'                     
--   ,paid_amount                decimal(15,4)        COMMENT '已还金额'                     
--   ,paid_principal             decimal(15,4)        COMMENT '已还本金'                     
--   ,paid_interest              decimal(15,4)        COMMENT '已还利息'                     
--   ,paid_fee                   decimal(15,4)        COMMENT '已还费用'                     
--   ,paid_penalty               decimal(15,4)        COMMENT '已还罚息'                     
--   ,execution_date             string               COMMENT '跑批日期'                     
--) COMMENT '借据台账-明细'                                                                  
--PARTITIONED BY (product_id string COMMENT '产品编号')                                      
--STORED AS PARQUET;                                                                         
--due_bill_no和name对应的临时表
--set hivevar:ST9=2020-10-13;
--set hivevar:suffix=;
with due_bill_no_name as
(
    select a.due_bill_no ,b.dim_decrypt
    from
    (
        select
        distinct
        due_bill_no
        ,cust_id
        from
        ods.loan_apply
    ) a
    left join
    (
        select cust_id,name,dim_decrypt
        from
        (
            select distinct cust_id,name from ods.customer_info
        ) c
        left join dim.dim_encrypt_info d
        on c.name = d.dim_encrypt
    ) b
    on a.cust_id = b.cust_id
)
--set ST9='2020-10-01';
--借据台账-明细 （取最新的数据）
--insert overwrite table dm_eagle${suffix}.operation_loan_ledger_detail partition(product_id)
select
 t2.channel_id                                                                  --'渠道编号'
,t2.project_id                                                                  --'项目编号'
,t3.contract_no                                                                 --'合同编号'
,t1.due_bill_no                                                                 --'借据编号'
,t4.dim_decrypt as customer_name                                                --'客户姓名'
,t1.loan_init_principal                                                         --'合同金额'
,t3.loan_original_principal                     as loan_init_contract_amount    --'初始合同金额'
,t3.loan_active_date                                                            --'借款生效日'
,null as debt_conversion_date                                                   --'债转日期'  --取值逻辑待定
,t3.cycle_day                                                                   --'还款日'
,t3.loan_type                                                                   --'还款方式'
,t3.loan_init_interest_rate                                                     --'利率'
,t3.loan_init_penalty_rate                                                      --'罚息率'
,t3.loan_init_interest_rate   as loan_init_fee_rate                             --'费率'
,t3.loan_expire_date                                                            --'借款到期日'
,t1.loan_status                                                                 --'借据状态'
,t1.paid_out_date                                                               --'借款结清日'
,t1.loan_init_term                                                              --'借款期数'
,t1.loan_term_remain                                                            --'剩余期数'
,t1.overdue_days                                                                --'逾期天数'
,t1.remain_amount                                                               --'剩余未还金额'
,t1.remain_principal                                                            --'剩余未还本金'
,t1.remain_interest                                                             --'剩余未还利息'
,(nvl(t1.remain_svc_fee,0) + nvl(t1.remain_term_fee,0)) as remain_fee           --'剩余未还费用'  --取值逻辑待定
,(nvl(t1.remain_amount,0) - nvl(t1.remain_principal,0)-nvl(t1.remain_interest,0) - nvl(t1.remain_svc_fee,0) - nvl(t1.remain_term_fee,0)) as remain_penalty_interest --'剩余未还罚息'  --取值逻辑待定
,(nvl(t1.overdue_principal,0) + nvl(t1.overdue_interest,0) + nvl(t1.overdue_svc_fee,0) + nvl(t1.overdue_term_fee,0) + nvl(t1.overdue_penalty,0)) as overdue_amount  --'逾期金额'
,t1.overdue_principal                                                           --'逾期本金'
,t1.overdue_interest                                                            --'逾期利息'
,(nvl(t1.overdue_svc_fee,0)+nvl(t1.overdue_term_fee,0)) as overdue_fee          --'逾期费用'
,t1.overdue_penalty                                                             --'逾期罚息'
,t1.paid_amount                                                                 --'已还金额'
,t1.paid_principal                                                              --'已还本金'
,t1.paid_interest                                                               --'已还利息'
,(nvl(t1.paid_svc_fee,0)+nvl(t1.paid_term_fee,0)) as paid_fee                   --'已还费用'
,t1.paid_penalty                                                                --'已还罚息'
,current_date() as execution_date                                               --'跑批日期'
,t1.product_id                                                                  --'产品编号'
from
(select
*
from
ods_cps${suffix}.loan_info
where '${ST9}' between s_d_date and date_sub(e_d_date,1)
) t1
join
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
on t1.product_id = t2.product_id
left join
(
select
     due_bill_no
    ,contract_no
    ,loan_active_date
    ,cycle_day
    ,loan_type
    ,loan_init_interest_rate
    ,loan_init_penalty_rate
    ,loan_expire_date
    ,loan_original_principal
from ods_cps${suffix}.loan_lending
) t3
on t1.due_bill_no = t3.due_bill_no
left join due_bill_no_name t4
on t1.due_bill_no = t4.due_bill_no
;
