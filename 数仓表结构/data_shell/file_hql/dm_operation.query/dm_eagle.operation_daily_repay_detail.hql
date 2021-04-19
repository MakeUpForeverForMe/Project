set mapred.job.name=dm_eagle.operation_daily_repay_detail;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set hive.execution.engine=mr;
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
--set hivevar:ST9=2021-01-05;
----日还款明细报表（取最新的数据）
--drop table if exists dm_eagle.operation_daily_repay_detail;
--create table if not exists dm_eagle.operation_daily_repay_detail(
--    channel_id              string             COMMENT '合同渠道方'
--   ,project_id              string             COMMENT '项目名称'
--   ,contract_no             string             COMMENT '合同编号'
--   ,due_bill_no             string             COMMENT '借据编号'
--   ,schedule_status         string             COMMENT '借据状态'
--   ,loan_active_date        string             COMMENT '借据生效日'
--   ,loan_expire_date        string             COMMENT '借据到期日'
--   ,customer_name           string             COMMENT '客户姓名'  ----取值逻辑待定
--   ,loan_init_principal     decimal(15,4)      COMMENT '借款金额'
--   ,loan_init_term          decimal(3,0)       COMMENT '总期数'
--   ,loan_term               decimal(3,0)       COMMENT '还款期次'
--   ,should_repay_date       string             COMMENT '应还日期'
--   ,biz_date                string             COMMENT '实还日期'
--   ,paid_out_date           string             COMMENT '核销日期'
--   ,fund_flow_date          string             COMMENT '资金流日期' ----取值逻辑待定
--   ,fund_flow_status        string             COMMENT '资金流状态' ----取值逻辑待定
--   ,overdue_days            decimal(5,0)       COMMENT '逾期天数'
--   ,repay_way               string             COMMENT '还款类型'
--   ,paid_amount             decimal(15,4)      COMMENT '还款金额'
--   ,paid_principal          decimal(15,4)      COMMENT '还款本金'
--   ,paid_interest           decimal(15,4)      COMMENT '还款利息'
--   ,paid_fee                decimal(15,4)      COMMENT '还款费用'
--   ,paid_penalty            decimal(15,4)      COMMENT '还款罚息'
--   ,execution_date          string             COMMENT '跑批日期'
--) COMMENT '日还款明细报表'
--PARTITIONED BY (product_id string COMMENT '产品编号')
--STORED AS PARQUET;
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
        ods${suffix}.loan_apply
    ) a
    left join
    (
        select cust_id,name,dim_decrypt
        from
        (
            select distinct cust_id,name from ods${suffix}.customer_info
        ) c
        left join
        (
            select dim_encrypt,dim_decrypt from dim_new.dim_encrypt_info where dim_type = 'userName'
            group by dim_encrypt,dim_decrypt
        ) d
        on c.name = d.dim_encrypt
    ) b
    on a.cust_id = b.cust_id
)

insert overwrite table dm_eagle${suffix}.operation_daily_repay_detail partition (product_id)
select
    biz.channel_id,
    biz.project_id,
    lending.contract_no,
    repay_detail.due_bill_no,
    loan.loan_status,
    loan.loan_active_date,
    lending.loan_expire_date,
    due_name.dim_decrypt                                                                    as customer_name,
    loan.loan_init_principal,
    loan.loan_init_term,
    repay_detail.repay_term                                                                 as loan_term,
    schedule.should_repay_date                                                              as should_repay_date,
    repay_detail.biz_date                                                                   as biz_date,
    schedule.paid_out_date                                                                  as paid_out_date,
    null                                                                                    as fund_flow_date,
    null                                                                                    as fund_flow_status,
    if(repay_detail.biz_date > schedule.should_repay_date and repay_detail.repay_term <> '0',datediff(repay_detail.biz_date,schedule.should_repay_date),0) as overdue_days,
    case
        when repay_detail.repay_type = 'BUYBACK' then '回购'
        when repay_detail.repay_type = 'DISCOUNT' then '商户贴息'
        when repay_detail.repay_type = 'RECOVERY_REPAYMENT' then '降额还本'
        when repay_detail.repay_type = 'INTEREST_REBATE' then '降额退息'
        when repay_detail.repay_type = 'INTERNAL_REFUND' then '退款退息'
        when repay_detail.repay_type = 'RECEIVABLE' then '退款回款'
        when repay_detail.repay_type = 'COMP' then '代偿'
        when repay_detail.repay_type = 'REFUND' then '退票'
        else '线上还款' end                                                                   as repay_way,
    sum(repay_detail.paid_principal + repay_detail.paid_interest + repay_detail.paid_fee + repay_detail.paid_penalty) as paid_amount,
    sum(repay_detail.paid_principal)                                                        as paid_principal,
    sum(repay_detail.paid_interest)                                                         as paid_interest,
    sum(repay_detail.paid_fee)                                                              as paid_fee,
    sum(repay_detail.paid_penalty)                                                          as paid_penalty,
    current_date()                                                                          as execution_date,
    repay_detail.product_id
from
(
    select
        *,
        if(bnp_type='Pricinpal',repay_amount,0)     as paid_principal,
        if(bnp_type='Interest',repay_amount,0)      as paid_interest,
        0                                           as paid_fee,
        if(bnp_type='Penalty',repay_amount,0)       as paid_penalty
    from ods_cps${suffix}.repay_detail where repay_type<>'REDUCE'
) repay_detail
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
on repay_detail.product_id = biz.product_id
left join ods_cps${suffix}.loan_lending lending
on repay_detail.due_bill_no = lending.due_bill_no
left join
(
    select
        due_bill_no,loan_status,loan_init_principal,loan_init_term,loan_active_date,overdue_days
    from ods_cps${suffix}.loan_info where '${ST9}' between s_d_date and date_sub(e_d_date,1)
) loan
on repay_detail.due_bill_no = loan.due_bill_no
left join due_bill_no_name due_name
on repay_detail.due_bill_no = due_name.due_bill_no
left join
(
    select
             due_bill_no,loan_term,should_repay_date,paid_amount,paid_principal,paid_interest,paid_term_fee,paid_svc_fee,paid_penalty,paid_out_date,schedule_status
    from ods_cps${suffix}.repay_schedule where '${ST9}' between s_d_date and date_sub(e_d_date,1)
) schedule
on repay_detail.due_bill_no = schedule.due_bill_no and repay_detail.repay_term = schedule.loan_term
group by
biz.channel_id,
biz.project_id,
lending.contract_no,
repay_detail.due_bill_no,
loan.loan_status,
loan.loan_active_date,
lending.loan_expire_date,
due_name.dim_decrypt,
loan.loan_init_principal,
loan.loan_init_term,
repay_detail.repay_term,
schedule.should_repay_date,
repay_detail.biz_date,
schedule.paid_out_date,
if(repay_detail.biz_date > schedule.should_repay_date and repay_detail.repay_term <> '0',datediff(repay_detail.biz_date,schedule.should_repay_date),0),
case
    when repay_detail.repay_type = 'BUYBACK' then '回购'
    when repay_detail.repay_type = 'DISCOUNT' then '商户贴息'
    when repay_detail.repay_type = 'RECOVERY_REPAYMENT' then '降额还本'
    when repay_detail.repay_type = 'INTEREST_REBATE' then '降额退息'
    when repay_detail.repay_type = 'INTERNAL_REFUND' then '退款退息'
    when repay_detail.repay_type = 'RECEIVABLE' then '退款回款'
    when repay_detail.repay_type = 'COMP' then '代偿'
    when repay_detail.repay_type = 'REFUND' then '退票'
    else '线上还款' end,
repay_detail.product_id;
