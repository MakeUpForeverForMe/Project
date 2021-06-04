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
--百度项目
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
    where  project_id in ('WS0012200001')
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


--乐信项目

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
,
duebillno_repayterm_repayway as
(
select
t1.due_bill_no
,t1.repay_term as term
,t1.biz_date
,case when t2.purpose = '回购' then '回购' 
      when t2.purpose = '代偿' then '代偿'
      when t2.purpose = '退车' or t2.purpose = '三方支付退票' then '退票/退车'
      else '线上还款' end as repay_way
from
(select
distinct
 due_bill_no
,repay_term
,order_id
,biz_date
from 
ods_cps.repay_detail a
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
    where  project_id in ('WS0009200001','WS0006200001','WS0006200002','WS0006200003','WS0013200001')
    ) b
on a.product_id = b.product_id
--where due_bill_no != '1120123112250874213037'
) t1
left join
(
 select
 distinct
 order_id,
 repay_way,
 purpose
 from
 ods_cps.order_info
 where 
 order_id is not null
 and purpose != '放款申请'
 and purpose != '放款') t2
on t1.order_id = t2.order_id
)

insert overwrite table dm_eagle.operation_daily_repay_detail_temp 
SELECT
    t4.channel_id,                                                                                                           --'合同渠道方'
    t4.project_id,                                                                                                           --'项目名称'
    t2.contract_no,                                                                                                          --'合同编号'
    t1.due_bill_no,                                                                                                          --'借据编号'
    t7.loan_status,                                                                                                          --'借据状态'
    t1.loan_active_date,                                                                                                     --'借据生效日'
    t2.loan_expire_date,                                                                                                     --'借据到期日'
    t6.dim_decrypt AS customer_name,                                                                                         --'客户姓名'  
    t2.loan_init_principal,                                                                                                  --'借款金额'
    t1.loan_init_term,                                                                                                       --'总期数'
    t1.loan_term,                                                                                                            --'还款期次'
    t1.should_repay_date,                                                                                                    --'应还日期'
    if(t1.due_bill_no = '1120123112250874213037' and t1.loan_term > 1,'2021-01-04',t3.biz_date) as biz_date,                 --'实还日期'
    t1.paid_out_date,                                                                                                        --'核销日期'
    NULL AS fund_flow_date,                                                                                                  --'资金流日期' 
    NULL AS fund_flow_status,                                                                                                --'资金流状态'
    IF( paid_out_date > should_repay_date, datediff( paid_out_date, should_repay_date ), 0 ) AS overdue_days,                --'逾期天数'
    
    case when t1.due_bill_no = '1120123112250874213037' and t3.biz_date = '2021-01-01'  then '线上还款'
         when t1.due_bill_no = '1120123112250874213037' and t3.biz_date is null  then '退票/退车'
         else t3.repay_way end  as repay_way,                                                                                --'还款类型'
         
    case when t3.biz_date = '2020-06-20'  and t1.due_bill_no = '1120061910384241252747' then 200.4
         when t3.biz_date = '2020-06-23'  and t1.due_bill_no = '1120061910384241252747' then 200
         when t3.biz_date = '2021-04-08'  and t1.due_bill_no = '1121040515414544993037' then 99.4
         when t3.biz_date = '2021-04-12'  and t1.due_bill_no = '1121040515414544993037' then 99
         when t3.biz_date = '2021-01-01'  and t1.due_bill_no = '1120123112250874213037' then 86.96
         when t3.biz_date = '2021-01-04'  and t1.due_bill_no = '1120123112250874213037' then 79.25
         else t1.paid_amount end as paid_amount,                                                                             --'还款金额'

    case when t3.biz_date = '2020-06-20' and t1.due_bill_no = '1120061910384241252747' then 200
         when t3.biz_date = '2020-06-23' and t1.due_bill_no = '1120061910384241252747' then 200
         when t3.biz_date = '2021-04-08' and t1.due_bill_no = '1121040515414544993037' then 99
         when t3.biz_date = '2021-04-12' and t1.due_bill_no = '1121040515414544993037' then 99
         when t3.biz_date = '2021-01-01' and t1.due_bill_no = '1120123112250874213037' then 79.25
         else t1.paid_principal end as paid_principal,                                                                       --'还款本金'
    case when t3.biz_date = '2020-06-20' and t1.due_bill_no = '1120061910384241252747' then 0.4
         when t3.biz_date = '2020-06-23' and t1.due_bill_no = '1120061910384241252747' then 0
         when t3.biz_date = '2021-04-08' and t1.due_bill_no = '1121040515414544993037' then 0.4
         when t3.biz_date = '2021-04-12' and t1.due_bill_no = '1121040515414544993037' then 0
         when t3.biz_date = '2021-01-01' and t1.due_bill_no = '1120123112250874213037' then 7.71
         when t3.biz_date = '2021-01-04' and t1.due_bill_no = '1120123112250874213037' then 0
         else t1.paid_interest end as paid_interest,                                                                         --'还款利息'
    ( nvl ( t1.paid_term_fee, 0 ) + nvl ( t1.paid_svc_fee, 0 ) ) AS paid_fee,                                                --'还款费用'
    t1.paid_penalty,                                                                                                         --'还款罚息'
    current_date() AS execution_date,                                                                                        --'跑批日期'
    t1.product_id                                                                                                            --'产品编号'
FROM
    ( SELECT 
    distinct
     due_bill_no
    ,loan_init_term
    ,loan_active_date
    ,loan_term
    ,should_repay_date
    ,paid_amount
    ,paid_principal
    ,paid_interest
    ,paid_term_fee
    ,paid_svc_fee
    ,paid_penalty
    ,paid_out_date
    ,product_id
    FROM 
    ods_cps.repay_schedule 
    WHERE 
    '${ST9}' BETWEEN s_d_date AND date_sub( e_d_date, 1 ) 
    AND schedule_status_cn = '已还清' 
    ) t1
    LEFT JOIN 
    (select 
    due_bill_no, 
    contract_no,
    loan_expire_date,
    loan_original_principal as loan_init_principal
    --loan_init_term
    from ods_cps.loan_lending
    ) t2 
    ON t1.due_bill_no = t2.due_bill_no
    LEFT JOIN 
    duebillno_repayterm_repayway t3 
    ON t1.due_bill_no = t3.due_bill_no 
    AND t1.loan_term = t3.term
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
    where  project_id in ('WS0009200001','WS0006200001','WS0006200002','WS0006200003','WS0013200001')  
    ) t4 
    ON t1.product_id = t4.product_id
    LEFT JOIN due_bill_no_name t6
    ON t1.due_bill_no = t6.due_bill_no
    LEFT JOIN
    (SELECT
    due_bill_no,    
    loan_status 
    FROM 
    ods_cps.loan_info
    where 
    '${ST9}' BETWEEN s_d_date AND date_sub( e_d_date, 1 ) 
    ) t7
    on t1.due_bill_no = t7.due_bill_no;

insert overwrite table dm_eagle.operation_daily_repay_detail partition(product_id)
select
    channel_id,                                                                                                 --'合同渠道方'
    project_id,                                                                                                 --'项目名称'
    contract_no,                                                                                                --'合同编号'
    t1.due_bill_no,                                                                                             --'借据编号'
    schedule_status,                                                                                            --'借据状态'
    loan_active_date,                                                                                           --'借据生效日'
    loan_expire_date,                                                                                           --'借据到期日'
    customer_name,                                                                                              --'客户姓名'  
    loan_init_principal,                                                                                        --'借款金额'
    loan_init_term,                                                                                             --'总期数'
    loan_term,                                                                                                  --'还款期次'
    should_repay_date,                                                                                          --'应还日期'
    if(t1.biz_date is null, t1.paid_out_date, t1.biz_date) as biz_date,                                         --'实还日期'
    paid_out_date,                                                                                              --'核销日期'
    fund_flow_date,                                                                                             --'资金流日期' 
    fund_flow_status,                                                                                           --'资金流状态' 
    overdue_days,                                                                                               --'逾期天数'
    if(t1.repay_way is  null, t2.repay_way, t1.repay_way) as repay_way,                                         --'还款类型'
    paid_amount,                                                                                                --'还款金额'
    paid_principal,                                                                                             --'还款本金'
    paid_interest,                                                                                              --'还款利息'
    paid_fee,                                                                                                   --'还款费用'
    paid_penalty,                                                                                               --'还款罚息'
    execution_date,                                                                                             --'跑批日期'
    product_id   
from
dm_eagle.operation_daily_repay_detail_temp t1
left join
(select
due_bill_no
,repay_way
from
(select
due_bill_no
,repay_way
,row_number() over(partition by due_bill_no order by loan_term desc) rn
from
dm_eagle.operation_daily_repay_detail_temp b
where b.biz_date is not null
and b.repay_way is not null)
c where c.rn = 1
) t2
on 
t1.due_bill_no = t2.due_bill_no
;
