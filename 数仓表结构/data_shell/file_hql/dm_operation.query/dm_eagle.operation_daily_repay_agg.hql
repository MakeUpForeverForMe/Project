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
--还款日报
--create table if not exists dm_eagle.operation_daily_repay_agg(
--   channel_id                       string             COMMENT '合同渠道方'
--  ,project_id                       string             COMMENT '项目名称'
--  ,biz_date                         string             COMMENT '实还日期'
--  ,paid_out_date                    string             COMMENT '核销日期'
--  ,repay_way                        string             COMMENT '还款类型'
--  ,repay_num                        decimal(10,0)      COMMENT '还款笔数'
--  ,paid_amount                      decimal(15,4)      COMMENT '还款金额'
--  ,paid_principal                   decimal(15,4)      COMMENT '还款本金'
--  ,paid_interest                    decimal(15,4)      COMMENT '还款利息'
--  ,paid_fee                         decimal(15,4)      COMMENT '还款费用'
--  ,paid_penalty                     decimal(15,4)      COMMENT '还款罚息'
--  ,execution_date                   string             COMMENT '跑批日期'
--) COMMENT '还款日报'
--PARTITIONED BY (`product_id` string COMMENT '产品编号')
--STORED AS PARQUET;
insert overwrite table dm_eagle${suffix}.operation_daily_repay_agg partition(product_id)
select
 d.channel_id                                                            --'合同渠道方'
,d.project_id                                                            --'项目名称'
,a.biz_date                                                              --'实还日期'
,c.paid_out_date                                                         --'核销日期'
, case when a.repay_type = 'DISCOUNT' then '商户贴息'
       when a.repay_type = 'INTEREST_REBATE' then '降额退息'
       when a.repay_type = 'INTERNAL_REFUND' then '退款退息'
       when a.repay_type = 'RECOVERY_REPAYMENT' then '降额还本'
       when a.repay_type = 'RECEIVABLE' then '退款回款'
       when a.repay_type = 'BUYBACK' then '回购'
       when a.repay_type = 'COMP' then '代偿'
       when a.repay_type = 'REFUND' then '退票'
       else '线上还款' end as repay_way                                   --'还款类型'
,count(distinct a.order_id)      as repay_num                            --'还款笔数'
,sum(a.paid_amount)        as paid_amount                                --'还款金额'
,sum(a.paid_principal)     as paid_principal                             --'还款本金'
,sum(a.paid_interest)      as paid_interest                              --'还款利息'
,0 as paid_fee                                                           --'还款费用'
,sum(a.paid_penalty)       as paid_penalty                               --'还款罚息'
,current_date() as execution_date                                        --'跑批日期'
,a.product_id                                                            --'产品编号'
from
(
select
due_bill_no
,biz_date
,repay_term
,product_id
,order_id
,sum(nvl(repay_amount,0)) as paid_amount
,sum(if(bnp_type_cn = '本金', nvl(repay_amount,0),0)) as paid_principal
,sum(if(bnp_type_cn = '利息', nvl(repay_amount,0),0)) as paid_interest
,0 as paid_fee
,sum(if(bnp_type_cn = '罚息', nvl(repay_amount,0),0)) as paid_penalty,
repay_type
from ods_cps${suffix}.repay_detail
where biz_date <= '${ST9}' and repay_type<>'REDUCE'
-- where repay_term<>'999'
group by
due_bill_no,biz_date,repay_term,product_id,order_id,repay_type
) a
left join
(
SELECT
distinct
due_bill_no,
loan_term,
paid_out_date
FROM ods_cps${suffix}.repay_schedule
WHERE
'${ST9}' BETWEEN s_d_date AND date_sub( e_d_date, 1 )
and paid_out_date is not null
) c
on a.due_bill_no = c.due_bill_no
and a.repay_term = c.loan_term
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
    where  project_id in ('WS0012200001')
) d
on a.product_id = d.product_id
group by
a.product_id
,d.channel_id
,d.project_id
,a.biz_date
,c.paid_out_date
,case when a.repay_type = 'DISCOUNT' then '商户贴息'
       when a.repay_type = 'INTEREST_REBATE' then '降额退息'
       when a.repay_type = 'INTERNAL_REFUND' then '退款退息'
       when a.repay_type = 'RECOVERY_REPAYMENT' then '降额还本'
       when a.repay_type = 'RECEIVABLE' then '退款回款'
       when a.repay_type = 'BUYBACK' then '回购'
       when a.repay_type = 'COMP' then '代偿'
       when a.repay_type = 'REFUND' then '退票'
       else '线上还款' end
;


----乐信项目
insert overwrite table dm_eagle.operation_daily_repay_agg partition(product_id)
select
 d.channel_id                                                            --'合同渠道方'
,d.project_id                                                            --'项目名称'
,a.biz_date                                                              --'实还日期'
,c.paid_out_date                                                         --'核销日期'
, case  when b.purpose = '回购' then '回购' 
       when b.purpose = '代偿' then '代偿'
       when b.purpose = '退车' or b.purpose = '三方支付退票' then '退票/退车'
       else '线上还款' end as repay_way                                  --'还款类型'      
,count(distinct a.order_id)      as repay_num                            --'还款笔数'
,sum(a.paid_amount)        as paid_amount                                --'还款金额'
,sum(a.paid_principal)     as paid_principal                             --'还款本金'
,sum(a.paid_interest)      as paid_interest                              --'还款利息'
,0 as paid_fee                                                           --'还款费用'
,sum(a.paid_penalty)       as paid_penalty                               --'还款罚息'
,current_date() as execution_date                                        --'跑批日期'
,a.product_id                                                            --'产品编号'
from
(
select 
due_bill_no
,biz_date
,repay_term
,product_id
,order_id
,sum(nvl(repay_amount,0)) as paid_amount
,sum(if(bnp_type_cn = '本金', nvl(repay_amount,0),0)) as paid_principal
,sum(if(bnp_type_cn = '利息', nvl(repay_amount,0),0)) as paid_interest
,0 as paid_fee
,sum(if(bnp_type_cn = '罚息', nvl(repay_amount,0),0)) as paid_penalty
from ods_cps.repay_detail 
group by 
due_bill_no
,biz_date
,repay_term
,product_id
,order_id
) a
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
 and purpose != '放款'
) b
on a.order_id = b.order_id
left join
(
SELECT
distinct
due_bill_no,
loan_term,
paid_out_date  
FROM ods_cps.repay_schedule 
WHERE 
'${ST9}' BETWEEN s_d_date AND date_sub( e_d_date, 1 )
and paid_out_date is not null
) c
on a.due_bill_no = c.due_bill_no
and a.repay_term = c.loan_term
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
    ) d
on a.product_id = d.product_id
group by
a.product_id
,d.channel_id
,d.project_id
,a.biz_date
,c.paid_out_date
,case  when b.purpose = '回购' then '回购' 
       when b.purpose = '代偿' then '代偿'
       when b.purpose = '退车' or b.purpose = '三方支付退票' then '退票/退车'
       else '线上还款' end
;



