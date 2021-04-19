set mapred.job.name=dm_eagle.operation_daily_repay_agg;
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
    where  project_id in ('WS0012200001','WS0009200001','WS0006200001','WS0006200002','WS0006200003','WS0013200001')
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



