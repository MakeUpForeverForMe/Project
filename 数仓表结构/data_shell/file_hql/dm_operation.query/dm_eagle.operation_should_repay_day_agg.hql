set mapred.job.name=dm_eagle.operation_should_repay_day_agg;
set hive.execution.engine=mr;
set mapreduce.map.memory.mb=2048;
set mapreduce.reduce.memory.mb=2048;
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=12;
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
--set hivevar:ST9=2020-10-01;
--应还日报表
--create table if not exists dm_eagle.operation_should_repay_day_agg(
--   channel_id                       string             COMMENT '合同渠道方'
--  ,project_id                       string             COMMENT '项目名称'
--  ,should_repay_date                string             COMMENT '应还日期'
--  ,should_repay_amount              decimal(15,4)      COMMENT '应还金额'
--  ,should_repay_principal           decimal(15,4)      COMMENT '应还本金'
--  ,should_repay_interest            decimal(15,4)      COMMENT '应还利息'
--  ,should_repay_fee                 decimal(15,4)      COMMENT '应还费用'
--  ,should_repay_penalty             decimal(15,4)      COMMENT '应还罚息'
--  ,should_repay_amount_unpaid       decimal(15,4)      COMMENT '应还金额(未结清)'
--  ,should_repay_principal_unpaid    decimal(15,4)      COMMENT '应还本金(未结清)'
--  ,should_repay_interest_unpaid     decimal(15,4)      COMMENT '应还利息(未结清)'
--  ,should_repay_fee_unpaid          decimal(15,4)      COMMENT '应还费用(未结清)'
--  ,should_repay_penalty_unpaid      decimal(15,4)      COMMENT '应还罚息(未结清)'
--) COMMENT '应还日报表'
--PARTITIONED BY (`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
--STORED AS PARQUET;
insert overwrite table dm_eagle${suffix}.operation_should_repay_day_agg partition(biz_date,product_id)
select
 t2.channel_id                                                                                        ,--'合同渠道方'
 t2.project_id                                                                                        ,--'项目名称'
 t1.should_repay_date                                                                                 ,--'应还日期'
 sum(if('${ST9}'<=should_repay_date,t1.should_repay_amount - t1.paid_amount - t1.reduce_amount,0)) as should_repay_amount,--'应还金额'
 sum(if('${ST9}'<=should_repay_date,t1.should_repay_principal - t1.paid_principal -t1.reduce_principal,0)) as should_repay_principal,--'应还本金'
 sum(if('${ST9}'<=should_repay_date,t1.should_repay_interest - t1.paid_interest -t1.reduce_interest,0)) as should_repay_interest,--'应还利息'
 sum(if('${ST9}'<=should_repay_date,t1.should_repay_term_fee +  t1.should_repay_svc_fee - t1.paid_term_fee - t1.paid_svc_fee - t1.reduce_term_fee - t1.reduce_svc_fee,0)) as should_repay_fee,--'应还费用'
 sum(if('${ST9}'<=should_repay_date,t1.should_repay_penalty - t1.paid_penalty - t1.reduce_penalty,0)) as should_repay_penalty, --'应还罚息'
 sum(if('${ST9}'<=should_repay_date and is_settled != 'F',t1.should_repay_amount - t1.paid_amount - t1.reduce_amount,0)) as should_repay_amount_unpaid,--'应还金额(未结清)'
 sum(if('${ST9}'<=should_repay_date and is_settled != 'F',t1.should_repay_principal - t1.paid_principal -t1.reduce_principal,0)) as should_repay_principal_unpaid,--'应还本金(未结清)'
 sum(if('${ST9}'<=should_repay_date and is_settled != 'F',t1.should_repay_interest - t1.paid_interest - t1.reduce_interest,0)) as should_repay_interest_unpaid,--'应还利息(未结清)'
 sum(if('${ST9}'<=should_repay_date and is_settled != 'F',t1.should_repay_term_fee +  t1.should_repay_svc_fee - t1.paid_term_fee - t1.paid_svc_fee - t1.reduce_term_fee - t1.reduce_svc_fee,0)) as should_repay_fee_unpaid,--'应还费用(未结清)'
 sum(if('${ST9}'<=should_repay_date,t1.should_repay_penalty - t1.paid_penalty - t1.reduce_penalty,0)) as should_repay_penalty_unpaid, --'应还罚息(未结清)'
 '${ST9}' as biz_date                                                                                 ,--'观察日'
 t1.product_id                                                                                         --'产品编号'
from
(
    SELECT
        product_id,
        due_bill_no,
        should_repay_date,
        should_repay_amount,
        should_repay_principal,
        should_repay_interest,
        should_repay_term_fee,
        should_repay_svc_fee,
        should_repay_penalty,
        paid_amount,
        paid_principal,
        paid_interest,
        paid_term_fee,
        paid_svc_fee,
        paid_penalty,
        reduce_amount,
        reduce_principal,
        reduce_interest,
        reduce_term_fee,
        reduce_svc_fee,
        reduce_penalty,
        if(schedule_status = 'F','F','N') AS is_settled
    FROM
        ods_cps${suffix}.repay_schedule
    WHERE
        '${ST9}' BETWEEN s_d_date
        AND date_sub( e_d_date, 1 )
        and '${ST9}' <= should_repay_date
        and '${ST9}' >= loan_active_date
        and schedule_status <> 'F'
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
group by
 t1.product_id
,t2.channel_id
,t2.project_id
,t1.should_repay_date
;
