set mapred.job.name=dm_eagle.operation_product_management_report;
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
--set hivevar:ST9=2020-09-01;
--产品管理报表 【代偿后】
--create table if not exists dm_eagle.operation_product_management_report(
--      channel_id                      string              COMMENT '合作渠道方'
--     ,project_id                      string              COMMENT '项目名称'
--     ,loan_num_count                  decimal(10,0)       COMMENT '累计放款笔数'
--     ,loan_principal_count            decimal(15,4)       COMMENT '累计放款金额'
--     ,cumulative_repaid_amount        decimal(15,4)       COMMENT '累计已还金额'
--     ,cumulative_repaid_principal     decimal(15,4)       COMMENT '累计已还本金'
--     ,cumulative_repaid_interest      decimal(15,4)       COMMENT '累计已还利息'
--     ,cumulative_repaid_fee           decimal(15,4)       COMMENT '累计已还费用'
--     ,cumulative_repaid_penalty       decimal(15,4)       COMMENT '累计已还罚息'
--     ,assets_number_on_loan           decimal(10,0)       COMMENT '在贷资产笔数'
--     ,remain_amount_sum               decimal(15,4)       COMMENT '在贷余额'
--     ,remain_principal_sum            decimal(15,4)       COMMENT '在贷本金余额'
--     ,remain_interest_sum             decimal(15,4)       COMMENT '在贷利息余额'
--     ,overdue_assets_number           decimal(10,0)       COMMENT '逾期资产笔数'
--     ,current_overdue_amount          decimal(15,4)       COMMENT '当前逾期金额'
--     ,current_overdue_principal       decimal(15,4)       COMMENT '当前逾期本金'
--     ,current_overdue_interest        decimal(15,4)       COMMENT '当前逾期利息'
--     ,current_overdue_fee             decimal(15,4)       COMMENT '当前逾期费用'
--     ,current_overdue_penalty         decimal(15,4)       COMMENT '当前逾期罚息'
--) COMMENT '产品管理报表'
--PARTITIONED BY (`biz_date` string COMMENT '观察日期',`product_id` string COMMENT '产品编号')
--STORED AS PARQUET;
insert overwrite table dm_eagle${suffix}.operation_product_management_report_agg partition( biz_date,product_id)
select
      t2.channel_id                                                                                                      --'合作渠道方'
     ,t2.project_id                                                                                                      --'项目名称'
     ,count(t1.due_bill_no) as loan_num_count                                                                            --'累计放款笔数'
     ,sum(nvl(t1.loan_init_principal,0)) as loan_principal_count                                                         --'累计放款金额'
     ,sum(nvl(t1.paid_amount,0)) as cumulative_repaid_amount                                                             --'累计已还金额'
     ,sum(nvl(t1.paid_principal,0)) as cumulative_repaid_principal                                                       --'累计已还本金'
     ,sum(nvl(t1.paid_interest,0)) as cumulative_repaid_interest                                                         --'累计已还利息'
     ,sum(nvl(t1.paid_svc_fee,0) + nvl(t1.paid_term_fee,0)) as cumulative_repaid_fee                                     --'累计已还费用'
     ,sum(nvl(t1.paid_penalty,0)) as cumulative_repaid_penalty                                                           --'累计已还罚息'
     ,sum(if(t1.loan_status != 'F',1,0)) as assets_number_on_loan                                                        --'在贷资产笔数'
     ,sum(if(t1.loan_status != 'F',(nvl(t1.remain_principal,0) + nvl(t1.remain_interest,0)),0)) as remain_amount_sum     --'在贷余额'
     ,sum(if(t1.loan_status != 'F',nvl(t1.remain_principal,0),0)) as remain_principal_sum                                --'在贷本金余额'
     ,sum(if(t1.loan_status != 'F',nvl(t1.remain_interest,0),0)) as remain_interest_sum                                  --'在贷利息余额'
     ,sum(if(loan_status == 'O',1,0)) as overdue_assets_number                                                           --'逾期资产笔数'
     ,sum(if(loan_status == 'O',(nvl(t1.overdue_principal,0)
                                +nvl(t1.overdue_interest ,0)
                                +nvl(t1.overdue_svc_fee,0)
                                +nvl(t1.overdue_term_fee,0)
                                +nvl(t1.overdue_penalty,0)),0))
                                as current_overdue_amount                                                                --'当前逾期金额'
     ,sum(if(loan_status == 'O',nvl(t1.overdue_principal,0),0)) as current_overdue_principal                             --'当前逾期本金'
     ,sum(if(loan_status == 'O',nvl(t1.overdue_interest,0),0)) as current_overdue_interest                               --'当前逾期利息'
     ,sum(if(loan_status == 'O',(nvl(t1.overdue_svc_fee,0) + nvl(t1.overdue_term_fee,0)),0)) as current_overdue_fee      --'当前逾期费用'
     ,sum(if(loan_status == 'O',nvl(t1.overdue_penalty,0),0)) as current_overdue_penalty                                 --'当前逾期罚息'
     ,'${ST9}' as biz_date                                                                                               --'快照日'
     ,t2.product_id                                                                                                      --'产品编号'
from
(
    select * from ods_cps${suffix}.loan_info
    where '${ST9}' between s_d_date and date_sub(e_d_date, 1)
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
 t2.channel_id
,t2.project_id
,t2.product_id
;
