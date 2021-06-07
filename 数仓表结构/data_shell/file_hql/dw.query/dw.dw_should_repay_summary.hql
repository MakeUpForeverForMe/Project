--create table if not exists dw_cps.dw_should_repay_summary(
-- should_repay_date                   string                 comment '计划应还日'
--,loan_init_term                      int                    comment '贷款期数'
--,loan_term                           int                    comment '期次'
--,loan_num                            decimal(15,0)          comment '借据笔数'
--,loan_amount                         decimal(15,4)          comment '放款金额-最多维度下可用'
--,should_repay_amount                 decimal(15,4)          comment '应还金额'
--,should_repay_principal              decimal(15,4)          comment '应还本金'
--,should_repay_interest               decimal(15,4)          comment '应还利息'
--,should_repay_term_fee               decimal(15,4)          comment '应还手续费'
--,should_repay_svc_fee                decimal(15,4)          comment '应还服务费'
--,should_repay_penalty                decimal(15,4)          comment '应还罚息'
--,should_repay_mult_amt               decimal(15,4)          comment '应还滞纳金'
--,should_repay_penalty_acru           decimal(15,4)          comment '应还累计罚息金额'
--,paid_amount                         decimal(15,4)          comment '已还金额'
--,paid_principal                      decimal(15,4)          comment '已还本金'
--,paid_interest                       decimal(15,4)          comment '已还利息'
--,paid_term_fee                       decimal(15,4)          comment '已还手续费'
--,paid_svc_fee                        decimal(15,4)          comment '已还服务费'
--,paid_penalty                        decimal(15,4)          comment '已还罚息'
--,paid_mult                           decimal(15,4)          comment '已还滞纳金'
--
--,loan_num_should_repay_3             decimal(10,0)          comment '应还日大于等于快照日减3天的借据笔数'
--,loan_num_should_repay_5             decimal(10,0)          comment '应还日大于等于快照日减5天的借据笔数'
--,loan_num_should_repay_7             decimal(10,0)          comment '应还日大于等于快照日减7天的借据笔数'
--,loan_num_should_repay_30            decimal(10,0)          comment '应还日大于等于快照日减30天的借据笔数'
--,loan_num_should_repay_60            decimal(10,0)          comment '应还日大于等于快照日减60天的借据笔数'
--,loan_num_should_repay_90            decimal(10,0)          comment '应还日大于等于快照日减90天的借据笔数'
--,principal_should_repay_3            decimal(15,4)          comment '应还日大于等于快照日减3天的应还本金'
--,principal_should_repay_5            decimal(15,4)          comment '应还日大于等于快照日减5天的应还本金'
--,principal_should_repay_7            decimal(15,4)          comment '应还日大于等于快照日减7天的应还本金'
--,principal_should_repay_30           decimal(15,4)          comment '应还日大于等于快照日减30天的应还本金'
--,principal_should_repay_60           decimal(15,4)          comment '应还日大于等于快照日减60天的应还本金'
--,principal_should_repay_90           decimal(15,4)          comment '应还日大于等于快照日减90天的应还本金'
--) comment '应还日应还统计'
--partitioned by (biz_date string comment '观察日',product_id string comment '产品号')
--stored as parquet;
--set spark.executor.memory=4g;
--set spark.executor.memoryOverhead=4g;
--set spark.shuffle.memoryFraction=0.8;         -- shuffle操作的内存占比
--set spark.maxRemoteBlockSizeFetchToMem=200m;
--set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;   dw_new.dw_should_repay_summary
--set spark.shuffle.sort.bypassMergeThreshold=400;
--set spark.storage.memoryFraction=0.4;
--set spark.yarn.executor.memoryOverhead=2g;
--set hivevar:ST9=2021-01-03;
--set hivevar:db_suffix=;
--set hivevar:hive_param_str=and product_id in ('002001','001802','pl00282','002401','002402','001906','DIDI201908161538','001603','001901','002006','001801','001601','001602','002002','001702','001902','002501','002601','002602');
set hive.auto.convert.join=false;
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

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=512000000;
set mapred.min.split.size.per.rack=512000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.reducers.bytes.per.reducer=1024000000; --| |#每个节点的reduce 默认是处理1G大小的数据
set hive.exec.reducers.max=50;
--应还日 >= 应还日的借据笔数
insert overwrite table dw${db_suffix}.dw_should_repay_summary partition(biz_date='${ST9}', product_id)
select
 t1.should_repay_date
,t1.loan_init_term 
,t1.loan_term
,t1.loan_num
,t1.loan_amount
,t1.should_repay_amount       
,t1.should_repay_principal   
,t1.should_repay_interest    
,t1.should_repay_term_fee    
,t1.should_repay_svc_fee     
,t1.should_repay_penalty     
,t1.should_repay_mult_amt    
,t1.should_repay_penalty_acru
,t1.paid_amount              
,t1.paid_principal           
,t1.paid_interest            
,t1.paid_term_fee            
,t1.paid_svc_fee             
,t1.paid_penalty             
,t1.paid_mult
,t2.loan_num_should_repay_3
,t2.loan_num_should_repay_5
,t2.loan_num_should_repay_7
,t2.loan_num_should_repay_30
,t2.loan_num_should_repay_60
,t2.loan_num_should_repay_90
,t2.principal_should_repay_3
,t2.principal_should_repay_5
,t2.principal_should_repay_7
,t2.principal_should_repay_30
,t2.principal_should_repay_60
,t2.principal_should_repay_90
,t1.product_id
from
(select
should_repay_date
,loan_init_term
,loan_term
,count(distinct due_bill_no)             as  loan_num
,sum(nvl(loan_init_principal        ,0)) as  loan_amount
,sum(nvl(should_repay_amount        ,0)) as  should_repay_amount           
,sum(nvl(should_repay_principal     ,0)) as  should_repay_principal   
,sum(nvl(should_repay_interest      ,0)) as  should_repay_interest    
,sum(nvl(should_repay_term_fee      ,0)) as  should_repay_term_fee    
,sum(nvl(should_repay_svc_fee       ,0)) as  should_repay_svc_fee     
,sum(nvl(should_repay_penalty       ,0)) as  should_repay_penalty     
,sum(nvl(should_repay_mult_amt      ,0)) as  should_repay_mult_amt    
,sum(nvl(should_repay_penalty_acru  ,0)) as  should_repay_penalty_acru
,sum(nvl(paid_amount                ,0)) as  paid_amount              
,sum(nvl(paid_principal             ,0)) as  paid_principal           
,sum(nvl(paid_interest              ,0)) as  paid_interest            
,sum(nvl(paid_term_fee              ,0)) as  paid_term_fee            
,sum(nvl(paid_svc_fee               ,0)) as  paid_svc_fee             
,sum(nvl(paid_penalty               ,0)) as  paid_penalty             
,sum(nvl(paid_mult                  ,0)) as  paid_mult 
,product_id              
from
ods${db_suffix}.repay_schedule
where '${ST9}' between s_d_date and date_sub(e_d_date, 1)
and loan_term > 0
and loan_active_date <= '${ST9}'
${hive_param_str}
group by 
product_id
,should_repay_date
,loan_init_term
,loan_term
) t1
 join
(
select
product_id
,loan_init_term
,count(distinct (if(should_repay_date >=  date_sub('${ST9}',3),  due_bill_no, null))) as loan_num_should_repay_3
,count(distinct (if(should_repay_date >=  date_sub('${ST9}',5),  due_bill_no, null))) as loan_num_should_repay_5
,count(distinct (if(should_repay_date >=  date_sub('${ST9}',7),  due_bill_no, null))) as loan_num_should_repay_7
,count(distinct (if(should_repay_date >=  date_sub('${ST9}',30), due_bill_no, null))) as loan_num_should_repay_30
,count(distinct (if(should_repay_date >=  date_sub('${ST9}',60), due_bill_no, null))) as loan_num_should_repay_60
,count(distinct due_bill_no)                                                          as loan_num_should_repay_90

,sum(if(should_repay_date >=  date_sub('${ST9}', 3), should_repay_principal,0))       as principal_should_repay_3
,sum(if(should_repay_date >=  date_sub('${ST9}', 5), should_repay_principal,0))       as principal_should_repay_5
,sum(if(should_repay_date >=  date_sub('${ST9}', 7), should_repay_principal,0))       as principal_should_repay_7
,sum(if(should_repay_date >=  date_sub('${ST9}',30), should_repay_principal,0))       as principal_should_repay_30
,sum(if(should_repay_date >=  date_sub('${ST9}',60), should_repay_principal,0))       as principal_should_repay_60
,sum(should_repay_principal)                                                          as principal_should_repay_90

from
ods${db_suffix}.repay_schedule
where '${ST9}' between s_d_date and date_sub(e_d_date, 1)
and should_repay_date >= date_sub('${ST9}',90)
and loan_active_date <='${ST9}'
and loan_term > 0
${hive_param_str}
group by 
product_id
,loan_init_term
) t2
on t1.product_id = t2.product_id
and t1.loan_init_term = t2.loan_init_term
;