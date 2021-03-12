set mapred.job.name=dm_eagle.operation_overdue_datail;
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
--set hivevar:ST9=2020-08-06;
--逾期明细报表
--create table if not exists dm_eagle.operation_overdue_datail(
--    channel_id               string               COMMENT '合作渠道方'
--   ,project_id               string               COMMENT '项目名称'
--   ,due_bill_no              string               COMMENT '借据编号'
--   ,contract_no              string               COMMENT '合同编号'
--   ,loan_active_date         string               COMMENT '借据生效日'
--   ,loan_expire_date         string               COMMENT '借据到期日'
--   ,loan_init_principal      decimal(15,4)        COMMENT '借款金额'
--   ,cycle_day                decimal(2,0)         COMMENT '还款日'
--   ,curr_overdue_stage       string               COMMENT '当前逾期阶段'
--   ,overdue_date_first       string               COMMENT '首次逾期起始日期'
--   ,overdue_date_start       string               COMMENT '当前逾期起始日期'
--   ,overdue_terms_count      decimal(3,0)         COMMENT '累计逾期期数'
--   ,overdue_term             decimal(3,0)         COMMENT '当前逾期期数'
--   ,overdue_days             decimal(3,0)         COMMENT '当前逾期天数'
--   ,overdue_amount           decimal(15,4)        COMMENT '逾期金额'
--   ,overdue_principal        decimal(15,4)        COMMENT '逾期本金'
--   ,overdue_interest         decimal(15,4)        COMMENT '逾期利息'
--   ,overdue_fee              decimal(15,4)        COMMENT '逾期费用'
--   ,overdue_penalty          decimal(15,4)        COMMENT '逾期罚息'
--   ,remain_amount            decimal(15,4)        COMMENT '剩余未还金额'
--   ,remain_principal         decimal(15,4)        COMMENT '剩余未还本金'
--   ,remain_interest          decimal(15,4)        COMMENT '剩余未还利息'
--   ,remain_fee               decimal(15,4)        COMMENT '剩余未还费用'
--   ,remain_penalty_interest  decimal(15,4)        COMMENT '剩余未还罚息'
--) COMMENT '逾期明细报表' PARTITIONED BY (product_id string COMMENT '产品编号',snapshot_day string COMMENT '快照日')
--STORED AS PARQUET; 
insert overwrite table dm_eagle.operation_overdue_datail partition(product_id,snapshot_day)
select
 t2.channel_id                                                         --'合作渠道方'                             
,t2.project_id                                                         --'项目名称'
,t1.due_bill_no                                                        --'借据编号'
,t3.contract_no                                                        --'合同编号'
,t3.loan_active_date                                                   --'借据生效日'
,t3.loan_expire_date                                                   --'借据到期日'
,t1.loan_init_principal                                                --'借款金额'
,t3.cycle_day                                                          --'还款日'
,case 
    when overdue_days =0   then 'C'
    when overdue_days >0   and overdue_days <=30  then 'M1'--m1
    when overdue_days >30  and overdue_days <=60  then 'M2'--m2
    when overdue_days >60  and overdue_days <=90  then 'M3'--m3
    when overdue_days >90  and overdue_days <=120 then 'M4'--m4
    when overdue_days >120 and overdue_days <=150 then 'M5'--m5
    when overdue_days >150 and overdue_days <=180 then 'M6'--m6
    when overdue_days >180 then 'M7+'--m7
    else 'M7+'
    end as curr_overdue_stage                                          --当前逾期阶段     
,overdue_date_first                                                    --'首次逾期起始日期'     
,overdue_date_start                                                    --'当前逾期起始日期'
,overdue_terms_count                                                   --'累计逾期期数'
,overdue_term                                                          --'当前逾期期数'
,overdue_days                                                          --'当前逾期天数'
,(nvl(overdue_principal, 0) +
  nvl(overdue_interest , 0) +
  nvl(overdue_svc_fee  , 0) +
  nvl(overdue_term_fee , 0) +
  nvl(overdue_penalty  , 0) +
  nvl(overdue_mult_amt , 0)) as overdue_amount                         --'逾期金额'         
,overdue_principal                                                     --'逾期本金'
,overdue_interest                                                      --'逾期利息'
,(nvl(overdue_svc_fee,0)+nvl(overdue_term_fee,0)) as overdue_fee       --'逾期费用'
,overdue_penalty                                                       --'逾期罚息'
,remain_amount                                                         --'剩余未还金额'
,remain_principal                                                      --'剩余未还本金'
,remain_interest                                                       --'剩余未还利息'
,(nvl(remain_svc_fee, 0) + nvl(remain_term_fee,0)) as remain_fee       --'剩余未还费用'          
,0 as remain_penalty_interest                                          --'剩余未还罚息'
,t1.product_id                                                          
,'${ST9}' as snapshot_day
FROM (
    SELECT 
    *
    FROM 
    ods_new_s_cps.loan_info
    WHERE 
    '${ST9}' BETWEEN s_d_date AND date_sub(e_d_date, 1)
    and overdue_days > 0
) t1
     JOIN (    
    SELECT 
    DISTINCT 
    product_id, 
    channel_id,
    project_id
    FROM 
    dim_new.biz_conf
    where project_id in ('WS0009200001','WS0006200001','WS0006200002','WS0006200003')
    ) t2
    ON t1.product_id = t2.product_id
left join
    (select 
     due_bill_no
    ,contract_no
    ,loan_active_date
    ,loan_expire_date
    ,cycle_day
    from
    ods_new_s_cps.loan_lending) t3
    on t1.due_bill_no = t3.due_bill_no
union all
select
 t2.channel_id                                                         --'合作渠道方'
,t2.project_id                                                         --'项目名称'
,t1.due_bill_no                                                        --'借据编号'
,t3.contract_no                                                        --'合同编号'
,t3.loan_active_date                                                   --'借据生效日'
,t3.loan_expire_date                                                   --'借据到期日'
,t1.loan_init_principal                                                --'借款金额'
,t3.cycle_day                                                          --'还款日'
,case
    when overdue_days =0   then 'C'
    when overdue_days >0   and overdue_days <=30  then 'M1'--m1
    when overdue_days >30  and overdue_days <=60  then 'M2'--m2
    when overdue_days >60  and overdue_days <=90  then 'M3'--m3
    when overdue_days >90  and overdue_days <=120 then 'M4'--m4
    when overdue_days >120 and overdue_days <=150 then 'M5'--m5
    when overdue_days >150 and overdue_days <=180 then 'M6'--m6
    when overdue_days >180 then 'M7+'--m7
    else 'M7+'
    end as curr_overdue_stage                                          --当前逾期阶段
,overdue_date_first                                                    --'首次逾期起始日期'
,overdue_date_start                                                    --'当前逾期起始日期'
,overdue_terms_count                                                   --'累计逾期期数'
,overdue_term                                                          --'当前逾期期数'
,overdue_days                                                          --'当前逾期天数'
,(nvl(overdue_principal, 0) +
  nvl(overdue_interest , 0) +
  nvl(overdue_svc_fee  , 0) +
  nvl(overdue_term_fee , 0) +
  nvl(overdue_penalty  , 0) +
  nvl(overdue_mult_amt , 0)) as overdue_amount                         --'逾期金额'
,overdue_principal                                                     --'逾期本金'
,overdue_interest                                                      --'逾期利息'
,(nvl(overdue_svc_fee,0)+nvl(overdue_term_fee,0)) as overdue_fee       --'逾期费用'
,overdue_penalty                                                       --'逾期罚息'
,remain_amount                                                         --'剩余未还金额'
,remain_principal                                                      --'剩余未还本金'
,remain_interest                                                       --'剩余未还利息'
,(nvl(remain_svc_fee, 0) + nvl(remain_term_fee,0)) as remain_fee       --'剩余未还费用'
,0 as remain_penalty_interest                                          --'剩余未还罚息'
,t1.product_id
,'${ST9}' as snapshot_day
FROM (
    SELECT
    *
    FROM
    ods_new_s.loan_info
    WHERE
    '${ST9}' BETWEEN s_d_date AND date_sub(e_d_date, 1)
    and overdue_days > 0
) t1
     JOIN (
    SELECT
    DISTINCT
    product_id,
    channel_id,
    project_id
    FROM
    dim_new.biz_conf
    where project_id in ('bd')
    ) t2
    ON t1.product_id = t2.product_id
left join
    (select
     due_bill_no
    ,contract_no
    ,loan_active_date
    ,loan_expire_date
    ,cycle_day
    from
    ods_new_s.loan_lending) t3
    on t1.due_bill_no = t3.due_bill_no
;

