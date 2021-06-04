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
--set hivevar:ST9=2020-10-13;
--set hivevar:suffix=;
insert overwrite table dm_eagle${suffix}.operation_overdue_detail partition(product_id,snapshot_day)
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
,remain_othamounts as remain_penalty_interest                                          --'剩余未还罚息'
,t1.product_id
,'${ST9}' as snapshot_day
FROM
(
    SELECT
    *
    FROM
    ods_cps${suffix}.loan_info
    WHERE
    '${ST9}' BETWEEN s_d_date AND date_sub(e_d_date, 1)
    and overdue_days > 0
) t1
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
      where  project_id in ('WS0012200001','WS0009200001','WS0006200001','WS0006200002','WS0006200003','WS0013200001')
) t2
ON t1.product_id = t2.product_id
left join
(
    select
         due_bill_no
        ,contract_no
        ,loan_active_date
        ,loan_expire_date
        ,cycle_day
    from ods_cps${suffix}.loan_lending
) t3
on t1.due_bill_no = t3.due_bill_no
;

