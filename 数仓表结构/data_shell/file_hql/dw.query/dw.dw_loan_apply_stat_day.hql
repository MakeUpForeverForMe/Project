-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;

--CREATE TABLE IF NOT EXISTS `dw.dw_loan_apply_stat_day`(
--  `loan_terms`                            decimal(3,0)   COMMENT '贷款期数',
--
--  `loan_apply_date`                       string         COMMENT '用信申请日期',
--  `loan_apply_num`                        decimal(20,0)  COMMENT '用信申请笔数',
--  `loan_apply_num_count`                  decimal(20,0)  COMMENT '累计申请笔数',
--  `loan_apply_num_person`                 decimal(20,0)  COMMENT '用信申请人数',
--  `loan_apply_num_person_count`           decimal(20,0)  COMMENT '累计用信申请人数',
--  `loan_apply_amount`                     decimal(25,5)  COMMENT '用信申请金额',
--  `loan_apply_amount_count`               decimal(25,5)  COMMENT '累计用信申请金额',
--  `loan_approval_date`                    string         COMMENT '用信通过日期',
--  `loan_approval_num`                     decimal(20,0)  COMMENT '用信通过笔数',
--  `loan_approval_num_person`              decimal(20,0)  COMMENT '用信通过人数',
--  `loan_approval_amount`                  decimal(25,5)  COMMENT '用信通过金额'
--) COMMENT '轻度用信统计（申请）'
--PARTITIONED BY (`biz_date` string COMMENT '用信申请日期',`product_id` string COMMENT '产品编号')
--STORED AS PARQUET;


--set hivevar:ST9=2021-01-01;
--set hivevar:hive_param_str=and product_id in ('001801') ;
insert overwrite table dw.dw_loan_apply_stat_day partition(biz_date,product_id)
select
nvl(loan_apply_day.loan_terms,loan_apply_acc.loan_terms)              as loan_terms,
nvl(loan_apply_day.loan_apply_date,loan_apply_acc.loan_apply_date)    as loan_apply_date,
nvl(loan_apply_day.loan_apply_num,0)                                  as loan_apply_num,
nvl(loan_apply_acc.loan_apply_num_count,0)                            as loan_apply_num_count,
nvl(loan_apply_day.loan_apply_num_person,0)                           as loan_apply_num_person,
nvl(loan_apply_acc.loan_apply_num_person_count,0)                     as loan_apply_num_person_count,
nvl(loan_apply_day.loan_apply_amount,0)                               as loan_apply_amount,
nvl(loan_apply_acc.loan_apply_amount_count,0)                         as loan_apply_amount_count,
nvl(loan_apply_day.loan_approval_date,null)                           as loan_approval_date,
nvl(loan_apply_day.loan_approval_num,0)                               as loan_approval_num,
nvl(loan_apply_day.loan_approval_num_person,0)                        as loan_approval_num_person,
nvl(loan_apply_day.loan_approval_amount,0)                            as loan_approval_amount,
nvl(loan_apply_day.biz_date,loan_apply_acc.loan_apply_date)           as biz_date,
nvl(loan_apply_day.product_id,loan_apply_acc.product_id)              as product_id
from
(select
  loan_terms,
  loan_apply_date                                             as loan_apply_date,
  count(due_bill_no)                                          as loan_apply_num,
  count(distinct user_hash_no)                                as loan_apply_num_person,
  sum(loan_amount_apply)                                      as loan_apply_amount,
  loan_approval_date                                          as loan_approval_date,
  count(if(apply_status in (1,4),due_bill_no,null))           as loan_approval_num,
  count(distinct if(apply_status in (1,4),user_hash_no,null)) as loan_approval_num_person,
  sum(if(apply_status in (1,4),loan_amount_approval,0))       as loan_approval_amount,
  loan_apply_date                                             as biz_date,
  product_id
from (
  select
    user_hash_no,
    apply_id,
    due_bill_no,
    loan_terms,
    biz_date as loan_apply_date,
    loan_amount_apply,
    apply_status,
    to_date(issue_time) as loan_approval_date,
    loan_amount_approval,
    product_id
  from ods.loan_apply
  where 1 > 0
    and biz_date = '${ST9}'
    and (
      case
        when product_id = 'pl00282' and biz_date > '2019-02-22' then false
        else true
      end
    )
   ${hive_param_str}
) as loan_apply
group by
  loan_terms,
  loan_apply_date,
  loan_approval_date,
  product_id
-- order by product_id,loan_apply_date
-- limit 5
)loan_apply_day
full join (
select
    loan_terms,
    '${ST9}'                                                    as loan_apply_date,
    product_id,
    count(due_bill_no)                                          as loan_apply_num_count,
    count(distinct user_hash_no)                                as loan_apply_num_person_count,
    sum(loan_amount_apply)                                      as loan_apply_amount_count
    from ods.loan_apply
    where 1 > 0
    and biz_date <= '${ST9}'
    and (
      case
        when product_id = 'pl00282' and biz_date > '2019-02-22' then false
        else true
      end
    )
   ${hive_param_str}
    group by
    loan_terms,product_id
)loan_apply_acc
on loan_apply_day.loan_terms=loan_apply_acc.loan_terms
and loan_apply_day.loan_apply_date=loan_apply_acc.loan_apply_date
and loan_apply_day.product_id=loan_apply_acc.product_id

;
