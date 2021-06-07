--set hive.exec.parallel=true;
--set hive.exec.parallel.thread.number=8;
--SET hivevar:ST9=2021-04-30;
set hivevar:product_id_list='001801','001802';
--set hivevar:product_id_list='001801','001802';
set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=256000000;      -- 64M
set hive.merge.smallfiles.avgsize=256000000; -- 64M
set mapreduce.input.fileinputformat.split.minsize=268435456;

-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
set hive.auto.convert.join.noconditionaltask=false;
set hive.auto.convert.join=false;
insert overwrite table eagle.one_million_random_risk_data partition (project_id,cycle_key)
select
    t1.due_bill_no                                          ,
    t1.product_id                                           ,
    t1.risk_level                                      ,
    t1.loan_init_principal                                  ,
    t1.loan_init_term                                       ,
    t1.loan_init_interest_rate                              ,
    nvl(t2.creditcoef, t1.loan_init_interest_rate) as credit_coef                                          ,
    t1.loan_init_penalty_rate                               ,
    t1.loan_active_date                                     ,
    t1.settled          ,
    t1.paid_out_date                                        ,
    t1.loan_out_reason                                      ,
    t1.paid_out_type                                        ,
    t1.schedule as schedule_detail                          ,
    t1.Loan_status                                          ,
    t1.paid_amount                                          ,
    t1.paid_principal                                       ,
    t1.paid_interest                                        ,
    t1.paid_penalty                                         ,
    t1.paid_svc_fee                                         ,
    t1.paid_term_fee                                        ,
    t1.paid_mult                                            ,
    t1.remain_amount                                        ,
    t1.remain_principal                                     ,
    t1.remain_interest                                      ,
    t1.remain_svc_fee                                       ,
    t1.remain_term_fee                                      ,
    t1.overdue_principal                                    ,
    t1.overdue_interest                                     ,
    t1.overdue_svc_fee                                      ,
    t1.overdue_term_fee                                     ,
    t1.overdue_penalty                                      ,
    t1.overdue_mult_amt                                     ,
    t1.overdue_date_first                                   ,
    t1.overdue_date_start                                   ,
    t1.overdue_days                                         ,
    t1.overdue_date                                         ,
    t1.dpd_begin_date                                       ,
    t1.dpd_days                                             ,
    t1.dpd_days_count                                       ,
    t1.dpd_days_max                                         ,
    t1.collect_out_date                                     ,
    t1.overdue_term                                         ,
    t1.overdue_terms_count                                  ,
    t1.overdue_terms_max                                    ,
    t1.overdue_principal_accumulate                         ,
    t1.overdue_principal_max                                ,
    t2.creditLimit as creditlimit
    ,t2.edu  --
    ,t2.degree --
    ,t2.homests --
    ,t2.marriage --
    ,t2.mincome    --
    ,t2.homeIncome as homeincome
    ,t2.zxhomeIncome as zxhomeincome
    ,t2.custType  as custtype
    ,t2.workType
    ,t2.workDuty as workduty  --
    ,t2.workTitle as worktitle --
    ,t2.idcard_area
    ,t2.riskLevel
    ,'6' AS scoreRange,
    cast(row_number() over ( partition by biz_conf.project_id order by t1.due_bill_no asc ) as int) as rn,

    t1.biz_date,
    biz_conf.project_id,
    "0" as cycle_key
from
(select * from eagle.one_million_random_loan_data_day  where biz_date='${ST9}')t1
join eagle.one_million_random_customer_reqdata t2 on t1.due_bill_no = t2.due_bill_no and  t1.product_id = t2.product_id
join(select
max(if(col_name='project_id',col_val,null)) as project_id,
max(if(col_name='product_id',col_val,null)) as product_id
from dim.data_conf
where col_type='ac'
group by col_id
)biz_conf on t1.product_id =biz_conf.product_id
;