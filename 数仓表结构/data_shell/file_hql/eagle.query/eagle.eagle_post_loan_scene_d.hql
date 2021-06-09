--set hivevar:ST9=2021-01-03;
--set hivevar:db_suffix=;
--create table if not exists eagle.eagle_post_loan_scene_d_cps(
--     project_id                        string             comment '项目ID'
--    ,remain_principal_d                decimal(15,4)      comment '日末本金余额'
--    ,loan_avg_days                     decimal(15,2)      comment '平均用信天数-当日结清借据，从放款至结清的天数的平均值'
--    ,overdue3_Principal                decimal(15,4)      comment 'DPD3逾期本金' 
--    ,overdue3_bill                     decimal(15,0)      comment 'DPD3逾期笔数'
--    ,shouldPayPrin_3days_before        decimal(15,4)      comment '3天前应还本金' 
--    ,shouldPayBill_3days_before        decimal(15,0)      comment '3天前应还笔数'
--    ,DPD3_entryRate_amount             decimal(15,4)      comment 'DPD3金额入催率'
--    ,DPD3_entryRate_bill               decimal(15,4)      comment 'DPD3笔数入催率'     
--) comment '贷后-场景表-日表'
--partitioned by (biz_date string comment '观察日',product_id string comment '产品号')
--stored as parquet;

---set hivevar:db_suffix=;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=209715200;
set hive.auto.convert.join.noconditionaltask=true;

-- 多个mapjoin 转换为1个时，限制输入的最大的数据量 影响tez，默认10m 
set hive.auto.convert.join.noconditionaltask.size =209715200;
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
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=512000000;
set mapred.min.split.size.per.rack=512000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.reducers.bytes.per.reducer=1024000000; --| |#每个节点的reduce 默认是处理1G大小的数据
set hive.exec.reducers.max=50;

set hivevar:product_id_list='001601','001602','001603','001802','002001','001906','002006','001901','001801','002002','001902','DIDI201908161538','002501','002601','002602';


insert overwrite table eagle.eagle_post_loan_scene_d${db_suffix} partition(biz_date='${ST9}',product_id)
select
    t4.project_id                                                        as project_id                
    ,nvl(t1.remain_principal_d,0)                                        as remain_principal_d        
    ,nvl(settle_loan_days/settle_num,0)                                  as loan_avg_days             
    ,nvl(t1.overdue3_Principal        ,0)                                as overdue3_Principal        
    ,nvl(t1.overdue3_bill             ,0)                                as overdue3_bill             
    ,nvl(t2.shouldPayPrin_3days_before,0)                                as shouldPayPrin_3days_before
    ,nvl(t2.shouldPayBill_3days_before,0)                                as shouldPayBill_3days_before
    ,nvl(t1.overdue3_remain_Principal/t2.shouldPayPrin_3days_before,0  ) as DPD3_entryRate_amount     
    ,nvl(t1.overdue3_bill/t2.shouldPayBill_3days_before,0)               as DPD3_entryRate_bill
    ,t4.product_id                                                       AS product_id
from
(select
    product_id
    ,sum(remain_principal) as remain_principal_d
    ,sum(if(overdue_days = 3, remain_principal, 0))  as overdue3_remain_Principal
    ,sum(if(overdue_days = 3, overdue_loan_num, 0))  as overdue3_bill
    ,sum(if(overdue_days = 3, overdue_principal, 0)) as overdue3_Principal
from
    dw${db_suffix}.dw_loan_base_stat_overdue_num_day
where 
    biz_date = '${ST9}'
    and product_id in (${product_id_list})
group by 
    product_id
) t1
full join
(select
  product_id
  ,sum(shouldPayPrin_3days_before) as shouldPayPrin_3days_before
  ,sum(shouldPayBill_3days_before) as shouldPayBill_3days_before
from
(select
    product_id
    ,loan_init_term
    ,max(principal_should_repay_3)      as shouldPayPrin_3days_before
    ,max(loan_num_should_repay_3)       as shouldPayBill_3days_before
from
    dw${db_suffix}.dw_should_repay_summary
where biz_date = '${ST9}'
    and product_id in (${product_id_list})
group by 
    product_id
    ,loan_init_term
) t
group by product_id   
) t2
on t1.product_id = t2.product_id
full join
(
select
    product_id
    ,sum(settle_num)       as settle_num
    ,sum(settle_loan_days) as settle_loan_days
from
    dw${db_suffix}.dw_loan_base_stat_repay_detail_day 
where 
    biz_date = '${ST9}'
    and product_id in (${product_id_list})
group by
    product_id
) t3
on t1.product_id = t3.product_id
join
     (
  select distinct
    project_id,
    product_id
  from (
    select
      max(if(col_name = 'project_id',   col_val,null)) as project_id,
      max(if(col_name = 'product_id',   col_val,null)) as product_id
    from dim.data_conf
    where col_type = 'ac'
    group by col_id
  ) as tmp
  where product_id is not null
) as t4
on coalesce(t1.product_id,t2.product_id,t3.product_id) = t4.product_id
;
