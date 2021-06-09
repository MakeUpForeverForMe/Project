--create table if not exists eagle.eagle_post_loan_scene_w_cps(
-- project_id                        string             comment '项目ID'
--,overdue3_bill                     decimal(15,0)      comment '3+借据笔数'
--,overdue7_bill                     decimal(15,0)      comment '7+借据笔数'
--,overdue30_bill                    decimal(15,0)      comment '30+借据笔数'
--,overdue90_bill                    decimal(15,0)      comment '90+借据笔数'
--
--,overdue3_contract_num             decimal(15,0)      comment '第一次还款日3天前的合同笔数'
--,overdue7_contract_num             decimal(15,0)      comment '第一次还款日7天前的合同笔数'
--,overdue30_contract_num            decimal(15,0)      comment '第一次还款日30天前的合同笔数'
--,overdue90_contract_num            decimal(15,0)      comment '第一次还款日90天前的合同笔数'
--
--,overdue3_remainPrin_bill          decimal(15,4)      comment '3+本金余额'                                                  
--,overdue7_remainPrin_bill          decimal(15,4)      comment '7+本金余额'
--,overdue30_remainPrin_bill         decimal(15,4)      comment '30+本金余额'   
--,overdue90_remainPrin_bill         decimal(15,4)      comment '90+本金余额' 
--
--,overdue3_contract_amount          decimal(15,4)      comment '第一次还款日3天前的合同余额'
--,overdue7_contract_amount          decimal(15,4)      comment '第一次还款日7天前的合同余额'
--,overdue30_contract_amount         decimal(15,4)      comment '第一次还款日30天前的合同余额'
--,overdue90_contract_amount         decimal(15,4)      comment '第一次还款日90天前的合同余额'
--
--) comment '贷后-场景表-周表'
--partitioned by (biz_date string comment '观察日',product_id string comment '产品号')
--stored as parquet;
--set hivevar:db_suffix=;
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
--set hivevar:ST9=2021-01-01;
set hivevar:weekend_date=date_sub(next_day('${ST9}','MONDAY'), 1);
set hivevar:monday_date=date_sub(next_day('${ST9}','MONDAY'), 7);
set hivevar:product_id_list='001601','001602','001603','001802','002001','001906','002006','001901','001801','002002','001902','DIDI201908161538','002501','002601','002602';

insert overwrite table eagle.eagle_post_loan_scene_w${db_suffix} partition(biz_date, product_id)

select
 t3.project_id                  --'项目ID'
,t1.overdue3_bill               --'3+借据笔数'
,t1.overdue7_bill               --'7+借据笔数'
,t1.overdue30_bill              --'30+借据笔数'
,t1.overdue90_bill              --'90+借据笔数'
                                
,t2.overdue3_contract_num       --'第一次还款日3天前的合同笔数'
,t2.overdue7_contract_num       --'第一次还款日7天前的合同笔数'
,t2.overdue30_contract_num      --'第一次还款日30天前的合同笔数'
,t2.overdue90_contract_num      --'第一次还款日90天前的合同笔数'
                                
,t1.overdue3_remainPrin_bill    --'3+本金余额'                   
,t1.overdue7_remainPrin_bill    --'7+本金余额'
,t1.overdue30_remainPrin_bill   --'30+本金余额'   
,t1.overdue90_remainPrin_bill   --'90+本金余额' 
                                
,t2.overdue3_contract_amount    --'第一次还款日3天前的合同余额'
,t2.overdue7_contract_amount    --'第一次还款日7天前的合同余额'
,t2.overdue30_contract_amount   --'第一次还款日30天前的合同余额'
,t2.overdue90_contract_amount   --'第一次还款日90天前的合同余额'

,${weekend_date} as biz_date
,t3.product_id 

from
(
select
     product_id
    ,sum(if(overdue_days>3, overdue_loan_num,0)) as overdue3_bill
    ,sum(if(overdue_days>7, overdue_loan_num,0)) as overdue7_bill
    ,sum(if(overdue_days>30,overdue_loan_num,0)) as overdue30_bill
    ,sum(if(overdue_days>90,overdue_loan_num,0)) as overdue90_bill
    
    ,sum(if(overdue_days>3, remain_principal,0)) as overdue3_remainPrin_bill
    ,sum(if(overdue_days>7, remain_principal,0)) as overdue7_remainPrin_bill
    ,sum(if(overdue_days>30,remain_principal,0)) as overdue30_remainPrin_bill
    ,sum(if(overdue_days>90,remain_principal,0)) as overdue90_remainPrin_bill
from
    dw${db_suffix}.dw_loan_base_stat_overdue_num_day
where
    biz_date = ${weekend_date}
    AND loan_active_date <= ${weekend_date}
    and product_id in (${product_id_list})
    group by
    product_id
) t1
full join
(
select
    product_id
   ,sum(loan_amount)                                                             overdue3_contract_amount
   ,sum(if(should_repay_date <= date_sub(${weekend_date},7 ), loan_amount, 0))   overdue7_contract_amount
   ,sum(if(should_repay_date <= date_sub(${weekend_date},30), loan_amount, 0))   overdue30_contract_amount
   ,sum(if(should_repay_date <= date_sub(${weekend_date},90), loan_amount, 0))   overdue90_contract_amount
                                                                                
   ,sum(loan_num)                                                                overdue3_contract_num 
   ,sum(if(should_repay_date <= date_sub(${weekend_date},7 ), loan_num, 0))      overdue7_contract_num 
   ,sum(if(should_repay_date <= date_sub(${weekend_date},30), loan_num, 0))      overdue30_contract_num
   ,sum(if(should_repay_date <= date_sub(${weekend_date},90), loan_num, 0))      overdue90_contract_num   
from
    dw${db_suffix}.dw_should_repay_summary
where 
    biz_date = ${weekend_date}
    and loan_term = 1
    and product_id in (${product_id_list})
    and should_repay_date <= date_sub(${weekend_date},3)
group by product_id
) t2
on t1.product_id = t2.product_id
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
) as t3
on t1.product_id = t3.product_id
;