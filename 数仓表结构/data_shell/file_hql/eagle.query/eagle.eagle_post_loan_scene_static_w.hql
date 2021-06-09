--create table if not exists eagle.eagle_post_loan_scene_static_w_cps(
-- project_id                        string             comment '项目ID'
--,loan_month                        string             comment '放款月'
--,loan_num_count                    decimal(15,0)      comment '总放款借据数'
--,loan_principal_count              decimal(15,4)      comment '总放款本金'
--,overdue3_bill                     decimal(15,0)      comment '3+借据笔数'                                                  
--,overdue7_bill                     decimal(15,0)      comment '7+借据笔数'                                                  
--,overdue30_bill                    decimal(15,0)      comment '30+借据笔数'                                                 
--,overdue90_bill                    decimal(15,0)      comment '90+借据笔数'    
--,overdue3_remainPrin_bill          decimal(15,4)      comment '3+本金余额'                                                  
--,overdue7_remainPrin_bill          decimal(15,4)      comment '7+本金余额'
--,overdue30_remainPrin_bill         decimal(15,4)      comment '30+本金余额'   
--,overdue90_remainPrin_bill         decimal(15,4)      comment '90+本金余额' 
--,prepayment_amount                 decimal(15,4)      comment '提前还款金额'            
--) comment '贷后-场景表-周表(静态池)'                                                                                                
--partitioned by (biz_date string comment '观察日',product_id string comment '产品号')
--stored as parquet;
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
---- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
set hive.auto.convert.join=true;

--explain
--set hivevar:db_suffix=;
--set hivevar:ST9=2021-02-28;
set hivevar:weekend_date=cast(date_sub(next_day('${ST9}','MONDAY'), 1) as string);
set hivevar:monday_date =cast(date_sub(next_day('${ST9}','MONDAY'), 7)as string);
set hivevar:product_id_list='001601','001602','001603','001802','002001','001906','002006','001901','001801','002002','001902','DIDI201908161538','002501','002601','002602';
--explain
insert overwrite table eagle.eagle_post_loan_scene_static_w${db_suffix} partition(biz_date, product_id)
select
 t3.project_id                
,t1.loan_month                
,t1.loan_num_count            
,t1.loan_principal_count      
,t2.overdue3_bill             
,t2.overdue7_bill             
,t2.overdue30_bill            
,t2.overdue90_bill            
,t2.overdue3_remainPrin_bill  
,t2.overdue7_remainPrin_bill  
,t2.overdue30_remainPrin_bill 
,t2.overdue90_remainPrin_bill 
,t4.prepayment_amount
,${weekend_date} AS biz_date
,t1.product_id         
from
(
select
  product_id,
  substr(loan_active_date,0,7)                                                         as loan_month,
  count(due_bill_no)                                                                   as loan_num_count,
  sum(loan_init_principal)                                                             as loan_principal_count
from
   ods${db_suffix}.repay_schedule 
   where 
   '${ST9}' between s_d_date and date_sub(e_d_date, 1)
   and loan_term = 1
   and loan_active_date <= '${ST9}'
   and product_id in (${product_id_list})
   group by 
   substr(loan_active_date,0,7)
   ,product_id
) t1
left join
(
select
     product_id
    ,substr(loan_active_date,0,7) as loan_month
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
    substr(loan_active_date,0,7)
    ,product_id
) t2
on t1.product_id = t2.product_id
and t1.loan_month = t2.loan_month
left join
(
select
    product_id
    ,substr(loan_active_date,0,7) as loan_month
    ,sum(prepay_principal)        as prepayment_amount
from
dw${db_suffix}.dw_loan_base_stat_repay_detail_day
where 
    biz_date <= ${weekend_date}
    and biz_date >= ${monday_date}
    and product_id in (${product_id_list})
group by 
product_id
,substr(loan_active_date,0,7)
) t4
on t1.product_id  = t4.product_id
and t1.loan_month = t4.loan_month
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
