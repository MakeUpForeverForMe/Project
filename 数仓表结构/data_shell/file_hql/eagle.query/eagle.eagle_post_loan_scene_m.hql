--create table if not exists eagle.eagle_post_loan_scene_m(
-- project_id                        string             comment '项目ID'                                                      
--,total_settle_user                 decimal(15,0)      comment '累计结清用户数'                                              
--,month_settle_user                 decimal(15,0)      comment '月净结清用户数'                                              
--,on_loan_user_m                    decimal(15,0)      comment '月末在贷用户数'                                              
--,increase_user_m                   decimal(15,0)      comment '净增余额用户数'                                              
--,remain_principal_m                decimal(15,4)      comment '月末本金余额'                                                
--,remain_principal_avg              decimal(15,4)      comment '月均本金余额'                                                
--,remain_principal_user             decimal(15,4)      comment '月末户均余额'                                                
--,due_should_repay_principal        decimal(15,4)      comment '月到期应还本金'                                              
--,due_paid_principal                decimal(15,4)      comment '月到期实还本金'                                              
--,interest_income_m                 decimal(15,4)      comment '月息费收入'                                                  
--,avg_interest_rate                 decimal(15,4)      comment '平均年化利率'                                                
--,overdue30_user                    decimal(15,0)      comment '30+用户数'                                                   
--,overdue30_remainPrin_user         decimal(15,4)      comment '30+本金余额(用户)'                                           
--,overdue30_remainPrin_bill         decimal(15,4)      comment '30+本金余额(借据)'                                           
--,overdue60_user                    decimal(15,0)      comment '60+用户数'                                                   
--,overdue60_remainPrin_user         decimal(15,4)      comment '60+本金余额(用户)'                                           
--,overdue60_remainPrin_bill         decimal(15,4)      comment '60+本金余额(借据)'                                           
--,overdue90_user                    decimal(15,0)      comment '90+用户数'                                                   
--,overdue90_remainPrin_user         decimal(15,4)      comment '90+本金余额(用户)'                                           
--,overdue90_remainPrin_bill         decimal(15,4)      comment '90+本金余额(借据)'                                           
--,overdue30_increase_remainPrin     decimal(15,4)      comment '月增30+本金余额'                                             
--,overdue30_increase_overduerate    decimal(15,4)      comment '月增30+逾期率'                                               
--,overdue60_increase_remainPrin     decimal(15,4)      comment '月增60+本金余额'                                             
--,overdue60_increase_overduerate    decimal(15,4)      comment '月增60+逾期率'                                               
--,overdue90_increase_remainPrin     decimal(15,4)      comment '月增90+本金余额'                                             
--,overdue90_increase_overduerate    decimal(15,4)      comment '月增90+逾期率'                                               
--,first_term_overduePrin            decimal(15,4)      comment '首期还款拖欠金额'                                            
--,first_term_loan_amount            decimal(15,4)      comment '首期应还的放款金额' 
--,first_term_overdue_rate           decimal(15,4)      comment '首期逾期率' 
--,M1_repay_principal                decimal(15,4)      comment 'M1回收金额'
--,M1_should_repay_principal         decimal(15,4)      comment 'M1应还金额'
--,M1_recovery_rate                  decimal(15,4)      comment 'M1回收率'
--,M2_repay_principal                decimal(15,4)      comment 'M2回收金额'
--,M2_should_repay_principal         decimal(15,4)      comment 'M2应还金额'
--,M2_recovery_rate                  decimal(15,4)      comment 'M2回收率'
--,M3_repay_principal                decimal(15,4)      comment 'M3回收金额'
--,M3_should_repay_principal         decimal(15,4)      comment 'M3应还金额'
--,M3_recovery_rate                  decimal(15,4)      comment 'M3回收率'
--,normal_remain_rate                decimal(15,4)      comment '正常余额占比'                                                
--,dpd30_remain_rate                 decimal(15,4)      comment 'DPD30+余额占比'                                              
--,dpd60_remain_rate                 decimal(15,4)      comment 'DPD60+余额占比'                                              
--,dpd90_remain_rate                 decimal(15,4)      comment 'DPD90+余额占比'                                              
--,dpd1_30remain_rate                decimal(15,4)      comment 'DPD1-30余额占比'                                             
--,dpd31_60remain_rate               decimal(15,4)      comment 'DPD31-60余额占比'                                            
--,dpd61_90remain_rate               decimal(15,4)      comment 'DPD61-90余额占比'                                            
--,dpd1_30migration_rate             decimal(15,4)      comment 'C/(DPD1-30)迁徙率'                                           
--,dpd30_migration_rate              decimal(15,4)      comment '(DPD1-30)/(DPD30+)迁徙率'                                                                                     
--,overdue3_bill                     decimal(15,0)      comment '3+借据笔数'                                                  
--,overdue7_bill                     decimal(15,0)      comment '7+借据笔数'                                                  
--,overdue30_bill                    decimal(15,0)      comment '30+借据笔数'                                                 
--,overdue90_bill                    decimal(15,0)      comment '90+借据笔数'                                                 
--,overdue3_contract_num             decimal(15,0)      comment '第一次还款日3天前的合同笔数'                                 
--,overdue7_contract_num             decimal(15,0)      comment '第一次还款日7天前的合同笔数'                                 
--,overdue30_contract_num            decimal(15,0)      comment '第一次还款日30天前的合同笔数'                                
--,overdue90_contract_num            decimal(15,0)      comment '第一次还款日90天前的合同笔数'                                
--,overdue3_remainPrin_bill          decimal(15,4)      comment '3+本金余额'                                                  
--,overdue7_remainPrin_bill          decimal(15,4)      comment '7+本金余额'                                                  
--,overdue3_contract_amount          decimal(15,4)      comment '第一次还款日3天前的合同余额'                                 
--,overdue7_contract_amount          decimal(15,4)      comment '第一次还款日7天前的合同余额'                                 
--,overdue30_contract_amount         decimal(15,4)      comment '第一次还款日30天前的合同余额'                                
--,overdue90_contract_amount         decimal(15,4)      comment '第一次还款日90天前的合同余额'                                
--) comment '贷后-场景表-月表'                                                                                                
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
--set hivevar:db_suffix=;
--set hivevar:ST9=2021-01-03;
insert overwrite table eagle.eagle_post_loan_scene_m${db_suffix} partition(biz_date, product_id)
select
t5.project_id
,t2.total_settle_user
,(t2.total_settle_user - t2.lastM_settle_user) as month_settle_user
,t2.on_loan_user_m
,(t2.on_loan_user_m - t2.lastM_on_loan_user_m) as increase_user_m
,t1.remain_principal_m
,round(t1.remain_principal_m/datediff(last_day('${ST9}'), last_day(add_months('${ST9}',-1))), 4) as remain_principal_avg
,round(t1.remain_principal_m/t2.on_loan_user_m, 4) as remain_principal_user
,t2.due_should_repay_principal
,t2.due_paid_principal
,t2.interest_income_m
,round(t2.interest_income_m/(t1.remain_principal_m/datediff(last_day('${ST9}'), last_day(add_months('${ST9}',-1)))),4) as avg_interest_rate
,t2.overdue30_user
,t2.overdue30_remainPrin_user
,t1.overdue30_remainPrin_bill
,t2.overdue60_user
,t2.overdue60_remainPrin_user
,t1.overdue60_remainPrin_bill
,t2.overdue90_user
,t2.overdue90_remainPrin_user
,t1.overdue90_remainPrin_bill
,nvl(t1.overdue30_remainPrin_bill-t1.lastM_overdue30_remainPrin_bill,0) as overdue30_increase_remainPrin
,nvl(t1.overdue30_remainPrin_bill-t1.lastM_overdue30_remainPrin_bill,0)/(t1.remain_principal_m/datediff(last_day('${ST9}'), last_day(add_months('${ST9}',-1)))) as overdue30_increase_overduerate
,nvl(t1.overdue60_remainPrin_bill-t1.lastM_overdue60_remainPrin_bill,0) as overdue60_increase_remainPrin
,nvl(t1.overdue60_remainPrin_bill-t1.lastM_overdue60_remainPrin_bill,0)/(t1.remain_principal_m/datediff(last_day('${ST9}'), last_day(add_months('${ST9}',-1)))) as overdue60_increase_overduerate
,nvl(t1.overdue90_remainPrin_bill-t1.lastM_overdue90_remainPrin_bill,0) as overdue90_increase_remainPrin
,nvl(t1.overdue90_remainPrin_bill-t1.lastM_overdue90_remainPrin_bill,0)/(t1.remain_principal_m/datediff(last_day('${ST9}'), last_day(add_months('${ST9}',-1)))) as overdue90_increase_overduerate
,t2.first_term_overduePrin
,t2.first_term_loan_amount
,(t2.first_term_overduePrin/t2.first_term_loan_amount) as first_term_overdue_rate
,t4.M1_repay_principal
,t4.M1_should_repay_principal
,(t4.M1_repay_principal/t4.M1_should_repay_principal) as M1_recovery_rate
,t4.M2_repay_principal
,t4.M2_should_repay_principal
,(t4.M2_repay_principal/t4.M2_should_repay_principal) as M2_recovery_rate
,t4.M3_repay_principal
,t4.M3_should_repay_principal
,(t4.M3_repay_principal/t4.M3_should_repay_principal) as M3_recovery_rate
,(t1.normal_remainPrin/t1.remain_principal_m) as normal_remain_rate
,(t1.overdue30_remainPrin_bill/t1.remain_principal_m) as dpd30_remain_rate
,(t1.overdue60_remainPrin_bill/t1.remain_principal_m) as dpd60_remain_rate
,(t1.overdue90_remainPrin_bill/t1.remain_principal_m) as dpd90_remain_rate
,(t1.dpd1_30remainPrin/t1.remain_principal_m) as dpd1_30remain_rate
,(t1.dpd31_60remainPrin/t1.remain_principal_m) as dpd31_60remain_rate
,(t1.dpd61_90remainPrin/t1.remain_principal_m) as dpd61_90remain_rate
,(t1.dpd1_30remainPrin/lastM_normal_remainPrin) as dpd1_30migration_rate
,(t1.overdue30_remainPrin_bill/t1.lastM_dpd1_30remainPrin) as dpd30_migration_rate
,t1.overdue3_bill
,t1.overdue7_bill
,t1.overdue30_bill
,t1.overdue90_bill
,t3.overdue3_contract_num 
,t3.overdue7_contract_num 
,t3.overdue30_contract_num
,t3.overdue90_contract_num
,t1.overdue3_remainPrin_bill
,t1.overdue7_remainPrin_bill
,t3.overdue3_contract_amount 
,t3.overdue7_contract_amount 
,t3.overdue30_contract_amount
,t3.overdue90_contract_amount
,'${ST9}' as biz_date
,t5.product_id
from
(select
    product_id
    ,sum(if(biz_date = '${ST9}' ,remain_principal, 0)) as remain_principal_m
    ,sum(if(biz_date = '${ST9}' and overdue_days=0,remain_principal,0))  as normal_remainPrin
    ,sum(if(biz_date = last_day(add_months('${ST9}',-1)) AND overdue_days=0,remain_principal,0)) as lastM_normal_remainPrin
    ,sum(if(biz_date = '${ST9}' AND overdue_days>3, overdue_loan_num,0)) as overdue3_bill
    ,sum(if(biz_date = '${ST9}' AND overdue_days>7, overdue_loan_num,0)) as overdue7_bill
    ,sum(if(biz_date = '${ST9}' AND overdue_days>30,overdue_loan_num,0)) as overdue30_bill
    ,sum(if(biz_date = '${ST9}' AND overdue_days>90,overdue_loan_num,0)) as overdue90_bill
    
    ,sum(if(biz_date = '${ST9}' AND overdue_days>3, remain_principal,0)) as overdue3_remainPrin_bill
    ,sum(if(biz_date = '${ST9}' AND overdue_days>7, remain_principal,0)) as overdue7_remainPrin_bill
    ,sum(if(biz_date = '${ST9}' AND overdue_days>30,remain_principal,0)) as overdue30_remainPrin_bill
    ,sum(if(biz_date = '${ST9}' AND overdue_days>60,remain_principal,0)) as overdue60_remainPrin_bill
    ,sum(if(biz_date = '${ST9}' AND overdue_days>90,remain_principal,0)) as overdue90_remainPrin_bill
    
    ,sum(if(biz_date = last_day(add_months('${ST9}',-1)) AND overdue_days>30,remain_principal,0)) as lastM_overdue30_remainPrin_bill
    ,sum(if(biz_date = last_day(add_months('${ST9}',-1)) AND overdue_days>60,remain_principal,0)) as lastM_overdue60_remainPrin_bill
    ,sum(if(biz_date = last_day(add_months('${ST9}',-1)) AND overdue_days>90,remain_principal,0)) as lastM_overdue90_remainPrin_bill
    ,sum(if(biz_date = last_day(add_months('${ST9}',-1)) AND overdue_days>=1  and overdue_days<=30,remain_principal,0)) as lastM_dpd1_30remainPrin
    ,sum(if(biz_date = '${ST9}' AND overdue_days>=1  and overdue_days<=30,remain_principal,0)) as dpd1_30remainPrin
    ,sum(if(biz_date = '${ST9}' AND overdue_days>=31 and overdue_days<=60,remain_principal,0)) as dpd31_60remainPrin
    ,sum(if(biz_date = '${ST9}' AND overdue_days>=61 and overdue_days<=90,remain_principal,0)) as dpd61_90remainPrin

from
    dw${db_suffix}.dw_loan_base_stat_overdue_num_day
where 
    (biz_date = '${ST9}'
    or
    biz_date = last_day(add_months('${ST9}',-1)))
    and product_id in (${product_id_list})
group by product_id
) t1
full join
(select
    product_id
    ,sum(if(loan_num=settle_loan_num and biz_date = '${ST9}' ,1,0))                                                 as total_settle_user
    ,sum(if(loan_num=settle_loan_num and biz_date = last_day(add_months('${ST9}',-1)) ,1,0))                        as lastM_settle_user
    ,sum(if(loan_num>settle_loan_num and biz_date = '${ST9}',1,0))                                                  as on_loan_user_m
    ,sum(if(loan_num>settle_loan_num and biz_date = last_day(add_months('${ST9}',-1)),1,0))                         as lastM_on_loan_user_m
    ,sum(if(substr(biz_date,0,7) = substr('${ST9}',0,7),should_repay_principal,0))                                  as due_should_repay_principal
    ,sum(if(substr(biz_date,0,7) = substr('${ST9}',0,7),paid_principal,0))                                          as due_paid_principal
    ,sum(if(substr(biz_date,0,7) = substr('${ST9}',0,7),(paid_interest+paid_penalty+paid_svc_fee+paid_fee),0))      as interest_income_m
    ,sum(if(overdue_days > 30 and biz_date = '{ST9}', 1, 0)) as overdue30_user
    ,sum(if(overdue_days > 60 and biz_date = '{ST9}', 1, 0)) as overdue60_user
    ,sum(if(overdue_days > 90 and biz_date = '{ST9}', 1, 0)) as overdue90_user
    ,sum(if(overdue_days > 30 and biz_date = '{ST9}', remain_principal,0)) as overdue30_remainPrin_user
    ,sum(if(overdue_days > 60 and biz_date = '{ST9}', remain_principal,0)) as overdue60_remainPrin_user
    ,sum(if(overdue_days > 90 and biz_date = '{ST9}', remain_principal,0)) as overdue90_remainPrin_user
    ,sum(if(biz_date = '{ST9}',first_term_over_principal,0)) as first_term_overduePrin
    ,sum(if(biz_date = '{ST9}',first_term_loan_amount,0))    as first_term_loan_amount
from
    dw${db_suffix}.dw_user_information_stat_d t1
where biz_date <= '${ST9}' 
    and biz_date >= last_day(add_months('${ST9}',-1))
    and product_id in (${product_id_list})
    group by
    product_id
) t2
on t1.product_id = t2.product_id
full join
(
select
    product_id
   ,sum(loan_amount)                                                                overdue3_contract_amount
   ,sum(if(should_repay_date <= date_sub(last_day('${ST9}'),7 ), loan_amount, 0))   overdue7_contract_amount
   ,sum(if(should_repay_date <= date_sub(last_day('${ST9}'),30), loan_amount, 0))   overdue30_contract_amount
   ,sum(if(should_repay_date <= date_sub(last_day('${ST9}'),90), loan_amount, 0))   overdue90_contract_amount
                                                                                
   ,sum(loan_num)                                                                   overdue3_contract_num 
   ,sum(if(should_repay_date <= date_sub(last_day('${ST9}'),7 ), loan_num, 0))      overdue7_contract_num 
   ,sum(if(should_repay_date <= date_sub(last_day('${ST9}'),30), loan_num, 0))      overdue30_contract_num
   ,sum(if(should_repay_date <= date_sub(last_day('${ST9}'),90), loan_num, 0))      overdue90_contract_num   
from
    dw${db_suffix}.dw_should_repay_summary
where 
    biz_date = last_day('${ST9}')
    and loan_term = 1
    and product_id in (${product_id_list})
    and should_repay_date <= date_sub(last_day('${ST9}'),3 )
group by product_id
) t3
on t1.product_id = t3.product_id
full join
(
select
    t3.product_id
    ,sum(if(overdue_days_max >= 1  and overdue_days_max <= 30 and should_repay_date <= last_day('${ST9}') and should_repay_date >= trunc('${ST9}','MM'),should_repay_principal,0 ))
     +sum(if(overdue_days_max >= 1  and overdue_days_max <= 30 and should_repay_date <= last_day(add_months('${ST9}',-1)) ,(should_repay_principal-paid_principal),0 )) as                           M1_should_repay_principal
     
    ,sum(if(overdue_days_max >= 31 and overdue_days_max <= 60 and should_repay_date <= last_day('${ST9}') and should_repay_date >= trunc('${ST9}','MM'),should_repay_principal,0 ))
     +sum(if(overdue_days_max >= 31 and overdue_days_max <= 60 and should_repay_date <= last_day(add_months('${ST9}',-1)) ,(should_repay_principal-paid_principal),0 )) as                           M2_should_repay_principal
     
    ,sum(if(overdue_days_max >= 61 and overdue_days_max <= 90 and should_repay_date <= last_day('${ST9}') and should_repay_date >= trunc('${ST9}','MM'),should_repay_principal,0 )) 
     +sum(if(overdue_days_max >= 61 and overdue_days_max <= 90 and should_repay_date <= last_day(add_months('${ST9}',-1)) ,(should_repay_principal-paid_principal),0 )) as                           M3_should_repay_principal
    
    ,sum(if(overdue_days_max >= 1  and overdue_days_max <= 30 and paid_out_date <= last_day('${ST9}') and paid_out_date >= trunc('${ST9}','MM'),paid_principal,0 )) as                               M1_repay_principal
    ,sum(if(overdue_days_max >= 31 and overdue_days_max <= 60 and paid_out_date <= last_day('${ST9}') and paid_out_date >= trunc('${ST9}','MM'),paid_principal,0 )) as                               M2_repay_principal
    ,sum(if(overdue_days_max >= 61 and overdue_days_max <= 90 and paid_out_date <= last_day('${ST9}') and paid_out_date >= trunc('${ST9}','MM'),paid_principal,0 )) as                               M3_repay_principal

from
(select
    t1.due_bill_no
    ,t2.product_id
    ,max(t2.overdue_days) as overdue_days_max
from
(select
    due_bill_no
    ,max(s_d_date) as s_d_date_max
    ,min(s_d_date) as s_d_date_min
    ,count(distinct s_d_date) as s_d_date_num
from
    ods${db_suffix}.loan_info
where 
(
    (trunc('${ST9}','MM') between s_d_date and date_sub(e_d_date, 1))
    or
    (last_day('${ST9}') between s_d_date and date_sub(e_d_date, 1))
)
    and loan_active_date <= last_day('${ST9}')
    and product_id in (${product_id_list})
group by
    due_bill_no
having count(distinct s_d_date) >= 2
) t1
join
    ods${db_suffix}.loan_info t2
on t1.due_bill_no = t2.due_bill_no
where t2.s_d_date >= t1.s_d_date_min
  and t2.s_d_date <= t1.s_d_date_max
  and t2.overdue_days > 0
  and t2.overdue_days <= 90
group 
by 
    t1.due_bill_no
    ,t2.product_id
) t3
join
(
select
    due_bill_no
    ,should_repay_date
    ,should_repay_principal
    ,paid_out_date
    ,paid_principal
from
    ods${db_suffix}.repay_schedule
where 
    last_day('${ST9}') between s_d_date and date_sub(e_d_date, 1)
    and product_id in (${product_id_list}) 
) t4
on t3.due_bill_no = t4.due_bill_no
group by
    t3.product_id
)  t4
on t1.product_id = t4.product_id
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
) as t5
on coalesce(t1.product_id,t2.product_id,t3.product_id,t4.product_id) = t5.product_id
;

--"借据范围：快照日在1月1日 到 1月31日之间的最大当前逾期天数在【1,30】天的借据。
--金额范围：12月31日的应还未还金额+在1月1日 到 1月31日的应还金额"
--"借据范围：快照日在1月1日 到 1月31日之间的最大当前逾期天数在【1,30】天的借据。
--金额范围：实还日在1月1日到1月31日的实还本金"





















