--create table if not exists eagle.eagle_loan_apply_stat_d(
--   project_id                           string             comment '项目ID'
--  ,loan_apply_num_person                decimal(11,0)      comment '当日用信申请用户数'
--  ,loan_approval_num_person             decimal(11,0)      comment '当日用信通过用户数' 
--  ,loan_approval_rate                   decimal(11,4)      comment '用信通过率(当日)--当日用信申请用户数/当日用信通过用户数'
--  ,second_apply_num_person              decimal(11,0)      comment '当日二审申请用户数'
--  ,second_approval_num_person           decimal(11,0)      comment '当日二审通过用户数'
--  ,second_approval_rate                 decimal(11,4)      comment '二审通过率(当日)--当日二审申请用户数/当日二审通过用户数'
--  ,loan_principal                       decimal(15,4)      comment '当日放款金额'
--  ,loan_num                             decimal(11,0)      comment '当日放款笔数'
--  ,avg_credit_amount                    decimal(15,4)      comment '平均用信金额(当日)--当日放款金额/当日放款笔数'
--  ,weig_interest_rate                   decimal(11,4)      comment '当日放款加权利率'
--  ,loan_apply_num_person_count          decimal(11,0)      comment '累计用信申请用户数'
--  ,loan_approval_num_person_count       decimal(11,0)      comment '累计用信通过用户数'
--  ,loan_approval_rate_count             decimal(11,4)      comment '用信通过率(累计)--累计用信申请用户数/累计用信通过用户数'
--  ,second_apply_num_person_count        decimal(11,0)      comment '累计二审申请用户数'
--  ,second_approval_num_person_count     decimal(11,0)      comment '累计二审通过用户数'
--  ,second_approval_rate_count           decimal(11,4)      comment '二审通过率(累计)--累计二审申请用户数/累计二审通过用户数'
--  ,loan_uesr_count                      decimal(15,0)      comment '累计放款用户数'
--  ,loan_principal_count                 decimal(15,4)      comment '累计放款金额'
--  ,loan_num_count                       decimal(15,0)      comment '累计放款笔数'
--  ,avg_loan_amount                      decimal(11,4)      comment '平均用信金额(累计)--累计放款金额/累计放款笔数'
--  ,weig_interest_rate_count             decimal(11,4)      comment '累计放款加权利率'
--  ,loan_apply_num                       decimal(11,0)      comment '当日用信申请订单数'
--  ,loan_approval_num                    decimal(11,0)      comment '当日用信通过订单数'
--  ,loan_apply_approval_rate             decimal(11,4)      comment '当日订单用信通过率'
--  ,second_apply_num                     decimal(11,0)      comment '当日二审申请订单数'
--  ,second_approval_num                  decimal(11,0)      comment '当日二审通过订单数'
--  ,second_approval_num_rate             decimal(11,4)      comment '当日二审订单通过率'
--  ,loan_apply_num_acc                   decimal(14,0)      comment '累计用信申请订单数'
--  ,loan_approval_num_acc                decimal(14,0)      comment '累计用信通过订单数'
--  ,loan_approval_acc_rate               decimal(11,4)      comment '用信通过率--累计用信通过订单数/累计用信申请订单数'
--  ,second_loan_apply_num_acc            decimal(14,0)      comment '累计二审申请订单数'
--  ,second_loan_approval_num_acc         decimal(14,0)      comment '累计二审通过订单数'
--  ,second_loan_approval_acc_rate        decimal(11,4)      comment '用信通过率--累计二审通过订单数/累计二审申请订单数'
--) comment '风控用信统计-日表'
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

--set hivevar:db_suffix=;
--set hivevar:ST9=2021-01-01;
set hivevar:car_product=('001601','001602','001603'); 
set hivevar:product_id_list='001601','001602','001603','001802','002001','001906','002006','001901','001801','002002','001902','DIDI201908161538','002501','002601','002602';
                            
insert overwrite table eagle.eagle_loan_apply_stat_d partition(biz_date='${ST9}',product_id)
select
    t3.project_id                                                                                                                                                       as project_id
    ,if(t3.product_id not in ${car_product},t1.loan_apply_num_person,0)                                                                                                 as loan_apply_num_person
    ,if(t3.product_id not in ${car_product},t1.loan_approval_num_person,0)                                                                                              as loan_approval_num_person
    
    ,nvl(if(t3.product_id not in ${car_product},t1.loan_approval_num_person,0)/if(t3.product_id not in ${car_product},loan_apply_num_person,0),0)                       as loan_approval_rate
    ,if(t3.product_id in ${car_product},t1.loan_apply_num_person,0)                                                                                                     as second_apply_num_person
    ,if(t3.product_id in ${car_product},t1.loan_approval_num_person,0)                                                                                                  as second_approval_num_person
    
    ,nvl(if(t3.product_id in ${car_product},t1.loan_approval_num_person,0)/if(t3.product_id in ${car_product},loan_apply_num_person,0),0)                               as second_approval_rate
    ,t2.loan_principal                                                                                                                                                  as loan_principal
    ,t2.loan_num                                                                                                                                                        as loan_num
    ,nvl(t2.loan_principal/t2.loan_num,0)                                                                                                                               as avg_credit_amount
    ,nvl(t1.weight_interest/t2.loan_principal,0)                                                                                                                        as weig_interest_rate
    ,if(t3.product_id not in ${car_product},t1.loan_apply_num_person_count,0)                                                                                           as loan_apply_num_person_count
    ,if(t3.product_id not in ${car_product},t1.loan_approval_num_person_count,0)                                                                                        as loan_approval_num_person_count
    
    ,nvl(if(t3.product_id not in ${car_product},t1.loan_approval_num_person_count,0)/if(t3.product_id not in ${car_product},t1.loan_apply_num_person_count,0) ,0)       as loan_approval_rate_count
    ,if(t3.product_id in ${car_product},t1.loan_apply_num_person_count,0)                                                                                               as second_apply_num_person_count
    ,if(t3.product_id in ${car_product},t1.loan_approval_num_person_count,0)                                                                                            as second_approval_num_person_count
    
    ,nvl(if(t3.product_id in ${car_product},t1.loan_approval_num_person_count,0)/if(t3.product_id in ${car_product},t1.loan_apply_num_person_count,0),0)                as second_approval_rate_count
    ,t1.loan_uesr_count                                                                                                                                                 as loan_uesr_count         
    ,t2.loan_principal_count                                                                                                                                            as loan_principal_count    
    ,t2.loan_num_count                                                                                                                                                  as loan_num_count          
    ,nvl(t2.loan_principal_count/t2.loan_num_count,0)                                                                                                                   as avg_loan_amount         
    ,nvl(t1.weight_interest_accumulate/t2.loan_principal_count,0)                                                                                                       as weig_interest_rate_count
    
    ,if(t3.product_id not in ${car_product},t1.loan_apply_num,0)                                                                                                        as loan_apply_num
    ,if(t3.product_id not in ${car_product},t1.loan_approval_num,0)                                                                                                     as loan_approval_num
    ,nvl(if(t3.product_id not in ${car_product},t1.loan_approval_num,0)/if(t3.product_id not in ${car_product},loan_apply_num,0),0)                                     as loan_apply_approval_rate
    ,if(t3.product_id in ${car_product},t1.loan_apply_num,0)                                                                                                            as second_apply_num
    ,if(t3.product_id in ${car_product},t1.loan_approval_num,0)                                                                                                         as second_approval_num
    ,nvl(if(t3.product_id in ${car_product},t1.loan_approval_num,0)/if(t3.product_id not in ${car_product},loan_apply_num,0),0)                                         as second_approval_num_rate
    ,if(t3.product_id not in ${car_product},t1.loan_apply_num_accumulate,0)                                                                                             as loan_apply_num_accumulate
    ,if(t3.product_id not in ${car_product},t1.loan_approval_num_accumulate,0)                                                                                          as loan_approval_num_accumulate
    ,nvl(if(t3.product_id not in ${car_product},t1.loan_approval_num_accumulate,0)/if(t3.product_id not in ${car_product},loan_apply_num_accumulate,0),0)               as loan_approval_acc_rate
    ,if(t3.product_id  in ${car_product},t1.loan_apply_num_accumulate,0)                                                                                                as second_loan_apply_num_acc
    ,if(t3.product_id  in ${car_product},t1.loan_approval_num_accumulate,0)                                                                                             as second_loan_approval_num_acc
    ,nvl(if(t3.product_id in ${car_product},t1.loan_approval_num_accumulate,0)/if(t3.product_id not in ${car_product},loan_apply_num_accumulate,0),0)                   as second_loan_approval_acc_rate
    ,t3.product_id                                                                                                                                                      as product_id
from
(
select
    product_id
    ,sum(if(loan_apply_num>0,1,0))                                    as loan_apply_num_person
    ,sum(if(loan_approval_num>0,1,0))                                 as loan_approval_num_person
    ,count(1)                                                         as loan_apply_num_person_count
    ,count(if(loan_approval_num_accumulate > 0, user_hash_no, null))  as loan_approval_num_person_count
    ,sum(if(loan_amount > 0, 1, 0))                                   as loan_uesr_count
    ,sum(weight_interest)                                             as weight_interest
    ,sum(weight_interest_accumulate)                                  as weight_interest_accumulate
    
    ,sum(loan_apply_num)                                              as loan_apply_num
    ,sum(loan_approval_num)                                           as loan_approval_num
    ,sum(loan_apply_num_accumulate)                                   as loan_apply_num_accumulate
    ,sum(loan_approval_num_accumulate)                                as loan_approval_num_accumulate
from
    dw.dw_user_information_stat_d
where
    biz_date = '${ST9}'
group by product_id
) t1
full join
(
select
    product_id
    ,sum(loan_num_count)                                          as loan_num_count
    ,sum(loan_principal_count)                                    as loan_principal_count
    ,sum(loan_principal)                                          as loan_principal
    ,sum(loan_num)                                                as loan_num
from
    dw.dw_loan_base_stat_loan_num_day
where biz_date = '${ST9}'
  and product_id in (${product_id_list})
GROUP BY
    product_id
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
on coalesce(t1.product_id,t2.product_id) = t3.product_id
;