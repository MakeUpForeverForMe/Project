--create table if not exists eagle.eagle_loan_apply_stat_m(
--     project_id                           string             comment '项目ID'
--    ,loan_apply_num_person                decimal(11,0)      comment '当月用信申请用户数'
--    ,loan_approval_num_person             decimal(11,0)      comment '当月用信通过用户数' 
--    ,loan_approval_rate                   decimal(11,4)      comment '用信通过率(当月)--当月用信申请用户数/当月用信通过用户数'
--
--    ,loan_principal                       decimal(15,4)      comment '当月放款金额'
--    ,loan_num                             decimal(11,0)      comment '当月放款笔数'
--    ,avg_credit_amount                    decimal(15,4)      comment '平均用信金额(当月)--当月放款金额/当月放款笔数'
--    ,weig_interest_rate                   decimal(11,4)      comment '当月放款加权利率'
--    ,monthly_new_users                    decimal(8,0)       comment '月新增放款用户数'
--
--    ,loan_apply_num                       decimal(11,0)      comment '当月用信申请订单数'
--    ,loan_approval_num                    decimal(11,0)      comment '当月用信通过订单数'
--    ,loan_apply_approval_rate             decimal(11,4)      comment '当月订单用信通过率'
--
--) comment '风控用信统计-月表'
--partitioned by (biz_date string comment '观察日',product_id string comment '产品号')
--stored as parquet;
--set hivevar:ST9=2021-02-28;
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

--set hivevar:car_product=('001601','001602','001603'); 
set hivevar:product_id_list='001601','001602','001603','001802','002001','001906','002006','001901','001801','002002','001902','DIDI201908161538','002501','002601','002602';
insert overwrite table eagle.eagle_loan_apply_stat_m partition(biz_date,product_id)

select
    t3.project_id                                                                                                                                        as project_id
    ,nvl(t1.loan_apply_num_person,0)                                                                                                                     as loan_apply_num_person
    ,nvl(t1.loan_approval_num_person,0)                                                                                                                  as loan_approval_num_person
    ,nvl(t1.loan_approval_num_person/t1.loan_apply_num_person,0)                                                                                         as loan_approval_rate

    ,nvl(t2.loan_principal,0)                                                                                                                            as loan_principal
    ,nvl(t2.loan_num,0)                                                                                                                                  as loan_num
    ,nvl(t2.loan_principal/t2.loan_num,0)                                                                                                                as avg_credit_amount
    ,nvl(t1.weight_interest/t2.loan_principal,0)                                                                                                         as weig_interest_rate
    ,nvl(this_month_loan_accmul-last_month_loan_accmul,0)                                                                                                as monthly_new_users

    ,nvl(t1.loan_apply_num,0)                                                                                                                            as loan_apply_num
    ,nvl(t1.loan_approval_num,0)                                                                                                                         as loan_approval_num
    ,nvl(t1.loan_approval_num/t1.loan_apply_num,0)                                                                                                       as loan_apply_approval_rate


    ,last_day('${ST9}')                                                                                                                                  as biz_date
    ,t3.product_id                                                                                                                                       as product_id
from
(
select
    product_id
    ,count( distinct (if(substr('${ST9}',0,7)=substr(biz_date, 0, 7) and loan_apply_num>0   ,user_hash_no, null)))           as loan_apply_num_person
    ,count( distinct (if(substr('${ST9}',0,7)=substr(biz_date, 0, 7) and loan_approval_num>0,user_hash_no,null)))            as loan_approval_num_person
    ,sum(if(substr('${ST9}',0,7)=substr(biz_date, 0, 7),weight_interest,0))                                                  as weight_interest
    ,count(distinct (if(biz_date=last_day(add_months('${ST9}',-1)) and loan_num > 0, user_hash_no,null )))                   as last_month_loan_accmul
    ,count(distinct (if(biz_date=last_day('${ST9}') and loan_num > 0, user_hash_no,null )))                                  as this_month_loan_accmul
    
    ,sum(loan_apply_num)                                              as loan_apply_num
    ,sum(loan_approval_num)                                           as loan_approval_num
from
    dw.dw_user_information_stat_d
where
    1>0
    and biz_date >= last_day(add_months('${ST9}',-1))
    and biz_date <= last_day('${ST9}')
group by product_id
) t1
full join
(
select
    product_id
    ,sum(loan_principal)                                          as loan_principal
    ,sum(loan_num)                                                as loan_num
from
    dw.dw_loan_base_stat_loan_num_day
where 
    1>0
    and substr(biz_date,0,7) = substr('${ST9}',0,7)
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
