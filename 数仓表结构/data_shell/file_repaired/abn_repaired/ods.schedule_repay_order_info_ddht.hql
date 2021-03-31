set hive.support.quoted.identifiers=None;
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

--create table if not exists ods.schedule_repay_order_info_ddht
--(
-- due_bill_no     string              comment '借据号'
--,order_id        string              comment '订单编号'
--,term            decimal(5,0)        comment '期数'
--,paid_out_date   string              comment '还款计划上结清日期'
--,txn_date        string              comment '实还明细上实还日期'
--,txn_amt         decimal(10,4)       comment '实还明细上交易金额'
--) COMMENT '还款计划和实还明细实还时间对照临时表'
--partitioned by (biz_date string COMMENT '观察日期',product_id string COMMENT '产品编号')
--stored as parquet;
--set hivevar:ST9=2021-03-29;
set hivevar:p_types='ddht','htgy';
set hivevar:product_id_list='001601','001602','001603','002201','002202','002203';

insert overwrite table stage.schedule_repay_order_info_ddht partition(biz_date = '${ST9}',product_id)
select
 t1.due_bill_no
,t2.order_id
,t1.curr_term as term
,t1.paid_out_date
,t2.txn_date
,t2.txn_amt
,t1.product_id
from
(select
due_bill_no
,curr_term
,paid_out_date
,product_code as product_id
from
stage.ecas_repay_schedule
where 
product_code in (${product_id_list})
and d_date = '${ST9}'
and p_type in(${p_types})
and paid_out_date is not null
and due_bill_no not in ('1000004836')
) t1
left join
(
select
order_id
,due_bill_no
,term
,txn_date
,sum(repay_amt) as txn_amt
from
stage.ecas_repay_hst
where
d_date = '${ST9}'
and p_type in(${p_types})
and due_bill_no not in ('1000004836')
group by
order_id
,due_bill_no
,term
,txn_date
) t2
on
t1.due_bill_no = t2.due_bill_no
and t1.curr_term = t2.term
where
    t1.curr_term != 0
and t2.term != 0
and t1.paid_out_date != t2.txn_date
;  