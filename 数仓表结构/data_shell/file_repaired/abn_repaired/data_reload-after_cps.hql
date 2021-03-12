set spark.app.name=hive_table_cps;
set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;         -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.support.quoted.identifiers=None;     -- 设置可以使用正则表达式查找字段
set hive.groupby.orderby.position.alias=true; -- 设置 Hive 可以使用 group by 1,2,3
set hive.auto.convert.join=false;             -- 关闭自动 MapJoin
set hive.mapjoin.optimized.hashtable=false;
set hive.mapjoin.followby.gby.localtask.max.memory.usage=0.9;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;

set hivevar:p_type=lx2;
set hivevar:ST9=2020-10-08;


alter table ods.ecas_repay_hst partition(d_date = '2020-09-30',p_type = '${p_type}') rename to partition(d_date = '9999-09-30',p_type = '${p_type}');
alter table ods.ecas_repay_hst partition(d_date = '2020-10-01',p_type = '${p_type}') rename to partition(d_date = '9999-10-01',p_type = '${p_type}');
alter table ods.ecas_repay_hst partition(d_date = '2020-10-02',p_type = '${p_type}') rename to partition(d_date = '9999-10-02',p_type = '${p_type}');
alter table ods.ecas_repay_hst partition(d_date = '2020-10-03',p_type = '${p_type}') rename to partition(d_date = '9999-10-03',p_type = '${p_type}');
alter table ods.ecas_repay_hst partition(d_date = '2020-10-04',p_type = '${p_type}') rename to partition(d_date = '9999-10-04',p_type = '${p_type}');
alter table ods.ecas_repay_hst partition(d_date = '2020-10-05',p_type = '${p_type}') rename to partition(d_date = '9999-10-05',p_type = '${p_type}');
alter table ods.ecas_repay_hst partition(d_date = '2020-10-06',p_type = '${p_type}') rename to partition(d_date = '9999-10-06',p_type = '${p_type}');
alter table ods.ecas_repay_hst partition(d_date = '2020-10-07',p_type = '${p_type}') rename to partition(d_date = '9999-10-07',p_type = '${p_type}');


set hivevar:month_day=10-07;

insert overwrite table ods.ecas_repay_hst partition(d_date = '2020-${month_day}',p_type = '${p_type}')
select
  b.payment_id,
  a.due_bill_no,
  a.acct_nbr,
  a.acct_type,
  a.bnp_type,
  a.repay_amt,
  a.batch_date,
  a.create_time,
  a.create_user,
  a.lst_upd_time,
  a.lst_upd_user,
  a.jpa_version,
  a.term,
  a.org,
  b.order_id,
  a.txn_seq,
  a.txn_date,
  b.overdue_days,
  b.loan_status
from (
  select * from ods.ecas_repay_hst where d_date = '9999-${month_day}' and p_type='${p_type}'
) as a
join (
  select * from ods.ecas_repay_hst where d_date = '${ST9}' and p_type = '${p_type}'
) as b
on  a.due_bill_no = b.due_bill_no
and a.term        = b.term
and a.bnp_type    = b.bnp_type
and a.batch_date  = b.batch_date
-- limit 10
;


select count(1) from ods.ecas_repay_hst where d_date = '9999-${month_day}' and p_type = '${p_type}'
union all
select count(1) from ods.ecas_repay_hst where d_date = '2020-${month_day}' and p_type = '${p_type}'
;
