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




insert overwrite table ods.repay_detail partition(biz_date,product_id)
select
  repay_hst.due_bill_no                                                         as due_bill_no,
  repay_hst.term                                                                as repay_term,
  repay_hst.order_id                                                            as order_id,
  null                                                                          as repay_type,
  null                                                                          as repay_type_cn,
  repay_hst.payment_id                                                          as payment_id,
  cast(repay_hst.txn_date as timestamp)                                         as txn_time,
  null                                                                          as post_time,
  repay_hst.bnp_type                                                            as bnp_type,
  case repay_hst.bnp_type
  when 'Pricinpal'         then '本金'
  when 'Interest'          then '利息'
  when 'Penalty'           then '罚息'
  when 'TXNFee'            then '交易费'
  when 'TERMFee'           then '手续费'
  when 'SVCFee'            then '服务费'
  when 'LatePaymentCharge' then '滞纳金'
  when 'RepayFee'          then '实还费用'
  when 'EarlyRepayFee'     then '提前还款手续费'
  when 'OtherFee'          then '其它相关费用'
  when 'CardFee'           then '年费'
  when 'Mulct'             then '罚金'
  when 'Compensation'      then '赔偿金'
  when 'Damages'           then '违约金'
  when 'Compound'          then '复利'
  when 'OverLimitFee'      then '超限费'
  when 'NSFCharge'         then '资金不足罚金'
  when 'LifeInsuFee'       then '寿险计划包费'
  else repay_hst.bnp_type
  end                                                                           as bnp_type_cn,
  repay_hst.repay_amt                                                           as repay_amount,
  repay_hst.batch_date                                                          as batch_date,
  cast(datefmt(repay_hst.create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as create_time,
  cast(datefmt(repay_hst.lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
  repay_hst.biz_date                                                            as biz_date,
  ecas_loan.product_id                                                          as product_id
from (
  select
    repay_hst.payment_id,
    repay_hst.due_bill_no,
    repay_hst.bnp_type,
    repay_hst.repay_amt,
    repay_hst.batch_date,
    repay_hst.create_time,
    repay_hst.lst_upd_time,
    repay_hst.term,
    repay_hst.order_id,
    repay_hst.txn_date                                                                             as  txn_date,
    if(repay_hst_repair.due_bill_no is not null,repay_hst_repair.paid_out_date,repay_hst.txn_date) as  biz_date,
    repay_hst.d_date
  from (
    select * from stage.ecas_repay_hst where 1 > 0 and d_date = '${ST9}'
  ) as repay_hst
  left join (
    select distinct
      due_bill_no,
      order_id,
      paid_out_date
    from stage.schedule_repay_order_info_ddht
    where biz_date = date_sub(current_date,1)
  ) as repay_hst_repair
  on  repay_hst.due_bill_no = repay_hst_repair.due_bill_no
  and repay_hst.order_id    = repay_hst_repair.order_id
  where if(repay_hst_repair.due_bill_no is not null,repay_hst_repair.paid_out_date,repay_hst.txn_date) >= date_sub('${ST9}',20)
) as repay_hst
join (
  select
    product_code as product_id,
    due_bill_no
  from stage.ecas_loan
  where 1 > 0
    and d_date = '${ST9}'
    and product_code in (
      'DIDI201908161538',
      '001601',
      '001602',
      '001603',
      '001701',
      '001702',
      '002201',
      '002202',
      '002203',
      ''
    )
) as ecas_loan
on repay_hst.due_bill_no = ecas_loan.due_bill_no
-- limit 10
;
