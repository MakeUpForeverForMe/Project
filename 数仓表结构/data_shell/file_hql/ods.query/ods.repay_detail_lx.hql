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


-- set hivevar:ST9=2020-07-10; -- 跑全量数据后，单独 --- 去重 --- 跑 2020-07-10 这一天的数据。

-- set hivevar:db_suffix=;set hivevar:tb_suffix=_asset;
-- set hivevar:db_suffix=_cps;set hivevar:tb_suffix=;

-- set hivevar:ST9=2020-10-02;

set hivevar:p_type='lx','lxzt','lx2','lx3';


insert overwrite table ods${db_suffix}.repay_detail partition(biz_date,product_id)
select
  repay_hst_today.due_bill_no                    as due_bill_no,
  repay_hst_today.term                           as repay_term,
  repay_hst_today.order_id                       as order_id,
  null                                           as repay_type,
  null                                           as repay_type_cn,
  repay_hst_today.payment_id                     as payment_id,
  cast(repay_hst_today.txn_date as timestamp)    as txn_time,
  null                                           as post_time,
  repay_hst_today.bnp_type                       as bnp_type,
  case repay_hst_today.bnp_type
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
  else repay_hst_today.bnp_type
  end                                            as bnp_type_cn,
  repay_hst_today.repay_amt                      as repay_amt,
  repay_hst_today.batch_date                     as batch_date,
  cast(repay_hst_today.create_time as timestamp) as create_time,
  cast(repay_hst_today.update_time as timestamp) as update_time,
  repay_hst_today.d_date                         as biz_date,
  repay_hst_today.product_id                     as product_id
from (
  select
    repay.due_bill_no,
    repay.term,
    repay.order_id,
    repay.payment_id,
    repay.txn_date,
    repay.bnp_type,
    repay.repay_amt,
    repay.batch_date,
    repay.create_time,
    repay.update_time,
    repay.d_date,
    ecas_loan.product_id
  from (
    select
      due_bill_no,
      term,
      order_id,
      payment_id,
      txn_date,
      bnp_type,
      repay_amt,
      batch_date,
      datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')  as create_time,
      datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
      d_date
    from stage.ecas_repay_hst${tb_suffix}
    where 1 > 0
      --and d_date <= to_date(current_timestamp())
      and d_date = '${ST9}'
      and if('${tb_suffix}' = '_asset',true,
        payment_id not in ( -- 代偿后  在 2020-07-10 到 2020-07-13 这四天有六笔，最新分区只有一笔。
          '000015945007161admin000083000002',
          '000015945007161admin000083000003',
          '000015945007161admin000083000004',
          '000015945007161admin000083000005',
          '000015945007161admin000083000006'
        )
      )
  ) as repay
  join (
    select distinct
      product_code as product_id,
      due_bill_no
    from stage.ecas_loan${tb_suffix}
    where 1 > 0
      and d_date = '${ST9}'
      and p_type in (${p_type})
  ) as ecas_loan
  on repay.due_bill_no = ecas_loan.due_bill_no
) as repay_hst_today
left join (
  select
    repay.due_bill_no,
    repay.term,
    repay.order_id,
    repay.payment_id,
    repay.txn_date,
    repay.bnp_type,
    repay.repay_amt,
    repay.batch_date,
    repay.create_time,
    repay.update_time,
    repay.d_date,
    ecas_loan.product_id
  from (
    select
      due_bill_no,
      term,
      order_id,
      payment_id,
      txn_date,
      bnp_type,
      repay_amt,
      batch_date,
      datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss')  as create_time,
      datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as update_time,
      d_date
    from stage.ecas_repay_hst${tb_suffix}
    where 1 > 0
      and d_date = date_sub('${ST9}',1)
      and if('${tb_suffix}' = '_asset',true,
        payment_id not in ( -- 代偿后  在 2020-07-10 到 2020-07-13 这四天有六笔，最新分区只有一笔。
          '000015945007161admin000083000002',
          '000015945007161admin000083000003',
          '000015945007161admin000083000004',
          '000015945007161admin000083000005',
          '000015945007161admin000083000006'
        )
      )
  ) as repay
  join (
    select
      product_code as product_id,
      due_bill_no
    from stage.ecas_loan${tb_suffix}
    where 1 > 0
      and d_date = date_sub('${ST9}',1)
      and p_type in (${p_type})
  ) as ecas_loan
  on repay.due_bill_no = ecas_loan.due_bill_no
) as repay_hst_yesterday
on  nvl(repay_hst_today.product_id,  'a') = nvl(repay_hst_yesterday.product_id,  'a')
and nvl(repay_hst_today.due_bill_no, 'a') = nvl(repay_hst_yesterday.due_bill_no, 'a')
and nvl(repay_hst_today.term,        0  ) = nvl(repay_hst_yesterday.term,        0  )
and nvl(repay_hst_today.order_id,    'a') = nvl(repay_hst_yesterday.order_id,    'a')
and nvl(repay_hst_today.payment_id,  'a') = nvl(repay_hst_yesterday.payment_id,  'a')
and nvl(repay_hst_today.txn_date,    'a') = nvl(repay_hst_yesterday.txn_date,    'a')
and nvl(repay_hst_today.bnp_type,    'a') = nvl(repay_hst_yesterday.bnp_type,    'a')
and cast(nvl(repay_hst_today.repay_amt,0) as decimal(30,10)) = cast(nvl(repay_hst_yesterday.repay_amt,0) as decimal(30,10))
where repay_hst_yesterday.payment_id is null and repay_hst_yesterday.order_id is null
-- order by due_bill_no,biz_date,repay_term,payment_id
-- limit 10
;
