set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
insert overwrite table ods${db_suffix}.repay_detail partition(biz_date,product_id)
select
a.due_bill_no                        as due_bill_no,
a.term                               as repay_term,
a.flow_sn                            as order_id,
a.receipt_type                       as repay_type,
case a.receipt_type
  when 'NORMAL'             then '用户正常还款'
  when 'PRE'                then '用户提前还款'
  when 'PRE_SETTLE'         then '用户提前结清'
  when 'COMP'               then '代偿还款'
  when 'RECOVER'            then '逾期追偿还款'
  when 'REFUND'             then '退票'
  when 'BUYBACK'            then '回购'
  when 'OVERDUE'            then '逾期还款'
  when 'REDUCE'             then '减免'
  when 'DISCOUNT'           then '商户贴息'
  when 'RECOVERY_REPAYMENT' then '降额还本'
  when 'INTEREST_REBATE'    then '降额退息'
  when 'RECEIVABLE'         then '退款回款'
  when 'INTERNAL_REFUND'    then '退款退息'
  end                                as repay_type_cn,
a.receipt_no                         as payment_id,
cast(a.repay_date as timestamp)      as txn_time,
''                                   as post_time,
a.fee_type                      as bnp_type,
case a.fee_type
  when 'Pricinpal'      then '本金'
  when 'Interest'       then '利息'
  when 'Penalty'        then '罚息'
  when 'COMMISSION'     then '佣金'
  when 'OVERDUE'        then '滞纳金'
  when 'FEE'            then '服务费'
  when 'OTHER'          then '其他'
  else a.fee_type end                as bnp_type_cn,
a.amount                             as repay_amount,
a.batch_date,
cast(a.created_date/1000 as bigint)*1000,
a.last_modified_date,
a.batch_date as biz_date,
a.product_no as product_id
from
(select * from stage.${tb_prefix}receipt_detail where d_date='${ST9}' and p_type='WS0013200001' ) a
left join
(select * from
(select
*,
row_number() over(partition by due_bill_no order by batch_date desc) rn from stage.loan_contract where d_date<='${ST9}' and p_type='WS0013200001') a
where a.rn=1
 ) b
on a.due_bill_no=b.due_bill_no and a.product_no=b.product_no
left join
(select * from stage.${tb_prefix}repayment_plan where d_date='${ST9}' and p_type='WS0013200001') c
on a.due_bill_no=c.due_bill_no and a.product_no=c.product_no and a.term=c.term
left join
(select * from stage.${tb_prefix}repayment_summary where d_date='${ST9}' and p_type='WS0013200001') d
on a.due_bill_no=d.due_bill_no and a.product_no=d.product_no
left join
(select * from stage.${tb_prefix}overdue_summary where d_date='${ST9}' and p_type='WS0013200001') e
on a.due_bill_no=e.due_bill_no and a.product_no=e.product_no and a.term=e.current_overdue_term;
