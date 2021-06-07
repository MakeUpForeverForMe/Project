set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
insert overwrite table ods${db_suffix}.order_info partition(biz_date,product_id)
select
a.flow_sn                 as order_id,
b.apply_no                as apply_no,
a.due_bill_no,
c.term,
a.trans_gateway           as pay_channel,
''                        as command_type,
case a.trans_status
when 'SUCCESS' then 'S'
end                       as order_status,
a.online_flag             as repay_way,
a.trans_amount            as txn_amt,
a.trans_amount            as success_amt,
a.currency                as currency,
a.batch_date              as business_date,
case a.trans_flow_type
when 'REPAY' then 'N'
when 'RECOVER' then 'F'
when 'REFUND' then 'T'
when 'BUYBACK' then 'B'
when 'COMP' then 'D'
else a.trans_flow_type
end                       as loan_usage,
case a.trans_flow_type
when 'REPAY' then 'N'
when 'RECOVER' then 'F'
when 'REFUND' then 'T'
when 'BUYBACK' then 'B'
when 'COMP' then 'D'
else a.trans_flow_type
end                       as purpose,
a.bank_account_no         as bank_trade_act_no,
a.bank_account_name       as bank_trade_act_name,
''                        as bank_trade_act_phone,
a.trans_time              as txn_time,
from_unixtime(cast(a.trans_time/1000 as bigint),'yyyy-MM-dd')              as txn_date,
from_unixtime(cast(a.created_date/1000 as bigint),'yyyy-MM-dd HH:mm:ss')            as create_time,
from_unixtime(cast(a.last_modified_date/1000 as bigint),'yyyy-MM-dd HH:mm:ss')      as update_time,
a.batch_date              as biz_date,
a.product_no              as product_id
from
(
select
 project_no,
 product_no,
 flow_sn,
 due_bill_no,
 trans_flow_type,
 bank_trans_no,
 sum(trans_amount) trans_amount,
 currency,
 bank_account_name,
 bank_account_no,
 trans_time,
 trans_status,
 remark,
 batch_date,
 created_date,
 last_modified_date,
 flow_owner,
 trans_gateway,
 is_confirmed,
 online_flag
from stage.repay_trans_flow where batch_date='${ST9}' and p_type='WS0012200001'
group by project_no,product_no,flow_sn,due_bill_no,trans_flow_type,
bank_trans_no,currency,bank_account_name,bank_account_no,trans_time,
trans_status,remark,batch_date,created_date,last_modified_date,flow_owner,
trans_gateway,is_confirmed,online_flag) a
left join
(select * from
(select
*,
row_number() over(partition by due_bill_no order by batch_date desc) rn from stage.loan_contract where d_date<='${ST9}' and p_type='WS0012200001') a
where a.rn=1
) b
on a.due_bill_no=b.due_bill_no and a.product_no=b.product_no
left join
(select max(term) term,product_no,due_bill_no,flow_sn from stage.${tb_prefix}receipt_detail where d_date<='${ST9}' and p_type='WS0012200001'
group by product_no,due_bill_no,flow_sn) c
on a.due_bill_no=c.due_bill_no and a.product_no=c.product_no and a.flow_sn=c.flow_sn
union all
select
a.flow_sn                 as order_id,
b.apply_no                as apply_no,
a.due_bill_no,
b.total_term              as term,
'放款-银企直连'           as pay_channel,
''                        as command_type,
case a.trans_status
when 'SUCCESS' then 'S'
end                       as order_status,
''                        as repay_way,
a.trans_amount            as txn_amt,
a.trans_amount            as success_amt,
a.currency                as currency,
a.batch_date              as business_date,
'L'                       as loan_usage,
'L'                    as purpose,
a.bank_account_no         as bank_trade_act_no,
a.bank_account_name       as bank_trade_act_name,
''                        as bank_trade_act_phone,
a.trans_time              as txn_time,
from_unixtime(cast(a.trans_time/1000 as bigint),'yyyy-MM-dd')              as txn_date,
from_unixtime(cast(a.created_date/1000 as bigint), 'yyyy-MM-dd HH:mm:ss')            as create_time,
from_unixtime(cast(a.last_modified_date/1000 as bigint),'yyyy-MM-dd HH:mm:ss')       as update_time,
a.batch_date              as biz_date,
b.product_no              as product_id
from
(
select
 project_no,
 product_no,
 flow_sn,
 due_bill_no,
 bank_trans_no,
 trans_amount,
 currency,
 bank_account_name,
 bank_account_no,
 trans_time,
 trans_status,
 remark,
 batch_date,
 created_date,
 last_modified_date
from stage.loan_trans_flow where batch_date='${ST9}' and p_type='WS0012200001') a
left join
(select * from
(select
*,
row_number() over(partition by due_bill_no order by batch_date desc) rn from stage.loan_contract where d_date<='${ST9}' and p_type='WS0012200001') a
where a.rn=1
) b
on a.due_bill_no=b.due_bill_no and a.product_no=b.product_no