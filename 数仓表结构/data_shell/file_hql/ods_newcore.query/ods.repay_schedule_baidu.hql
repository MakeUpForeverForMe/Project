set hive.support.quoted.identifiers=None;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
create table if not exists ods${db_suffix}.repay_schedule_tmp like ods${db_suffix}.repay_schedule;
insert overwrite table ods${db_suffix}.repay_schedule_tmp partition(is_settled = 'no',product_id)
select
  `(is_settled)?+.+`
from ods${db_suffix}.repay_schedule
where 1 > 0
  and is_settled = 'no'
  and product_id in (${product_id})
  and s_d_date < '${ST9}';

with ods_new_s_repay_schedule_tmp as (
select
a.due_bill_no                                                                   as due_bill_no,
a.actual_loan_date                                                              as loan_active_date,
a.principal                                                                     as loan_init_principal,
a.total_term                                                                    as loan_init_term,
b.term                                                                          as loan_term,
b.term_start_date                                                               as start_interest_date,
if(b.term_status='REPAID',0,b.term_bill_amount-b.term_reduce_prin-b.term_reduce_int
-b.term_reduce_fee-b.term_reduce_penalty-b.term_repay_prin-b.term_repay_int
-b.term_repay_fee-b.term_repay_penalty)                                         as curr_bal,
b.term_due_date                                                                 as should_repay_date,
b.term_due_date                                                                 as should_repay_date_history,
b.term_grace_date                                                               as grace_date,
b.term_bill_amount                                                              as should_repay_amount,
b.term_prin                                                                     as should_repay_principal,
b.term_fee                                                                      as should_repay_interest,
0                                                                               as should_repay_term_fee,
0                                                                               as should_repay_svc_fee,
b.term_penalty                                                                  as should_repay_penalty,
0                                                                               as should_repay_mult_amt,
0                                                                               as should_repay_penalty_acru,
case b.term_status
when 'UNDUE' then 'N'
when 'REPAID' then 'F'
when 'OVERDUE' then 'O'
end                                                                             as schedule_status,
case b.term_status
when 'UNDUE' then '正常'
when 'REPAID' then '已还清'
when 'OVERDUE' then '逾期'
end                                                                             as schedule_status_cn,
''                                                                              as repay_status,
b.repay_date                                                                    as paid_out_date,
b.term_paid_out_type                                                            as paid_out_type,
case b.term_paid_out_type
  when 'NORMAL_PAIDOUT' then '正常结清'
  when 'OVERDUE_PAIDOUT' then '逾期结清'
  when 'PRE_PAIDOUT' then '提前结清'
  else b.term_paid_out_type
end                                                                             as paid_out_type_cn,
b.term_repay_prin+b.term_repay_fee+b.term_repay_int+b.term_repay_penalty       as paid_amount,
b.term_repay_prin                                                              as paid_principal,
b.term_repay_fee                                                               as paid_interest,
0                                                                              as paid_term_fee,
0                                                                              as paid_svc_fee,
b.term_repay_penalty                                                           as paid_penalty,
0                                                                              as paid_mult,
b.term_reduce_prin+b.term_reduce_int+b.term_reduce_fee+b.term_reduce_penalty    as reduce_amount,
b.term_reduce_prin                                                              as reduce_principal,
b.term_reduce_fee                                                               as reduce_interest,
0                                                                               as reduce_term_fee,
0                                                                               as reduce_svc_fee,
b.term_reduce_penalty                                                           as reduce_penalty,
0                                                                               as reduce_mult_amt,
b.d_date                                                                        as effective_date,
cast(datefmt(b.created_date, 'ms','yyyy-MM-dd HH:mm:ss') as timestamp)          as create_time,
cast(datefmt(b.last_modified_date, 'ms','yyyy-MM-dd HH:mm:ss') as timestamp)    as update_time,
b.d_date                                                                        as s_d_date,
'3000-12-31'                                                                    as e_d_date,
a.product_no                                                                    as product_id
from (select
 due_bill_no,
 apply_no,
 actual_loan_date,
 principal,
 total_term,
 product_no,
 interest_rate
from (select
*,
row_number() over(partition by due_bill_no order by batch_date desc) rn from stage.loan_contract where d_date<='${ST9}' and p_type='WS0012200001') a
where a.rn=1 ) a
join
(select
 *
from stage.${tb_prefix}repayment_plan where d_date='${ST9}' and p_type='WS0012200001') b
on a.due_bill_no=b.due_bill_no and a.product_no=b.product_no
left join
(select
  sum(if(remark like '减免%',0,amount)) paid_amount,
  sum(if(fee_type = 'Pricinpal',amount,0)) as paid_pric,
  sum(if(fee_type = 'Interest',amount,0)) as paid_int,
  sum(if(fee_type = 'SVCFee' and remark!='减免服务费' ,amount,0)) as paid_fee,
  sum(if(fee_type = 'Penalty',amount,0)) as paid_penalty,
  sum(if(fee_type = 'Mulct',amount,0)) as paid_mult,
  due_bill_no,
  term,
  product_no
from stage.${tb_prefix}receipt_detail where d_date<='${ST9}' and p_type='WS0012200001' and term !=999 and receipt_type !='REDUCE'
group by due_bill_no,
         term,
         product_no ) c
on b.due_bill_no=c.due_bill_no and b.product_no=c.product_no and b.term=c.term
)

insert overwrite table ods${db_suffix}.repay_schedule partition(is_settled = 'no',product_id)
select
a.due_bill_no,
a.loan_active_date,
a.loan_init_principal,
a.loan_init_term,
a.loan_term,
a.start_interest_date,
a.curr_bal,
a.should_repay_date,
a.should_repay_date_history,
a.grace_date,
a.should_repay_amount,
a.should_repay_principal,
a.should_repay_interest,
a.should_repay_term_fee,
a.should_repay_svc_fee,
a.should_repay_penalty,
a.should_repay_mult_amt,
a.should_repay_penalty_acru,
a.schedule_status,
a.schedule_status_cn,
a.repay_status,
a.paid_out_date,
a.paid_out_type,
a.paid_out_type_cn,
a.paid_amount,
a.paid_principal,
a.paid_interest,
a.paid_term_fee,
a.paid_svc_fee,
a.paid_penalty,
a.paid_mult,
a.reduce_amount,
a.reduce_principal,
a.reduce_interest,
a.reduce_term_fee,
a.reduce_svc_fee,
a.reduce_penalty,
a.reduce_mult_amt,
a.effective_date,
a.create_time,
a.update_time,
a.s_d_date,
if(b.due_bill_no is null,if('${ST9}'<a.e_d_date,if(a.loan_term>nvl(c.total_term,9999),'${ST9}','3000-12-31'),a.e_d_date),if('${ST9}'<a.e_d_date,'${ST9}',a.e_d_date)),
a.product_id
from
(select * from ods${db_suffix}.repay_schedule_tmp) a
left join
(select * from ods_new_s_repay_schedule_tmp) b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id and a.loan_term=b.loan_term
left join
(select * from stage.loan_contract where d_date='${ST9}' and p_type='WS0012200001') c
on a.due_bill_no=c.due_bill_no and a.product_id=c.product_no
union all
select
due_bill_no,
loan_active_date,
loan_init_principal,
loan_init_term,
loan_term,
start_interest_date,
curr_bal,
should_repay_date,
should_repay_date_history,
grace_date,
should_repay_amount,
should_repay_principal,
should_repay_interest,
should_repay_term_fee,
should_repay_svc_fee,
should_repay_penalty,
should_repay_mult_amt,
should_repay_penalty_acru,
schedule_status,
schedule_status_cn,
repay_status,
paid_out_date,
paid_out_type,
paid_out_type_cn,
paid_amount,
paid_principal,
paid_interest,
paid_term_fee,
paid_svc_fee,
paid_penalty,
paid_mult,
reduce_amount,
reduce_principal,
reduce_interest,
reduce_term_fee,
reduce_svc_fee,
reduce_penalty,
reduce_mult_amt,
effective_date,
create_time,
update_time,
s_d_date,
e_d_date,
product_id from ods_new_s_repay_schedule_tmp;
