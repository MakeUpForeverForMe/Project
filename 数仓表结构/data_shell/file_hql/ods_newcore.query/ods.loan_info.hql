set hive.support.quoted.identifiers=None;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;

create table if not exists ods${db_suffix}.loan_info_tmp like ods${db_suffix}.loan_info;

alter table ods${db_suffix}.loan_info_tmp drop partition(is_settled='no');

insert overwrite table ods${db_suffix}.loan_info_tmp partition(is_settled = 'no',product_id)
select
  `(is_settled)?+.+`
from ods${db_suffix}.loan_info
where 1 > 0
  and is_settled = 'no'
  and s_d_date < '${${ST9}}';

set hive.execution.engine=spark;

with ods_new_s_loan_tmp as (
select 
a.due_bill_no                                                              as due_bill_no,
a.apply_no                                                                 as apply_no,
a.actual_loan_date                                                         as loan_active_date,
a.total_term                                                               as loan_init_term,
a.principal                                                                as loan_init_principal,
a.interest                                                                 as loan_init_interest,
0                                                                          as loan_init_term_fee,
0                                                                          as loan_init_svc_fee,
b.current_term                                                             as loan_term,
0                                                                          as account_age,
b.current_term_due_date                                                    as should_repay_date,
b.return_term                                                              as loan_term_repaid,
a.total_term-b.return_term                                                 as loan_term_remain,
case b.asset_status
when 'NORMAL' then 'N'
when 'OVERDUE' then 'O'
when 'SETTLED' then 'F'
end                                                                        as loan_status,
case b.asset_status
when 'NORMAL' then '正常'
when 'OVERDUE' then '逾期'
when 'SETTLED' then '已还清'
end                                                                        as loan_status_cn,
''                                                                         as loan_out_reason,
b.settle_type                                                              as paid_out_type,
case b.settle_type  
  when 'NORMAL_SETTLE'     then '正常结清'
  when 'OVERDUE_SETTLE'    then '逾期结清'
  when 'PRE_SETTLE'        then '提前结清'
  when 'REFUND_SETTLE'     then '退票结清'
  when 'COMP_SETTLE'       then '逾期代偿结清'
  when 'RECOVER_SETTLE'    then '逾期追偿结清'
  when 'ASSET_REDEMPTION'  then '资产赎回'
  when 'ASSET_BUYBACK'     then '资产回购'
  when 'ASSET_VERIFY'      then '资产核销'                                 
  when 'RETURN_SETTLE'     then '退货结清'        
  else b.settle_type
end                                                                        as paid_out_type_cn,
b.settle_date                                                              as paid_out_date,
''                                                                         as terminal_date,
nvl(f.paid_amount,0)                                                      as paid_amount,
nvl(f.paid_prin,0)                                                     as paid_principal,
nvl(f.paid_int,0)                                                      as paid_interest,
nvl(f.paid_pena,0)                                                     as paid_penalty,
0                                                                      as paid_svc_fee,
0                                                                      as paid_term_fee,
0                                                                      as paid_mult,
if(b.asset_status='SETTLED',0,f.should_amount-nvl(f.paid_amount,0)-nvl(f.reduce_amount,0))         as remain_amount,
if(b.asset_status='SETTLED',0,f.should_prin-nvl(f.paid_prin,0)-nvl(f.reduce_prin,0))                   as remain_principal,
if(b.asset_status='SETTLED',0,f.should_int-nvl(f.paid_int,0)-nvl(f.reduce_int,0))                      as remain_interest,
0                                                                          as remain_svc_fee,
0                                                                          as remain_term_fee,
if(b.asset_status='SETTLED',0,f.should_pena-nvl(f.paid_pena,0)-nvl(f.reduce_pena,0))            as remain_othamounts,
d.current_overdue_principal                                                as overdue_principal,
d.current_overdue_interest                                                 as overdue_interest,
0                                                                          as overdue_svc_fee,
0                                                                          as overdue_term_fee,
d.overdue_penalty_amount-d.overdue_penalty_paid_amount                     as overdue_penalty,
0                                                                          as overdue_mult_amt,
d.first_overdue_date                                                       as overdue_date_first,
d.overdue_start_date                                                       as overdue_date_start,
if(d.current_overdue_days is null,0,d.current_overdue_days)                as overdue_days,
cast(date_add(d.overdue_start_date,cast(d.current_overdue_days as int) - 1) as string) as overdue_date,
d.overdue_start_date                                                       as dpd_begin_date,
if(d.current_overdue_days is null,0,d.current_overdue_days)                as dpd_days,
0                                                                          as dpd_days_count,
d.his_max_overdue_days                                                     as dpd_days_max,
''                                                                         as collect_out_date,
d.current_overdue_term                                                     as overdue_term,
d.overdue_terms_count                                                      as overdue_terms_count,
d.his_max_overdue_terms                                                    as overdue_terms_max,
d.his_total_overdue_principal                                              as overdue_principal_accumulate,
d.his_max_overdue_principal                                                as overdue_principal_max,
cast(datefmt(b.created_date, 'ms','yyyy-MM-dd HH:mm:ss') as timestamp)     as create_time,
cast(datefmt(b.last_modified_date, 'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
b.d_date                                                                   as s_d_date,
'3000-12-31'                                                               as e_d_date,
a.product_no                                                               as product_id 
from (select * from 
(select 
*,
row_number() over(partition by due_bill_no order by batch_date desc) rn from stage.loan_contract where d_date<='${ST9}' and p_type='WS0013200001') a
where a.rn=1
) a 
join 
(select 
 *
from stage.${tb_prefix}repayment_summary where d_date='${ST9}' and p_type='WS0013200001') b 
on a.due_bill_no=b.due_bill_no and a.product_no=b.product_no
left join(
select *
from 
(select
  *,
  row_number() over(partition by due_bill_no,product_no order by d_date desc) as rn
from stage.${tb_prefix}overdue_summary where d_date<='${ST9}' and p_type='WS0013200001') a 
where a.rn=1) d 
on a.due_bill_no=d.due_bill_no and a.product_no=d.product_no
left join
(
select 
 due_bill_no,
 product_no,
 sum(term_bill_amount)        as should_amount,
 sum(term_prin)               as should_prin,
 sum(term_int)                as should_int,
 sum(term_fee)                as should_fee,
 sum(term_penalty)            as should_pena,
 sum(term_repay_prin+term_repay_int+term_repay_fee+term_repay_penalty) as paid_amount,
 sum(term_repay_prin)         as paid_prin,
 sum(term_repay_int)          as paid_int,
 sum(term_repay_fee)          as paid_fee,
 sum(term_repay_penalty)      as paid_pena,
 sum(term_reduce_prin+term_reduce_int+term_reduce_fee+term_reduce_penalty) as reduce_amount,
 sum(term_reduce_prin)        as reduce_prin,
 sum(term_reduce_int)         as reduce_int,
 sum(term_reduce_fee)         as reduce_fee,
 sum(term_reduce_penalty)     as reduce_pena
from (
select a.* from 
(select a.* from 
(select
  *,
  row_number() over(partition by due_bill_no,product_no,term order by batch_date desc) as rn
from stage.${tb_prefix}repayment_plan where d_date<='${ST9}' and p_type='WS0013200001' ) a
where a.rn =1) a
left join
(
select 
due_bill_no,
  product_no,
  total_term from 
(select
  due_bill_no,
  product_no,
  total_term,
  row_number() over(partition by due_bill_no order by batch_date desc) as rn
from stage_sic.loan_contract where d_date<='${ST9}' and p_type='WS0013200001' ) a 
where a.rn=1
) b 
on a.due_bill_no=b.due_bill_no and b.product_no=b.product_no
where a.term<=b.total_term
) a
where a.rn=1
group by due_bill_no,product_no
) f
on a.due_bill_no=f.due_bill_no and a.product_no=f.product_no
)

insert overwrite table ods${db_suffix}.loan_info partition(is_settled='no',product_id)
select 
a.due_bill_no,
a.apply_no,
a.loan_active_date,
a.loan_init_term,
a.loan_init_principal,
a.loan_init_interest,
a.loan_init_term_fee,
a.loan_init_svc_fee,
a.loan_term,
a.account_age,
a.should_repay_date,
a.loan_term_repaid,
a.loan_term_remain,
a.loan_status,
a.loan_status_cn,
a.loan_out_reason,
a.paid_out_type,
a.paid_out_type_cn,
a.paid_out_date,
a.terminal_date,
a.paid_amount,
a.paid_principal,
a.paid_interest,
a.paid_penalty,
a.paid_svc_fee,
a.paid_term_fee,
a.paid_mult,  
a.remain_amount,
a.remain_principal,
a.remain_interest,
a.remain_svc_fee,
a.remain_term_fee,
a.remain_othamounts,
nvl(a.overdue_principal,0),
nvl(a.overdue_interest,0),
nvl(a.overdue_svc_fee,0),
nvl(a.overdue_term_fee,0),
nvl(a.overdue_penalty,0),
nvl(a.overdue_mult_amt,0),
a.overdue_date_first,
a.overdue_date_start,
a.overdue_days,
a.overdue_date,
a.dpd_begin_date,
a.dpd_days,
a.dpd_days_count,
a.dpd_days_max,
a.collect_out_date,
a.overdue_term,
a.overdue_terms_count,
a.overdue_terms_max,
a.overdue_principal_accumulate,
a.overdue_principal_max,
a.create_time,
a.update_time,
a.s_d_date,
if(b.due_bill_no is null,if('${ST9}'<a.e_d_date,'3000-12-31',a.e_d_date),if('${ST9}'<a.e_d_date,'${ST9}',a.e_d_date)),
a.product_id
from 
(select * from ods${db_suffix}.loan_info_tmp) a 
left join 
(select * from ods_new_s_loan_tmp) b 
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id 
union all 
select 
due_bill_no                  ,
apply_no                     ,
loan_active_date             ,
loan_init_term               ,
loan_init_principal          ,
loan_init_interest           ,
loan_init_term_fee           ,
loan_init_svc_fee            ,
loan_term                    ,
account_age                  ,
should_repay_date            ,
loan_term_repaid             ,
loan_term_remain             ,
loan_status                  ,
loan_status_cn               ,
loan_out_reason              ,
paid_out_type                ,
paid_out_type_cn             ,
paid_out_date                ,
terminal_date                ,
paid_amount                  ,
paid_principal               ,
paid_interest                ,
paid_penalty                 ,
paid_svc_fee                 ,
paid_term_fee                ,
paid_mult                    ,
remain_amount                ,
remain_principal             ,
remain_interest              ,
remain_svc_fee               ,
remain_term_fee              ,
remain_othamounts            ,
nvl(overdue_principal,0)     ,
nvl(overdue_interest,0)      ,
nvl(overdue_svc_fee,0)       ,
nvl(overdue_term_fee,0)      ,
nvl(overdue_penalty,0)       ,
nvl(overdue_mult_amt,0)      ,
overdue_date_first           ,
overdue_date_start           ,
overdue_days                 ,
overdue_date                 ,
dpd_begin_date               ,
dpd_days                     ,
dpd_days_count               ,
dpd_days_max                 ,
collect_out_date             ,
overdue_term                 ,
overdue_terms_count          ,
overdue_terms_max            ,
overdue_principal_accumulate ,
overdue_principal_max        ,
create_time                  ,
update_time                  ,
s_d_date                     ,
e_d_date                     ,
product_id                    from ods_new_s_loan_tmp;

