--set spark.executor.memory=4g;
--set spark.executor.memoryOverhead=4g;
--set spark.shuffle.memoryFraction=0.6;
--set spark.maxRemoteBlockSizeFetchToMem=200m;
--set hivevar:ST9=2020-07-03;
--create table dw_new.sit_dw_credit_approval_stat_day_temp_20201106_check as


invalidate metadata ods_new_s.credit_apply;
invalidate metadata ods_new_s.loan_apply;

select
a.credit_approval_date
,a.biz_date
,a.product_id
,(a.credit_approval_num               -  nvl(c.credit_approval_num              ,0 ) ) as diff_credit_approval_num             
,(a.credit_approval_num_count         -  nvl(b.credit_approval_num_count        ,0 ) ) as diff_credit_approval_num_count            
,(a.credit_approval_num_person        -  nvl(c.credit_approval_num_person       ,0 ) ) as diff_credit_approval_num_person      
,(a.credit_approval_num_person_count  -  nvl(b.credit_approval_num_person_count ,0 ) ) as diff_credit_approval_num_person_count
,(a.credit_approval_amount            -  nvl(c.credit_approval_amount           ,0 ) ) as diff_credit_approval_amount          
,(a.credit_approval_amount_count      -  nvl(b.credit_approval_amount_count     ,0 ) ) as diff_credit_approval_amount_count    
,(a.credit_loan_approval_num          -  nvl(d.credit_loan_approval_num         ,0 ) ) as diff_credit_loan_approval_num        
,(a.credit_loan_approval_person       -  nvl(d.credit_loan_approval_person      ,0 ) ) as diff_credit_loan_approval_person     
,(a.credit_loan_approval_num_amount   -  nvl(d.credit_loan_approval_num_amount  ,0 ) ) as diff_credit_loan_approval_num_amount 

from
(select
 t1.credit_approval_date                        as credit_approval_date
,nvl(t2.credit_approval_num                , 0) as credit_approval_num
,nvl(t1.credit_approval_num_count          , 0) as credit_approval_num_count
,nvl(t2.credit_approval_num_person         , 0) as credit_approval_num_person
,nvl(t1.credit_approval_num_person_count   , 0) as credit_approval_num_person_count
,nvl(t2.credit_approval_amount             , 0) as credit_approval_amount
,nvl(t1.credit_approval_amount_count       , 0) as credit_approval_amount_count
,t3.biz_date                                    as biz_date
,nvl(t3.credit_loan_approval_num           ,0)  as credit_loan_approval_num
,nvl(t3.credit_loan_approval_person        ,0)  as credit_loan_approval_person
,nvl(t3.credit_loan_approval_num_amount    ,0)  as credit_loan_approval_num_amount
,t1.product_id                                  as product_id
from
(  --求累计到观察日的授信统计
  select
    product_id          as product_id,
    '${var:ST9}'            as credit_approval_date,
    count(apply_id)     as credit_approval_num_count,
    count(user_hash_no) as credit_approval_num_person_count,
    sum(credit_amount)  as credit_approval_amount_count
  from (
    select distinct
      user_hash_no,
      apply_id,
      credit_amount,
      product_id
    from ods_new_s.credit_apply
    where 1 > 0
      and resp_code = '1'
      and to_date(credit_approval_time) <= '${var:ST9}'
  ) as tmp
  group by product_id
) t1
left join
( --观察日的授信情况
select
      product_id,
      credit_approval_date,
      count(apply_id)     as credit_approval_num,
      count(user_hash_no) as credit_approval_num_person,
      sum(credit_amount)  as credit_approval_amount
    from (
      select distinct
        user_hash_no,
        apply_id,
        '${var:ST9}' as credit_approval_date,
        credit_amount,
        product_id
      from ods_new_s.credit_apply
      where 1 > 0
        and resp_code = '1'
        and to_date(credit_approval_time) = '${var:ST9}'
    ) as credit_apply
    group by product_id,credit_approval_date
) t2
on 
t1.product_id = t2.product_id
left join
( --授信当天用信t-0，t-1，t-2，t-3 天的统计
select
d.observ_day
,d.biz_date
,d.product_id
,c.credit_loan_approval_num
,c.credit_loan_approval_person
,c.credit_loan_approval_num_amount
from
(select
 '${var:ST9}' as observ_day
 ,biz_date
 ,product_id
from 
dim_new.dim_date 
where 
biz_date between '${var:ST9}' and date_add('${var:ST9}',3)
) d
left join
(select
 b.product_id
,b.loan_approval_date
,count(b.apply_id)     as credit_loan_approval_num
,count(b.user_hash_no) as credit_loan_approval_person
,sum(b.loan_amount)    as credit_loan_approval_num_amount
from
(
select 
distinct
user_hash_no,
apply_id,
'${var:ST9}' as credit_approval_date,
credit_amount,
product_id
from ods_new_s.credit_apply
where 1 > 0
and resp_code = '1'
and credit_approval_time = '${var:ST9}'
) a
join
(
select 
distinct
user_hash_no,
apply_id,
to_date(issue_time) as loan_approval_date,
loan_amount,
product_id
from ods_new_s.loan_apply
where 1 > 0
and apply_status in (1,4) 
and to_date(issue_time) between '${var:ST9}' and date_add('${var:ST9}',3)
) b
on a.apply_id = b.apply_id
and a.product_id = b.product_id
group by
b.loan_approval_date
,b.product_id
) c
on c.loan_approval_date = d.biz_date
and c.product_id = d.product_id
) t3
on 
t1.credit_approval_date = t3.observ_day
and
t1.product_id = t3.product_id
) a
left join
(
select
    product_id          as product_id,
    '${var:ST9}'            as credit_approval_date,
    count(apply_id)     as credit_approval_num_count,
    count(user_hash_no) as credit_approval_num_person_count,
    sum(credit_amount)  as credit_approval_amount_count
  from (
    select distinct
      user_hash_no,
      apply_id,
      credit_amount,
      product_id
    from ods_new_s.credit_apply
    where 1 > 0
      and resp_code = '1'
      and to_date(credit_approval_time) <= '${var:ST9}'
  ) as tmp
  group by product_id
) b
on 
a.credit_approval_date = b.credit_approval_date
and
a.product_id = b.product_id
left join
(
select
      product_id,
      credit_approval_date,
      count(apply_id)     as credit_approval_num,
      count(user_hash_no) as credit_approval_num_person,
      sum(credit_amount)  as credit_approval_amount
    from (
      select distinct
        user_hash_no,
        apply_id,
        '${var:ST9}' as credit_approval_date,
        credit_amount,
        product_id
      from ods_new_s.credit_apply
      where 1 > 0
        and resp_code = '1'
        and to_date(credit_approval_time) = '${var:ST9}'
    ) as credit_apply
    group by product_id,credit_approval_date

) c
on 
a.credit_approval_date = c.credit_approval_date
and
a.product_id = c.product_id
left join
(
select
d.observ_day
,d.biz_date
,d.product_id
,c.credit_loan_approval_num
,c.credit_loan_approval_person
,c.credit_loan_approval_num_amount
from
(select
 '${var:ST9}' as observ_day
 ,biz_date
 ,product_id
from 
dim_new.dim_date 
where 
biz_date between '${var:ST9}' and date_add('${var:ST9}',3)
) d
left join
(select
 b.product_id
,b.loan_approval_date
,count(b.apply_id)     as credit_loan_approval_num
,count(b.user_hash_no) as credit_loan_approval_person
,sum(b.loan_amount)    as credit_loan_approval_num_amount
from
(
select 
distinct
user_hash_no,
apply_id,
'${var:ST9}' as credit_approval_date,
credit_amount,
product_id
from ods_new_s.credit_apply
where 1 > 0
and resp_code = '1'
and credit_approval_time = '${var:ST9}'
) a
join
(
select 
distinct
user_hash_no,
apply_id,
to_date(issue_time) as loan_approval_date,
loan_amount,
product_id
from ods_new_s.loan_apply
where 1 > 0
and apply_status in (1,4) 
and to_date(issue_time) between '${var:ST9}' and date_add('${var:ST9}',3)
) b
on a.apply_id = b.apply_id
and a.product_id = b.product_id
group by
b.loan_approval_date
,b.product_id
) c
on c.loan_approval_date = d.biz_date
and c.product_id = d.product_id
) d
on d.biz_date = a.biz_date
and d.product_id = a.product_id
;