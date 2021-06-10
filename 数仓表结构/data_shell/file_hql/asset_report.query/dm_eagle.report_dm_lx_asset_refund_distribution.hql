set var:product_id='001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';

insert overwrite table dm_eagle.report_dm_lx_asset_refund_distribution partition(snapshot_date,project_id)
select
mold,
content,
cast(contact_amount as decimal(12,2)) as contact_amount,
cast(assets_sum as int ) as assets_sum,
snapshot_date,
b.project_id
from 
(select
'地区' as mold,
d.idcard_province as content,
a.product_id,
cast(sum(loan_init_principal) as decimal(12,2)) as contact_amount,
cast(count(0) as int) as assets_sum,
substring(cast(date_add(business_date, 1) as string), 1, 10) as snapshot_date
from
(select product_id,due_bill_no,loan_init_principal from ods_new_s_cps.loan_lending where biz_date<='${var:ST9}' and product_id in (${product_id})) a
join(select due_bill_no,business_date,product_id from ods_new_s_cps.order_info where biz_date<='${var:ST9}' and product_id in (${product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
left join (select distinct due_bill_no,user_hash_no,product_id from ods_new_s.loan_apply where product_id in (${product_id})) c 
on a.due_bill_no=c.due_bill_no and a.product_id=c.product_id
left join(select
    *
  from (
    select *,row_number() over(partition by user_hash_no,product_id order by user_hash_no desc) as rn
    from ods_new_s.customer_info where product_id in (${product_id})
  ) as a
  where a.rn = 1
) d 
on c.user_hash_no=d.user_hash_no and c.product_id=d.product_id
group by substring(cast(date_add(business_date, 1) as string), 1, 10),content
union all
select
'性别' as mold,
d.sex as content,
a.product_id,
cast(sum(loan_init_principal) as decimal(12,2)) as contact_amount,
cast(count(0) as int) as assets_sum,
substring(cast(date_add(business_date, 1) as string), 1, 10) as snapshot_date
from
(select product_id,due_bill_no,loan_init_principal from ods_new_s_cps.loan_lending where biz_date<='${var:ST9}' and product_id in (${product_id})) a
join(select due_bill_no,business_date,product_id from ods_new_s_cps.order_info where biz_date<='${var:ST9}' and product_id in (${product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
left join (select distinct due_bill_no,user_hash_no,product_id from ods_new_s.loan_apply where product_id in (${product_id})) c 
on a.due_bill_no=c.due_bill_no and a.product_id=c.product_id
left join(select
    *
  from (
    select *,row_number() over(partition by user_hash_no,product_id order by user_hash_no desc) as rn
    from ods_new_s.customer_info where product_id in (${product_id})
  ) as a
  where a.rn = 1
) d 
on c.user_hash_no=d.user_hash_no and c.product_id=d.product_id
group by substring(cast(date_add(business_date, 1) as string), 1, 10),content
union all 
select
'年龄' as mold,
case
when c.age < 18 then 'below 18'
when c.age >= 18 and c.age < 23 then '18-22'
when c.age >= 23 and c.age < 26 then '23-25'
when c.age >= 26 and c.age < 31 then '26-30'
when c.age >= 31 and c.age < 36 then '31-35'
when c.age >= 36 and c.age < 41 then '36-40'
when c.age >= 41 and c.age < 46 then '41-45'
when c.age >= 46 and c.age < 51 then '46-50'
when c.age >= 51 then 'above 50'
else null end as content,
a.product_id,
cast(sum(loan_init_principal) as decimal(12,2)) as contact_amount,
cast(count(0) as int) as assets_sum,
substring(cast(date_add(business_date, 1) as string), 1, 10) as snapshot_date
from
(select product_id,due_bill_no,loan_init_principal from ods_new_s_cps.loan_lending where biz_date<='${var:ST9}' and product_id in (${product_id})) a
join(select due_bill_no,business_date,product_id from ods_new_s_cps.order_info where biz_date<='${var:ST9}' and product_id in (${product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
left join (select distinct due_bill_no,age,product_id from ods_new_s.loan_apply where product_id in (${product_id})) c 
on a.due_bill_no=c.due_bill_no and a.product_id=c.product_id
group by substring(cast(date_add(business_date, 1) as string), 1, 10),content
union all 
select
'资产等级' as mold,
c.score_level as content,
a.product_id,
cast(sum(loan_init_principal) as decimal(12,2)) as contact_amount,
cast(count(0) as int) as assets_sum,
substring(cast(date_add(business_date, 1) as string), 1, 10) as snapshot_date
from
(select product_id,due_bill_no,loan_init_principal from ods_new_s_cps.loan_lending where biz_date<='${var:ST9}' and product_id in (${product_id})) a
join(select due_bill_no,business_date,product_id from ods_new_s_cps.order_info where biz_date<='${var:ST9}' and product_id in (${product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
left join(select * from ods_new_s.user_label ) c 
on a.due_bill_no=c.order_id and a.product_id=c.pro_code
group by substring(cast(date_add(business_date, 1) as string), 1, 10),content
union all 
select 
'未偿本金余额' as mold,
'[0, 500)' as content,
a.product_id,
cast(sum(loan_init_principal) as decimal(12,2)) as contact_amount,
cast(count(0) as int) as assets_sum,
substring(cast(date_add(business_date, 1) as string), 1, 10) as snapshot_date
from
(select product_id,due_bill_no,loan_init_principal from ods_new_s_cps.loan_lending where biz_date<='${var:ST9}' and product_id in (${product_id})) a
join(select due_bill_no,business_date,product_id from ods_new_s_cps.order_info where biz_date<='${var:ST9}' and product_id in (${product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
group by substring(cast(date_add(business_date, 1) as string), 1, 10),content
union all 
select 
'资产利率' as mold,
case 
when a.loan_init_interest_rate*100 >= 0 and a.loan_init_interest_rate*100 < 3   then '[0,3%)' 
when a.loan_init_interest_rate*100 >= 3 and a.loan_init_interest_rate*100 < 6   then '[3%,6%)'
when a.loan_init_interest_rate*100 >= 6 and a.loan_init_interest_rate*100 < 9   then '[6%,9%)'
when a.loan_init_interest_rate*100 >= 9 and a.loan_init_interest_rate*100 < 12  then '[9%,12%)'
when a.loan_init_interest_rate*100 >= 12 and a.loan_init_interest_rate*100 < 15 then '[12%,15%)'
when a.loan_init_interest_rate*100 >= 15 and a.loan_init_interest_rate*100 < 18 then '[15%,18%)'
when a.loan_init_interest_rate*100 >= 18 and a.loan_init_interest_rate*100 < 21 then '[18%,21%)'
when a.loan_init_interest_rate*100 >= 21 and a.loan_init_interest_rate*100 < 24 then '[21%,24%)'
when a.loan_init_interest_rate*100 >= 24 and a.loan_init_interest_rate*100 < 27 then '[24%, 27%)'
when a.loan_init_interest_rate*100 >= 27 and a.loan_init_interest_rate*100 < 30 then '[27%, 30%)'
when a.loan_init_interest_rate*100 >= 30 and a.loan_init_interest_rate*100 < 33 then '[30%, 33%)'
when a.loan_init_interest_rate*100 >= 33 and a.loan_init_interest_rate*100 < 36 then '[33%, 36%)'
when a.loan_init_interest_rate*100 >= 36 then '36%以上'
else null end as content,
a.product_id,
cast(sum(loan_init_principal) as decimal(12,2)) as contact_amount,
cast(count(0) as int) as assets_sum,
substring(cast(date_add(business_date, 1) as string), 1, 10) as snapshot_date
from
(select product_id,due_bill_no,loan_init_principal,loan_init_interest_rate from ods_new_s_cps.loan_lending where biz_date<='${var:ST9}' and product_id in (${product_id})) a
join(select due_bill_no,business_date,product_id from ods_new_s_cps.order_info where biz_date<='${var:ST9}' and product_id in (${product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
group by substring(cast(date_add(business_date, 1) as string), 1, 10),content 
union all 
select 
'合同期数' as mold,
cast(a.loan_init_term as string) as content,
a.product_id,
cast(sum(loan_init_principal) as decimal(12,2)) as contact_amount,
cast(count(0) as int) as assets_sum,
substring(cast(date_add(business_date, 1) as string), 1, 10) as snapshot_date
from
(select product_id,due_bill_no,loan_init_principal,loan_init_term from ods_new_s_cps.loan_lending where biz_date<='${var:ST9}' and product_id in (${product_id})) a
join(select due_bill_no,business_date,product_id from ods_new_s_cps.order_info where biz_date<='${var:ST9}' and product_id in (${product_id}) and loan_usage='T') b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
group by substring(cast(date_add(business_date, 1) as string), 1, 10),content
) a )a
    left join
    (
            select *
            from dim_new.biz_conf biz
            where product_id in (${var:product_id})
    ) b
on a.product_id = b.product_id
group by mold,content,project_id;
