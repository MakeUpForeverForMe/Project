set var:product_id='001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';

insert overwrite table dm_eagle.report_dm_lx_asset_distribution partition(snapshot_date,project_id)
select
a.mold,
a.content,
cast(sum(end_assets_prin) as DECIMAL(12,2)),
cast(sum(end_assets_num) as DECIMAL(12,0)),
cast(sum(new_assets_prin) as DECIMAL(12,2)),
cast(sum(new_assets_num) as DECIMAL(12,0)),
snapshot_date ,
b.project_id
from
(select
mold,
content,
product_id,
cast(end_assets_prin as DECIMAL(12,2)) as end_assets_prin,
cast(end_assets_num as DECIMAL(12,0)) as end_assets_num,
cast(new_assets_prin as DECIMAL(12,2)) as new_assets_prin,
cast(new_assets_num as DECIMAL(12,0)) as new_assets_num,
'${var:ST9}' as snapshot_date
from
(select
'地区' as mold,
c.idcard_province as content,
a.product_id,
cast(sum(a.loan_init_principal-a.paid_principal) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(if(if(a.loan_init_principal-a.paid_principal>0,a.due_bill_no,'')='',0,1)) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.today_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(if(a.today_due='',0,1)) as DECIMAL(12,0)) as new_assets_num
from
(
select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.loan_init_principal as loan_init_principal,a.paid_principal,0 as today_prin,'' as today_due
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,sum(b.should_repay_principal) as loan_init_principal,sum(if(b.due_bill_no='1120061910384241252747',200,b.paid_principal)) as  paid_principal
from
(select * from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<= date_sub('${var:ST9}',1)) a left join (select * from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1)) b on a.due_bill_no = b.due_bill_no and a.product_id=b.product_id group by a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.product_id)a
union all
select product_id,due_bill_no,loan_init_interest_rate,loan_init_term,loan_init_principal,0 as paid_principal,loan_init_principal as today_prin,due_bill_no as today_due  from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<='${var:ST9}'and biz_date='${var:ST9}')a
left join (select distinct
    user_hash_no,
    age,
    due_bill_no,
    product_id,
    credit_coef,
    loan_amount_apply,
    loan_terms,
    interest_rate
  from ods_new_s.loan_apply
  where product_id in (${var:product_id})) b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
left join(select
    *
  from (
    select *,row_number() over(partition by user_hash_no,product_id order by user_hash_no desc) as rn
    from ods_new_s.customer_info where product_id in (${var:product_id})
  ) as a
  where a.rn = 1
) c
on b.user_hash_no=c.user_hash_no and b.product_id=c.product_id
group by mold,content,product_id
union all
select
'性别' as mold,
c.sex as content,
a.product_id,
cast(sum(a.loan_init_principal-a.paid_principal) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(if(if(a.loan_init_principal-a.paid_principal>0,a.due_bill_no,'')='',0,1)) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.today_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(if(a.today_due='',0,1)) as DECIMAL(12,0)) as new_assets_num
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.loan_init_principal as loan_init_principal,a.paid_principal,0 as today_prin,'' as today_due
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,sum(b.should_repay_principal) as loan_init_principal,sum(if(b.due_bill_no='1120061910384241252747',200,b.paid_principal)) as  paid_principal
from
(select * from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<=date_sub('${var:ST9}',1)) a left join (select * from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1)) b on a.due_bill_no = b.due_bill_no and a.product_id=b.product_id group by a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.product_id)a
union all
select product_id,due_bill_no,loan_init_interest_rate,loan_init_term,loan_init_principal,0 as paid_principal,loan_init_principal as today_prin,due_bill_no as today_due  from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<='${var:ST9}'and biz_date='${var:ST9}')a
left join (select distinct due_bill_no,user_hash_no,product_id from ods_new_s.loan_apply where product_id in (${var:product_id})) b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
left join(select
    *
  from (
    select *,row_number() over(partition by user_hash_no,product_id order by user_hash_no desc) as rn
    from ods_new_s.customer_info where product_id in (${var:product_id})
  ) as a
  where a.rn = 1
) c
on b.user_hash_no=c.user_hash_no and b.product_id=c.product_id
group by mold,content,product_id
union all
select '年龄' as mold,
case
when b.age < 18 then 'below 18'
when b.age >= 18 and b.age < 23 then '18-22'
when b.age >= 23 and b.age < 26 then '23-25'
when b.age >= 26 and b.age < 31 then '26-30'
when b.age >= 31 and b.age < 36 then '31-35'
when b.age >= 36 and b.age < 41 then '36-40'
when b.age >= 41 and b.age < 46 then '41-45'
when b.age >= 46 and b.age < 51 then '46-50'
when b.age >= 51 then 'above 50'
else null end as content,
a.product_id,
cast(sum(a.loan_init_principal-a.paid_principal) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(if(if(a.loan_init_principal-a.paid_principal>0,a.due_bill_no,'')='',0,1)) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.today_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(if(a.today_due='',0,1)) as DECIMAL(12,0)) as new_assets_num
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.loan_init_principal as loan_init_principal,a.paid_principal,0 as today_prin,'' as today_due
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,sum(b.should_repay_principal) as loan_init_principal,sum(if(b.due_bill_no='1120061910384241252747',200,b.paid_principal)) as  paid_principal
from
(select * from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<=date_sub('${var:ST9}',1)) a left join (select * from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1)) b on a.due_bill_no = b.due_bill_no and a.product_id=b.product_id group by a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.product_id)a
union all
select product_id,due_bill_no,loan_init_interest_rate,loan_init_term,loan_init_principal,0 as paid_principal,loan_init_principal as today_prin,due_bill_no as today_due  from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<='${var:ST9}'and biz_date='${var:ST9}')a
left join (select distinct due_bill_no,age,product_id from ods_new_s.loan_apply where product_id in (${var:product_id})) b
on a.due_bill_no=b.due_bill_no and a.product_id=b.product_id
group by mold,content,product_id
union all
select
'资产等级' as mold,
b.score_level as content,
a.product_id,
cast(sum(a.loan_init_principal-a.paid_principal) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(if(if(a.loan_init_principal-a.paid_principal>0,a.due_bill_no,'')='',0,1)) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.today_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(if(a.today_due='',0,1)) as DECIMAL(12,0)) as new_assets_num
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.loan_init_principal as loan_init_principal,a.paid_principal,0 as today_prin,'' as today_due
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,sum(b.should_repay_principal) as loan_init_principal,sum(if(b.due_bill_no='1120061910384241252747',200,b.paid_principal)) as  paid_principal
from
(select * from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<=date_sub('${var:ST9}',1)) a left join (select * from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1)) b on a.due_bill_no = b.due_bill_no and a.product_id=b.product_id group by a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.product_id)a
union all
select product_id,due_bill_no,loan_init_interest_rate,loan_init_term,loan_init_principal,0 as paid_principal,loan_init_principal as today_prin,due_bill_no as today_due  from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<='${var:ST9}'and biz_date='${var:ST9}')a
left join(select * from ods_new_s.user_label ) b
on a.due_bill_no=b.order_id and a.product_id=b.pro_code
group by mold,content,product_id
union all
select
'未偿本金余额' as mold,
a.outstanding_principal as content,
a.product_id,
cast(sum(a.end_assets_prin) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(a.end_assets_num) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.new_assets_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(a.new_assets_num) as DECIMAL(12,0)) as new_assets_num
from
(select
case
when sum(loan_init_principal-paid_principal) >= 0 and sum(loan_init_principal-paid_principal) < 500 then '[0, 500)'
when sum(loan_init_principal-paid_principal) >= 500 and sum(loan_init_principal-paid_principal) < 2000 then '[500, 2000)'
when sum(loan_init_principal-paid_principal) >= 2000 and sum(loan_init_principal-paid_principal) < 5000 then '[2000, 5000)'
when sum(loan_init_principal-paid_principal) >= 5000 and sum(loan_init_principal-paid_principal) < 10000 then '[5000, 10000)'
when sum(loan_init_principal-paid_principal) >= 10000 and sum(loan_init_principal-paid_principal) < 20000 then '[10000,20000)'
when sum(loan_init_principal-paid_principal) >= 20000 and sum(loan_init_principal-paid_principal) < 30000 then '[20000, 30000)'
when sum(loan_init_principal-paid_principal) >= 30000 then '30000+'
else null end as outstanding_principal,
due_bill_no,
product_id,
cast(sum(a.loan_init_principal-a.paid_principal) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(if(if(a.loan_init_principal-a.paid_principal>0,a.due_bill_no,'')='',0,1)) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.today_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(if(a.today_due='',0,1)) as DECIMAL(12,0)) as new_assets_num
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.loan_init_principal as loan_init_principal,a.paid_principal,0 as today_prin,'' as today_due
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,sum(b.should_repay_principal) as loan_init_principal,cast(sum(b.paid_principal) as DECIMAL(38,4)) as  paid_principal
from
(select * from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<=date_sub('${var:ST9}',1)) a left join (select * from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1)) b on a.due_bill_no = b.due_bill_no and a.product_id=b.product_id group by a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.product_id)a
union all
select product_id,due_bill_no,loan_init_interest_rate,loan_init_term,loan_init_principal,0 as paid_principal,loan_init_principal as today_prin,due_bill_no as today_due  from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<='${var:ST9}' and biz_date='${var:ST9}')a
group by a.due_bill_no,a.product_id) a
group by mold,content,product_id
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
cast(sum(a.loan_init_principal-a.paid_principal) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(if(if(a.loan_init_principal-a.paid_principal>0,a.due_bill_no,'')='',0,1)) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.today_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(if(a.today_due='',0,1)) as DECIMAL(12,0)) as new_assets_num
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.loan_init_principal as loan_init_principal,a.paid_principal,0 as today_prin,'' as today_due
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,sum(b.should_repay_principal) as loan_init_principal,sum(if(b.due_bill_no='1120061910384241252747',200,b.paid_principal)) as  paid_principal
from
(select * from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<=date_sub('${var:ST9}',1)) a left join (select * from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1)) b on a.due_bill_no = b.due_bill_no and a.product_id=b.product_id group by a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.product_id)a
union all
select product_id,due_bill_no,loan_init_interest_rate,loan_init_term,loan_init_principal,0 as paid_principal,loan_init_principal as today_prin,due_bill_no as today_due  from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<='${var:ST9}' and biz_date='${var:ST9}')a
group by mold,content,product_id
union all
select
'合同期数' as mold,
cast(a.loan_init_term as string) as content,
a.product_id,
cast(sum(a.loan_init_principal-a.paid_principal) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(if(if(a.loan_init_principal-a.paid_principal>0,a.due_bill_no,'')='',0,1)) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.today_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(if(a.today_due='',0,1)) as DECIMAL(12,0)) as new_assets_num
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.loan_init_principal as loan_init_principal,a.paid_principal,0 as today_prin,'' as today_due
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,sum(b.should_repay_principal) as loan_init_principal,sum(if(b.due_bill_no='1120061910384241252747',200,b.paid_principal)) as  paid_principal
from
(select * from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<=date_sub('${var:ST9}',1)) a left join (select * from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1)) b on a.due_bill_no = b.due_bill_no and a.product_id=b.product_id group by a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.product_id)a
union all
select product_id,due_bill_no,loan_init_interest_rate,loan_init_term,loan_init_principal,0 as paid_principal,loan_init_principal as today_prin,due_bill_no as today_due  from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<='${var:ST9}' and biz_date='${var:ST9}')a
group by mold,content,product_id
union all
select
'剩余期数' as mold,
case
when b.remain_term >= 1 and b.remain_term < 3   then '(0,3)'
when b.remain_term >= 3 and b.remain_term < 6   then '[3,6)'
when b.remain_term >= 6 and b.remain_term < 9   then '[6,9)'
when b.remain_term >= 9 and b.remain_term < 12  then '[9,12)'
when b.remain_term >= 12 and b.remain_term < 15 then '[12,15)'
when b.remain_term >= 15 and b.remain_term < 18 then '[15,18)'
when b.remain_term >= 18 and b.remain_term < 21 then '[18,21)'
when b.remain_term >= 21 and b.remain_term < 24 then '[21,24)'
when b.remain_term >= 24 then '24以上(包含24)'
else null end as content,
a.product_id,
cast(sum(a.loan_init_principal-a.paid_principal) as DECIMAL(12,2)) as end_assets_prin,
cast(sum(if(if(a.loan_init_principal-a.paid_principal>0,a.due_bill_no,'')='',0,1)) as DECIMAL(12,0)) as end_assets_num,
cast(sum(a.today_prin) as DECIMAL(12,2)) as new_assets_prin,
cast(sum(if(a.today_due='',0,1)) as DECIMAL(12,0)) as new_assets_num
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.loan_init_principal as loan_init_principal,a.paid_principal,0 as today_prin,'' as today_due
from
(select a.product_id,a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,sum(b.should_repay_principal) as loan_init_principal,sum(b.paid_principal) as  paid_principal
from
(select * from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<=date_sub('${var:ST9}',1)) a left join (select * from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1)) b on a.due_bill_no = b.due_bill_no and a.product_id=b.product_id group by a.due_bill_no,a.loan_init_interest_rate,a.loan_init_term,a.product_id)a
union all
select product_id,due_bill_no,loan_init_interest_rate,loan_init_term,loan_init_principal,0 as paid_principal,loan_init_principal as today_prin,due_bill_no as today_due  from ods_new_s_cps.loan_lending where product_id in (${var:product_id}) and biz_date<='${var:ST9}' and biz_date='${var:ST9}')a
join
(select a.due_bill_no,a.remain_term from
(select due_bill_no,count(0) as remain_term from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= date_sub('${var:ST9}',1) and e_d_date>date_sub('${var:ST9}',1) and paid_out_date is null group by due_bill_no
union all
select due_bill_no,count(0) as remain_term from ods_new_s_cps.repay_schedule where product_id in (${var:product_id}) and s_d_date <= '${var:ST9}' and e_d_date>'${var:ST9}' and loan_active_date='${var:ST9}'  group by due_bill_no)a where a.remain_term>0 )b
on a.due_bill_no=b.due_bill_no
group by mold,content,product_id
)a)a
left join
(
        select *
        from dim_new.biz_conf biz
        where product_id in (${var:product_id})
) b
on a.product_id = b.product_id
group by mold,content,project_id
;
