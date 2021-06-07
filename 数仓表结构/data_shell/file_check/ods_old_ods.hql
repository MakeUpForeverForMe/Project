

--实还表 汇通校验结果 对不上
--EMR 重跑后修正了以前错误的数据

+-------------+------------+----------+-------------+------------+------------+
| due_bill_no | repay_term | biz_date | due_bill_no | repay_term | biz_date   |
+-------------+------------+----------+-------------+------------+------------+
| NULL        | NULL       | NULL     | 1000000720  | 3          | 2021-02-05 |
| NULL        | NULL       | NULL     | 1000000060  | 10         | 2021-02-05 |
| NULL        | NULL       | NULL     | 1000000223  | 9          | 2021-02-05 |
| NULL        | NULL       | NULL     | 1000000223  | 9          | 2021-02-05 |
| NULL        | NULL       | NULL     | 1000000720  | 3          | 2021-02-05 |
| NULL        | NULL       | NULL     | 1000000060  | 10         | 2021-02-05 |
+-------------+------------+----------+-------------+------------+------------+



set hivevar:tb_shuffix=;
set hivevar:db_shuffix=;
set hivevar:start_date=2020-06-02;
set hivevar:end_date=2021-03-28;

-- order 表
set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;
set mapreduce.input.fileinputformat.split.maxsize=1024000000;
set mapred.max.split.size=1024000000;
-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;

select
if('${tb_shuffix}'='_cps',"代偿后","代偿前"),
new_biz_date,
new_product_id,
new_ods_num,
new_txn_amt,
old_biz_date,
old_product_id
old_ods_num,
old_txn_amt
from
(
select
    biz_date as new_biz_date,
    product_id as new_product_id,
    count(due_bill_no) as new_ods_num,
    sum(txn_amt) as new_txn_amt
    from ods${db_shuffix}.order_info
where biz_date between '${start_date}' and '${end_date}' and order_status='S'
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202','pl00282')
        group by col_id
)
group by biz_date,product_id
)new_ods
full join
(
select
    biz_date            as old_biz_date,
    product_id          as old_product_id,
    count(due_bill_no)  as old_ods_num,
    sum(txn_amt)        as old_txn_amt
    from data_check.order_info${tb_shuffix}
where biz_date between '${start_date}' and '${end_date}' and order_status='S'
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202','pl00282')
        group by col_id
)
group by biz_date,product_id
)old_ods
on new_ods.new_biz_date=old_ods.old_biz_date and new_ods.new_product_id=old_ods.old_product_id
where abs(nvl(new_ods_num,0)-nvl(old_ods_num,0))>0
or abs(nvl(old_txn_amt,0)-nvl(old_txn_amt,0)) >0
limit 20;


--实还表


set hivevar:tb_shuffix=;
set hivevar:db_shuffix=;
set hivevar:start_date=2021-03-01;
set hivevar:end_date=2021-03-28;
select
if('${tb_shuffix}'='_cps',"代偿后","代偿前"),
new_biz_date,
new_product_id,
new_due_bill_no,
new_paid_Pricinpal,
new_paid_Interest,
old_biz_date,
old_product_id
old_ods_num,
old_due_bill_no,
old_paid_Pricinpal,
old_paid_Interest
from
(
select
    biz_date as new_biz_date,
    product_id as new_product_id,
    due_bill_no as new_due_bill_no,
    sum(if(bnp_type='Pricinpal',repay_amount,0)) as new_paid_Pricinpal,
    sum(if(bnp_type='Interest',repay_amount,0)) as new_paid_Interest
    from ods${db_shuffix}.repay_detail
where biz_date between '${start_date}' and '${end_date}' and  bnp_type in ('Pricinpal','Interest')
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac'  and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202','pl00282')
        group by col_id
)
group by biz_date,product_id,due_bill_no
)new_ods
full join
(
select
    biz_date            as old_biz_date,
    product_id          as old_product_id,
    due_bill_no         as old_due_bill_no,
    sum(if(bnp_type='Pricinpal',repay_amount,0)) as old_paid_Pricinpal,
    sum(if(bnp_type='Interest',repay_amount,0)) as old_paid_Interest
    from data_check.repay_detail${tb_shuffix}
where biz_date between '${start_date}' and '${end_date}' and  bnp_type in ('Pricinpal','Interest')
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202','pl00282')
        group by col_id
)
group by biz_date,product_id,due_bill_no
)old_ods
on new_ods.new_biz_date=old_ods.old_biz_date and new_ods.new_product_id=old_ods.old_product_id
and new_ods.new_due_bill_no=old_ods.old_due_bill_no
where
 abs(nvl(new_paid_Pricinpal,0)-nvl(old_paid_Pricinpal,0)) >0
or abs(nvl(new_paid_Interest,0)-nvl(old_paid_Interest,0)) >0
order by old_biz_date,new_biz_date
limit 20;


--放款表
set hivevar:tb_shuffix=_cps;
set hivevar:db_shuffix=_cps;
set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
set hivevar:start_date=2019-10-01;
set hivevar:end_date=2021-03-28;
-- order 表
select
if('${tb_shuffix}'='_cps',"代偿后","代偿前"),
new_biz_date,
new_product_id,
new_ods_num,
old_biz_date,
old_product_id
old_ods_num
from
(
select
    biz_date as new_biz_date,
    product_id as new_product_id,
    count(due_bill_no) as new_ods_num
    from ods${db_shuffix}.loan_lending
where biz_date between '${start_date}' and '${end_date}'
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202','001701','001702')
        group by col_id
)
group by biz_date,product_id
)new_ods
full join
(
select
    biz_date            as old_biz_date,
    product_id          as old_product_id,
    count(due_bill_no)  as old_ods_num
    from data_check.loan_lending${tb_shuffix}
where biz_date between '${start_date}' and '${end_date}'
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202','001701','001702')
        group by col_id
)
group by biz_date,product_id
)old_ods
on new_ods.new_biz_date=old_ods.old_biz_date and new_ods.new_product_id=old_ods.old_product_id
where abs(nvl(new_ods_num,0)-nvl(old_ods_num,0))>0
order by old_biz_date,new_biz_date
limit 20;



--借据表
select
if('${tb_shuffix}'='_cps',"代偿后","代偿前"),
new_product_id,
new_due_bill_no   ,
nvl(new_remain_term,0)-(nvl(old_remain_term,0))                 as diff_remain_term   ,
nvl(new_paid_principal,0)-(nvl(old_paid_principal,0))           as diff_paid_principal,
nvl(new_paid_interest,0)-(nvl(old_paid_interest,0))             as diff_paid_interest,

nvl(new_paid_svc_fee,0)-(nvl(old_paid_svc_fee,0))               as diff_paid_svc_fee,
nvl(new_paid_term_fee,0)-(nvl(old_paid_term_fee,0))             as diff_paid_term_fee,
nvl(new_paid_penalty,0)-(nvl(old_paid_penalty,0))               as diff_paid_penalty,
nvl(new_paid_mult,0)-(nvl(old_paid_mult,0))                     as diff_paid_mult,

nvl(new_overdue_prin,0)-(nvl(old_overdue_prin,0))               as diff_overdue_prin,
nvl(new_overdue_interest,0)-(nvl(old_overdue_interest,0))       as diff_overdue_interest,
nvl(new_overdue_days,0) -nvl(old_overdue_days,0)                as diff_overdue_days,
old_product_id,
old_due_bill_no
from

(
select
product_id                       new_product_id,
due_bill_no                      new_due_bill_no     ,
remain_term                      new_remain_term     ,
paid_principal                   new_paid_principal  ,
paid_interest                    new_paid_interest   ,
paid_svc_fee                     new_paid_svc_fee    ,
paid_term_fee                    new_paid_term_fee   ,
paid_penalty                     new_paid_penalty    ,
paid_mult                        new_paid_mult       ,
overdue_prin                     new_overdue_prin    ,
overdue_interest                 new_overdue_interest,
overdue_days                     new_overdue_days
from ods${db_shuffix}.loan_info
where "${ST9}" between  s_d_Date and date_sub(e_d_date,1)
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202')
        group by col_id )
)new_loan
full join (
select
product_id                       old_product_id,
due_bill_no                      old_due_bill_no     ,
remain_term                      old_remain_term     ,
paid_principal                   old_paid_principal  ,
paid_interest                    old_paid_interest   ,
paid_svc_fee                     old_paid_svc_fee    ,
paid_term_fee                    old_paid_term_fee   ,
paid_penalty                     old_paid_penalty    ,
paid_mult                        old_paid_mult       ,
overdue_prin                     old_overdue_prin    ,
overdue_interest                 old_overdue_interest,
overdue_days                     old_overdue_days
from ods.loan_info${tb_shuffix}
where "${ST9}" between  s_d_Date and date_sub(e_d_date,1)
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506')
        group by col_id )
)old_loan on new_loan.new_due_bill_no=old_loan.old_due_bill_no
and new_loan.new_product_id=old_loan.old_product_id
where
abs(nvl(new_remain_term,0)-(nvl(old_remain_term,0))          )>0 or
abs(nvl(new_paid_principal,0)-(nvl(old_paid_principal,0))    )>0 or
abs(nvl(new_paid_interest,0)-(nvl(old_paid_interest,0))      )>0 or
abs(nvl(new_paid_svc_fee,0)-(nvl(old_paid_svc_fee,0))        )>0 or
abs(nvl(new_paid_term_fee,0)-(nvl(old_paid_term_fee,0))      )>0 or
abs(nvl(new_paid_penalty,0)-(nvl(old_paid_penalty,0))        )>0 or
abs(nvl(new_paid_mult,0)-(nvl(old_paid_mult,0))              )>0 or
abs(nvl(new_overdue_prin,0)-(nvl(old_overdue_prin,0))        )>0 or
abs(nvl(new_overdue_interest,0)-(nvl(old_overdue_interest,0)))>0 or
abs(nvl(new_overdue_days,0) -nvl(old_overdue_days,0) )>0
limit 20;









select
new_old.order_id,
new_old.biz_date,
old_order.order_id,
old_order.biz_date
from
(select
order_id,biz_date
from ods.order_info where product_id in ('001601','001602','001603') and order_status='S' and biz_date between '2019-01-01' and '2020-01-01' and biz_date!='2021-03-25'
)new_old
full join (
select
order_id,biz_date
from data_check.order_info where product_id in ('001601','001602','001603') and order_status='S'  and biz_date between '2019-01-01' and '2020-01-01'  and biz_date!='2021-03-25'
)old_order on new_old.order_id=old_order.order_id and new_old.biz_date=old_order.biz_date
where old_order.order_id is null or new_old.order_id is null
order by new_old.biz_date,old_order.biz_date
limit 20;


select
new_old.due_bill_no,
new_old.repay_term,
new_old.biz_date,
old_order.due_bill_no,
old_order.repay_term,
old_order.biz_date
from
(select
due_bill_no,repay_term,biz_date
from ods.repay_detail where product_id in ('001601','001602','001603')  and biz_date between '2021-01-01' and '2021-04-01' and biz_date!='2021-03-25'
)new_old
full join (
select
due_bill_no,repay_term,biz_date
from data_check.repay_detail where product_id in ('001601','001602','001603')   and biz_date between '2021-01-01' and '2021-04-01'  and biz_date!='2021-03-25'
)old_order on new_old.due_bill_no=old_order.due_bill_no and new_old.repay_term=old_order.repay_term and new_old.biz_date=old_order.biz_date
where old_order.due_bill_no is null or new_old.due_bill_no is null
order by new_old.biz_date,old_order.biz_date
limit 20;











--还款计划











--用信表数据对照



set hivevar:tb_shuffix=;
set hivevar:db_shuffix=;
set hivevar:start_date=2020-12-01;
set hivevar:end_date=2021-04-05;
set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;

-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;



select
new_biz_date,
new_product_id,
new_ods_num,
old_biz_date,
old_product_id
old_ods_num
from
(
select
    biz_date as new_biz_date,
    product_id as new_product_id,
    count(distinct due_bill_no) as new_ods_num
    from ods.loan_apply
where biz_date between '${start_date}' and '${end_date}'
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202')
        group by col_id
)
group by biz_date,product_id
)new_ods
full join
(
select
    biz_date            as old_biz_date,
    product_id          as old_product_id,
    count(distinct due_bill_no)  as old_ods_num
    from data_check.loan_apply
where biz_date between '${start_date}' and '${end_date}'
and product_id in (
         select
          max(if(col_name = 'product_id',  col_val,null)) as product_id
        from dim.data_conf
        where col_type = 'ac' and col_val not in ('001504','001507','001503','J90400','001506','002201','002203','002202')
        group by col_id
)
group by biz_date,product_id
)old_ods
on new_ods.new_biz_date=old_ods.old_biz_date and new_ods.new_product_id=old_ods.old_product_id
where abs(nvl(new_ods_num,0)-nvl(old_ods_num,0))>0
order by old_biz_date,new_biz_date
limit 100;



