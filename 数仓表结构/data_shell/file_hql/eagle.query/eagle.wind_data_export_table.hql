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
set hivevar:project_id='CL202012280092','cl00297','cl00306','cl00309','CL201911130070','CL202002240081';
--set hivevar:ST9=2021-03-01;

set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;





insert overwrite table eagle.wind_data_export_table partition(biz_date,project_id)
select
loan.product_id,
loan.due_bill_no,
customer.name,
customer.idcard_no,
customer.mobie,
if(loan.loan_status='F',1,0) as loan_status,
'${ST9}' as biz_date,
loan.project_id
from
(select
distinct
due_bill_no,
loan_status,
product_id,
project_id
from
ods.loan_info_abs
where '${ST9}' between s_d_date and date_sub(e_d_date,1) and loan_status!='F'
and project_id in (${project_id})
)loan
left join (
select

customer_info.due_bill_no,
customer_info.project_id,
encrypt_aes(customer_name.dim_decrypt,"tencentabs123456") as name,
encrypt_aes(customer_phone.dim_decrypt,"tencentabs123456") as mobie,
encrypt_aes(idcard_no.dim_decrypt,"tencentabs123456") as idcard_no
from
(
select due_bill_no,product_id,project_id,name,mobie,idcard_no from ods.customer_info_abs where project_id in (${project_id})
)customer_info
inner join (
select dim_encrypt,dim_decrypt from  dim.dim_encrypt_info
)customer_name on customer_info.name=customer_name.dim_encrypt
inner join (
select dim_encrypt,dim_decrypt from  dim.dim_encrypt_info
)customer_phone on customer_info.mobie=customer_phone.dim_encrypt
inner join (
select dim_encrypt,dim_decrypt from  dim.dim_encrypt_info
)idcard_no on customer_info.idcard_no=idcard_no.dim_encrypt
)customer
on loan.due_bill_no=customer.due_bill_no
and loan.project_id=customer.project_id
--where  loan.due_bill_no="5100747086";


