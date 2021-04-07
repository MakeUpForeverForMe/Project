---统计的数据是快照日再减一
set hivevar:product_id_list='001801','001802','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007';
--set hivevar:last_date=2020-07-20;
--set hivevar:ST9=2020-07-18;
set hive.execution.engine=mr;
set mapreduce.map.memory.mb=4096;
set mapreduce.reduce.memory.mb=4096;
set hive.exec.dynamic.partition=true;
set mapreduce.input.fileinputformat.split.maxsize=1024000000;
set mapreduce.input.fileinputformat.split.minsize=1024000000;
set hive.exec.parallel=true;
set hive.auto.convert.join=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=30000;
set hive.exec.max.dynamic.partitions.pernode=10000;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
--alter table dim_new.dw_static_overdue_bill rename to dim_new.dim_static_overdue_bill;
--ALTER TABLE dw_new.dw_report_cal_day ADD COLUMNS (overdue_remain_principal decimal(15,4) COMMENT '逾期1+剩余本金');;
--ALTER TABLE dw_new.dw_report_cal_day ADD COLUMNS (acc_buy_back_amount decimal(15,4) COMMENT '累计180+回购金额');;

--累计静态逾期金额
insert overwrite table dw_new.dw_report_cal_day partition (biz_date='${ST9}')
select
--case
biz_conf.project_id as project_id,
--when static_over_amount.product_id in ('001801','001802') then 'WS0006200001'
--when static_over_amount.product_id in ('001901','001902','001903','001904','001905','001906','001907') then 'WS0009200001'
--when static_over_amount.product_id in ('002001','002002','002003','002004','002005','002006','002007') then 'WS0006200002'
--else  static_over_amount.product_id end  as project_id,
nvl(m2plus_recover_amount,0),
nvl(m2plus_recover_prin ,0),
nvl(m2plus_recover_inter,0) ,
nvl(m4plus_recover_amount ,0) ,
nvl(m4plus_recover_prin  ,0),
nvl(m4plus_recover_inter ,0),
nvl(m2plus_recover_amount_acc,0),
nvl(m2plus_recover_prin_acc,0),
nvl(m2plus_recover_inter_acc,0),
nvl(m4plus_recover_amount_acc,0),
nvl(m4plus_recover_prin_acc,0),
nvl(m4plus_recover_inter_acc,0),
nvl(m2plus_num,0) ,
nvl(m2plus_amount,0) ,
nvl(m2plus_prin,0) ,
nvl(m2plus_inter,0) ,
nvl(m4plus_num,0) ,
nvl(m4plus_amount,0) ,
nvl(m4plus_prin,0) ,
nvl(m4plus_inter,0) ,
nvl(static_30_new_amount,0),
nvl(static_30_new_prin,0),
nvl(static_30_new_inter,0),
nvl(static_90_new_amount,0),
nvl(static_90_new_prin,0),
nvl(static_90_new_inter,0),
nvl(static_30_acc_amount,0),
nvl(static_acc_30_prin,0),
nvl(static_acc_30_iner,0),
nvl(static_acc_90_amount,0),
nvl(static_acc_90_prin ,0),
nvl(static_acc_90_iner ,0),
nvl(overdue_remain_principal,0),
nvl(acc_buy_back_amount,0)
from
(
select * from dim_new.biz_conf where product_id in (${product_id_list})
)biz_conf
left join
(
select
product_id,
sum(if(overdue_days=31 and biz_date='${ST9}',if(remain_principal is null,0,remain_principal)+if(remain_interest is null,0,remain_interest),0)) as static_30_new_amount,
sum(if(overdue_days=31 and biz_date='${ST9}',remain_principal,0)) as static_30_new_prin,
sum(if(overdue_days=31 and biz_date='${ST9}',remain_interest,0)) as static_30_new_inter,
sum(if(overdue_days=91 and biz_date='${ST9}',if(remain_principal is null,0,remain_principal)+if(remain_interest is null,0,remain_interest),0)) as static_90_new_amount,
sum(if(overdue_days=91 and biz_date='${ST9}',remain_principal,0)) as static_90_new_prin,
sum(if(overdue_days=91 and biz_date='${ST9}',remain_interest,0)) as static_90_new_inter,
sum(if(overdue_days=31,if(remain_principal is null,0,remain_principal)+if(remain_interest is null,0,remain_interest),0)) as static_30_acc_amount,
sum(if(overdue_days=31,remain_principal,0)) as static_acc_30_prin,
sum(if(overdue_days=31,remain_interest,0)) as static_acc_30_iner,
sum(if(overdue_days=91,if(remain_principal is null,0,remain_principal)+if(remain_interest is null,0,remain_interest),0)) as static_acc_90_amount,
sum(if(overdue_days=91,remain_principal,0)) as static_acc_90_prin,
sum(if(overdue_days=91,remain_interest,0)) as static_acc_90_iner
from dim_new.dim_static_overdue_bill where prj_type in ('LXGM','LXZT','LXGM2') and biz_date<= '${ST9}' and overdue_days in (31,91)
group by  product_id
)static_over_amount on  biz_conf.product_id = static_over_amount.product_id
left join
(
select
static_loan.product_id,
sum(if(schedule.paid_out_date>=static_loan.biz_date and schedule.paid_out_date='${ST9}'  and overdue_days=31,if(schedule.paid_amount is not null,schedule.paid_amount,0),0)) as m2plus_recover_amount,
sum(if(schedule.paid_out_date>=static_loan.biz_date and schedule.paid_out_date='${ST9}' and overdue_days=31,if(schedule.paid_principal is not null,schedule.paid_principal,0),0)) as m2plus_recover_prin,
sum(if(schedule.paid_out_date>=static_loan.biz_date and schedule.paid_out_date='${ST9}' and overdue_days=31,if(schedule.paid_interest is not null,schedule.paid_interest,0),0))as m2plus_recover_inter,
sum(if(schedule.paid_out_date>=static_loan.biz_date and schedule.paid_out_date='${ST9}' and overdue_days=91,if(schedule.paid_amount is not null,schedule.paid_amount,0),0)) as m4plus_recover_amount,
sum(if(schedule.paid_out_date>=static_loan.biz_date and schedule.paid_out_date='${ST9}' and overdue_days=91,if(schedule.paid_principal is not null,schedule.paid_principal,0),0)) as m4plus_recover_prin,
sum(if(schedule.paid_out_date>=static_loan.biz_date and schedule.paid_out_date='${ST9}' and overdue_days=91,if(schedule.paid_interest is not null,schedule.paid_interest,0),0)) as m4plus_recover_inter,
sum(if(schedule.paid_out_date>=static_loan.biz_date and overdue_days=31,if(schedule.paid_amount is not null,schedule.paid_amount,0),0)) as m2plus_recover_amount_acc,
sum(if(schedule.paid_out_date>=static_loan.biz_date and overdue_days=31,if(schedule.paid_principal is not null,schedule.paid_principal,0),0)) as m2plus_recover_prin_acc,
sum(if(schedule.paid_out_date>=static_loan.biz_date and overdue_days=31,if(schedule.paid_interest is not null,schedule.paid_interest,0),0))as m2plus_recover_inter_acc,
sum(if(schedule.paid_out_date>=static_loan.biz_date and overdue_days=91,if(schedule.paid_amount is not null,schedule.paid_amount,0),0)) as m4plus_recover_amount_acc,
sum(if(schedule.paid_out_date>=static_loan.biz_date and overdue_days=91,if(schedule.paid_principal is not null,schedule.paid_principal,0),0)) as m4plus_recover_prin_acc,
sum(if(schedule.paid_out_date>=static_loan.biz_date and overdue_days=91,if(schedule.paid_interest is not null,schedule.paid_interest,0),0)) as m4plus_recover_inter_acc
from
(
select * from   dim_new.dim_static_overdue_bill where biz_date<='${ST9}' and prj_type in ('LXGM','LXZT','LXGM2')  and overdue_days in (31,91)
)static_loan inner join
(
select
due_bill_no,
product_id,
paid_out_date,
sum(paid_principal) as paid_principal,
sum(paid_interest) as paid_interest,
sum(paid_amount) as paid_amount
from ods_new_s.repay_schedule
where
product_id in (${product_id_list})
and paid_out_date<='${ST9}' and paid_out_date is not null
  and '${ST9}' between s_d_date and date_sub(e_d_date,1)
group by due_bill_no,product_id,paid_out_date
)schedule on static_loan.due_bill_no =schedule.due_bill_no and static_loan.product_id =schedule.product_id
group by static_loan.product_id
)recover_amount on  static_over_amount.product_id =recover_amount.product_id
left  join (
select
product_id,
sum(if(overdue_days>30,1,0)) as m2plus_num,
sum(if(overdue_days>30,remain_amount,0)) as m2plus_amount,
sum(if(overdue_days>30,remain_principal,0)) as m2plus_prin,
sum(if(overdue_days>30,remain_interest,0)) as m2plus_inter,
sum(if(overdue_days>90,1,0)) as m4plus_num,
sum(if(overdue_days>90,remain_amount,0)) as m4plus_amount,
sum(if(overdue_days>90,remain_principal,0)) as m4plus_prin,
sum(if(overdue_days>90,remain_interest,0)) as m4plus_inter
from
ods_new_s.loan_info
where
product_id in (${product_id_list})
and '${ST9}' between s_d_date and date_sub(e_d_date,1)  and overdue_days >30
group by product_id
)curr_over  on static_over_amount.product_id =curr_over.product_id
--新加字段逾期剩余本金
left join (
    select product_id,
    sum(remain_principal)  as overdue_remain_principal
    from ods_new_s.loan_info
    where "${ST9}" between s_d_date and date_sub(e_d_date,1)
    and overdue_days>=1 and loan_status='O' and product_id in (${product_id_list})
    and nvl(paid_out_type,"normal")!='BUY_BACK'
    group by product_id
)loan_over on biz_conf.product_id=loan_over.product_id
--统计逾期180天回购金额
left join(
        select
        loan.product_id,
        sum(paid_amount) as acc_buy_back_amount
        from
        (
            select due_bill_no,
            sum(paid_amount) as paid_amount
            from ods_new_s_cps.repay_schedule where
            "${ST9}" between s_d_date and date_sub(e_d_date,1)
            and paid_out_type='BUY_BACK'
            and paid_out_date<='${ST9}'
            and  product_id in (${product_id_list})
            group by due_bill_no
        )schedule
        inner join
        (
            select due_bill_no,
            product_id
            from ods_new_s.loan_info
            where
            "${ST9}" between s_d_date and date_sub(e_d_date,1)
            and overdue_days>=180
            and product_id in (${product_id_list})
         )loan on schedule.due_bill_no=loan.due_bill_no
         group by loan.product_id
)buyback on  biz_conf.product_id=buyback.product_id
