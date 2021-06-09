--create table if not exists dw_cps.dw_user_information_stat_d(
--    user_hash_no                                           string                  comment '用户身份证编码',
--    overdue_days                                           int                     comment '用户所有逾期借据中，逾期天数最大一天',
--    loan_num                                               decimal(15,4)           comment '用户所有借据数',
--    settle_loan_num                                        decimal(15,4)           comment '用户已结清借据数',
--    loan_amount                                            decimal(15,4)           comment '用户所有借据的放款金额',
--    remain_amount                                          decimal(15,4)           comment '所有借据未还剩余金额',
--    remain_principal                                       decimal(15,4)           comment '所有借据剩余未还本金',
--    remain_interest                                        decimal(15,4)           comment '所有借据剩余未还利息',
--    paid_amount_acc                                        decimal(15,4)           comment '所有借据已还金额',
--    paid_principal_acc                                     decimal(15,4)           comment '所有借据已还本金',
--    paid_interest_acc                                      decimal(15,4)           comment '所有借据已还利息',
--    paid_penalty_acc                                       decimal(15,4)           comment '所有借据已还罚息',
--    paid_svc_fee_acc                                       decimal(15,4)           comment '所有借据已还服务费',
--    overdue_amount                                         decimal(15,4)           comment '所有借据逾期金额',
--    overdue_principal                                      decimal(15,4)           comment '所有借据逾期本金',
--    overdue_interest                                       decimal(15,4)           comment '所有借据逾期利息',
--    early_settle_loan_num                                  decimal(15,4)           comment '本日用户提前结清借据数',
--    early_settle_amount                                    decimal(15,4)           comment '本日用户提前结清金额',
--    early_settle_principal                                 decimal(15,4)           comment '本日用户提前结清本金',
--    early_settle_interest                                  decimal(15,4)           comment '本日用户提前结清利息',
--    early_settle_penalty                                   decimal(15,4)           comment '本日用户提前结清罚息',
--    early_settle_svc_fee                                   decimal(15,4)           comment '本日用户提前结清服务费',
--    should_repay_amount                                    decimal(15,4)           comment '本日实际应还金额，（上月未结清，应还日在本月）',
--    should_repay_principal                                 decimal(15,4)           comment '本日实际应还本金，（上月未结清，应还日在本月）',
--    should_repay_interest                                  decimal(15,4)           comment '本日实际应还利息，（上月未结清，应还日在本月）',
--    paid_amount                                            decimal(15,4)           comment '本日所有借据实还金额，（实还日在本月）',
--    paid_principal                                         decimal(15,4)           comment '本日所有借据实还本金，（实还日在本月）',
--    paid_interest                                          decimal(15,4)           comment '本日所有借据实还利息，（实还日在本月）',
--    paid_penalty                                           decimal(15,4)           comment '本日所有借据实还罚息，（实还日在本月）',
--    paid_svc_fee                                           decimal(15,4)           comment '本日所有借据实还服务费，（实还日在本月）',
--    paid_fee                                               decimal(15,4)           comment '本日所有借据实还息费，（实还日在本月）',
--    loan_apply_num                                         decimal(15,2)           comment '本日该用户申请笔数',
--    loan_approval_num                                      decimal(15,2)           comment '本日该用户申请通过笔数',
--    loan_apply_amount                                      decimal(15,4)           comment '本日该用户申请金额',
--    loan_approval_amount                                   decimal(15,4)           comment '本日该用户申请通过金额',
--    loan_apply_num_accumulate                              decimal(15,4)           comment '累计到本日该用户申请笔数',
--    loan_approval_num_accumulate                           decimal(15,4)           comment '累计到本日该用户申请通过笔数',
--    loan_apply_amount_accumulate                           decimal(15,4)           comment '累计到本日该用户申请金额',
--    loan_approval_amount_accumulate                        decimal(15,4)           comment '累计到本日该用户申请通过金额',
--    weight_interest                                        decimal(15,4)           comment '该用户本日放款的借据的加权利息和',
--    weight_interest_accumulate                             decimal(15,4)           comment '累计到本日该用户借据的加权利息和',
--    first_term_over_principal                              decimal(15,4)           comment '第一期应还日在当月，且未还款的借据的应还本金',
--    first_term_loan_amount                                 decimal(15,4)           comment '第一期应还日在当月的借据的放款金额',
--
--    overdue_bill                                           decimal(4,0)            comment '该用户当前逾期借据数',
--    is_overdue30_user                                      decimal(2,0)            comment '该用户当前是否30+逾期 1逾期 0正常',
--    is_overdue60_user                                      decimal(2,0)            comment '该用户当前是否60+逾期 1逾期 0正常',
--    is_overdue90_user                                      decimal(2,0)            comment '该用户当前是否90+逾期 1逾期 0正常'
--)comment '用户级统计信息表'
--partitioned by (biz_date string comment '观察日',product_id string comment '产品号')
--stored as parquet;

--dw_user_information_stat_d 
--dw_user_stat_overdue_num_day
--set hivevar:db_suffix=;
--set hivevar:ST9=2021-02-02;
--set hivevar:product_id_list='001801';

set hive.exec.input.listing.max.threads=50;
set tez.grouping.min-size=50000000;
set tez.grouping.max-size=50000000;
set hive.exec.reducers.max=500;
set hive.auto.convert.join=true;
set hive.mapjoin.smalltable.filesize=209715200;
set hive.auto.convert.join.noconditionaltask=true;

-- 多个mapjoin 转换为1个时，限制输入的最大的数据量 影响tez，默认10m 
set hive.auto.convert.join.noconditionaltask.size =209715200;
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

set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=10;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=512000000;
set mapred.min.split.size.per.rack=512000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.exec.reducers.bytes.per.reducer=1024000000; --| |#每个节点的reduce 默认是处理1G大小的数据
set hive.exec.reducers.max=50;

set hivevar:product_id_list='001601','001602','001603','001802','002001','001906','002006','001901','001801','002002','001902','DIDI201908161538','002501','002601','002602';
--set hivevar:product_id_list='001801';

--a_00157aa42eb202a89e02cadc1466e2a001dd1c5bbea5ddb87d3e5dbfbfcadda2
insert overwrite table dw${db_suffix}.dw_user_information_stat_d partition(biz_date='${ST9}',product_id)

select
coalesce(user_hash_loan_info.user_hash_no,loan_apply.user_hash_no)                                                                   as user_hash_no,
max(nvl(user_hash_loan_info.overdue_days,0))                                                                                         as overdue_days,
count(user_hash_loan_info.due_bill_no)                                                                                               as loan_num,
sum(if(user_hash_loan_info.loan_status='F',1,0))                                                                                     as settle_loan_num,
sum(nvl(user_hash_loan_info.loan_init_principal,0))                                                                                  as loan_amount,
sum(nvl(user_hash_loan_info.remain_amount,0))                                                                                        as remain_amount,
sum(nvl(user_hash_loan_info.remain_principal,0))                                                                                     as remain_principal,
sum(nvl(user_hash_loan_info.remain_interest,0))                                                                                      as remain_interest,
sum(nvl(user_hash_loan_info.paid_amount,0))                                                                                          as paid_amount_acc,
sum(nvl(user_hash_loan_info.paid_principal,0))                                                                                       as paid_principal_acc,
sum(nvl(user_hash_loan_info.paid_interest,0))                                                                                        as paid_interest_acc,
sum(nvl(user_hash_loan_info.paid_penalty,0))                                                                                         as paid_penalty_acc,
sum(nvl(user_hash_loan_info.paid_svc_fee,0))                                                                                         as paid_svc_fee_acc,
sum(nvl(user_hash_loan_info.overdue_principal,0))+sum(nvl(user_hash_loan_info.overdue_interest,0))                                   as overdue_amount,
sum(nvl(user_hash_loan_info.overdue_principal,0))                                                                                    as overdue_principal,
sum(nvl(user_hash_loan_info.overdue_interest,0))                                                                                     as overdue_interest,
count(distinct early_settle.due_bill_no)                                                                                             as early_settle_loan_num,
sum(if(early_settle.early_settle_date=repay_hst.biz_date,nvl(amount,0),0))                                                           as early_settle_amount,
sum(if(early_settle.early_settle_date=repay_hst.biz_date,nvl(pricinpal,0),0))                                                        as early_settle_pricinpal,
sum(if(early_settle.early_settle_date=repay_hst.biz_date,nvl(interest,0),0))                                                         as early_settle_interest,
sum(if(early_settle.early_settle_date=repay_hst.biz_date,nvl(penalty,0),0))                                                          as early_settle_penalty,
sum(if(early_settle.early_settle_date=repay_hst.biz_date,nvl(svc_fee,0),0))                                                          as early_settle_svc_fee,
sum(nvl(should_repay.should_repay_amount,0))                                                                                         as should_repay_amount,
sum(nvl(should_repay.should_repay_principal,0))                                                                                      as should_repay_principal,
sum(nvl(should_repay.should_repay_interest,0))                                                                                       as should_repay_interest,

sum(nvl(repay_hst.amount,0))                                                                                                         as paid_amount,
sum(nvl(repay_hst.pricinpal,0))                                                                                                      as paid_principal,
sum(nvl(repay_hst.interest,0))                                                                                                       as paid_interest,
sum(nvl(repay_hst.penalty,0))                                                                                                        as paid_penalty,
sum(nvl(repay_hst.svc_fee,0))                                                                                                        as paid_svc_fee,
sum(nvl(repay_hst.interest,0))+sum(nvl(repay_hst.penalty,0))+sum(nvl(repay_hst.svc_fee,0))                                           as paid_fee,

max(loan_apply_num)                                                                                                                  as loan_apply_num,
max( loan_approval_num                              )                                                                                as loan_approval_num,
max( loan_apply_amount                              )                                                                                as loan_apply_amount,
max( loan_approval_amount                               )                                                                            as loan_approval_amount,

max( loan_apply_num_accumulate                              )                                                                        as loan_apply_num_accumulate,
max( loan_approval_num_accumulate                               )                                                                    as loan_approval_num_accumulate,
max( loan_apply_amount_accumulate                               )                                                                    as loan_apply_amount_accumulate,
max( loan_approval_amount_accumulate                                )                                                                as loan_approval_amount_accumulate,

sum(if(loan_lending.due_bill_no is not null and loan_lending.biz_date='${ST9}',weight_interest,0))                                   as weight_interest,
sum(if(loan_lending.due_bill_no is not null ,weight_interest,0))                                                                     as weight_interest_accumulate,  

sum(nvl(should_repay.first_term_over_principal,0))                                                                                   as first_term_over_principal,
sum(nvl(should_repay.first_term_loan_amount,0))                                                                                      as first_term_loan_amount,

sum(if(user_hash_loan_info.overdue_days > 0, 1, 0))                                                                                  as overdue_bill,
max(if(user_hash_loan_info.overdue_days > 30,1, 0))                                                                                  as is_overdue30_user,
max(if(user_hash_loan_info.overdue_days > 60,1, 0))                                                                                  as is_overdue60_user,
max(if(user_hash_loan_info.overdue_days > 90,1, 0))                                                                                  as is_overdue90_user,

coalesce(user_hash_loan_info.product_id,loan_apply.product_id)                                                                       as product_id
from
(
select
 loan_apply.user_hash_no
,loan_apply.due_bill_no
,loan_info.overdue_days       
--,loan_info.due_bill_no        
,loan_info.loan_status        
,loan_info.loan_init_principal
,loan_info.remain_amount      
,loan_info.remain_principal   
,loan_info.remain_interest    
,loan_info.paid_amount        
,loan_info.paid_principal     
,loan_info.paid_interest    
,loan_info.paid_penalty     
,loan_info.paid_svc_fee     
,loan_info.overdue_principal       
,loan_info.overdue_interest
,loan_info.product_id   
from
(
    select
        distinct                       --存在一笔借据 两个产品号的情况，例如一笔借据申请两次有一次失败，另外一次成功 申请的产品号虽然不同，但生成的借据号相同。防止数据发散。
        user_hash_no,
        due_bill_no
    from
    ods.loan_apply  where to_date(issue_time) <= '${ST9}'  
    and  product_id in (${product_id_list})
    --and due_bill_no="1120060420501247803433"
)loan_apply
 join(
    select
    *
    from ods${db_suffix}.loan_info
    where "${ST9}" between  s_d_date and date_sub(e_d_date,1) 
    and  product_id in (${product_id_list})
    --and due_bill_no="1120060420501247803433"
)loan_info
on loan_apply.due_bill_no=loan_info.due_bill_no
) user_hash_loan_info

--求加权利息
--
left join
(
select
due_bill_no
,product_id
,loan_original_principal * loan_init_interest_rate as weight_interest
,biz_date
from
ods${db_suffix}.loan_lending
where biz_date <= '${ST9}'
) loan_lending
on user_hash_loan_info.due_bill_no = loan_lending.due_bill_no
and user_hash_loan_info.product_id = loan_lending.product_id
-- 取本日提前结清日期
left join (
        select
            due_bill_no,
            product_id,
            paid_out_date     as early_settle_date
        from ods${db_suffix}.repay_schedule
        where "${ST9}" between  s_d_date and date_sub(e_d_date,1) 
        and  product_id in (${product_id_list})
        and loan_init_term=loan_term and  paid_out_date is not null and datediff(paid_out_date,should_repay_date) <0
        and paid_out_date='${ST9}'
)early_settle
on user_hash_loan_info.due_bill_no=early_settle.due_bill_no
and user_hash_loan_info.product_id =early_settle.product_id
left join (
 select
        due_bill_no,
        biz_date,
        product_id,
        sum(repay_amount)                                                as amount,
        sum(case bnp_type when 'Pricinpal' then repay_amount else 0 end) as pricinpal,
        sum(case bnp_type when 'Interest'  then repay_amount else 0 end) as interest,
        sum(case bnp_type when 'TERMFee'   then repay_amount else 0 end) as term_fee,
        sum(case bnp_type when 'SVCFee'    then repay_amount else 0 end) as svc_fee,
        sum(case bnp_type when 'Penalty'   then repay_amount else 0 end) as penalty
    from ods${db_suffix}.repay_detail
    where biz_date ='${ST9}' 
    and  product_id in (${product_id_list})
    --and due_bill_no="1120060420501247803433"
    group by due_bill_no,biz_date,product_id
)repay_hst
on user_hash_loan_info.due_bill_no=repay_hst.due_bill_no
and user_hash_loan_info.product_id =repay_hst.product_id
left join
(
        select
            due_bill_no,
            product_id,
            sum(if(should_repay_date='${ST9}' and (paid_out_date is null or paid_out_date>last_day(add_months('${ST9}',-1,'yyyy-MM-dd'))),should_repay_amount,0))                                  as should_repay_amount,
            sum(if(should_repay_date='${ST9}' and (paid_out_date is null or paid_out_date>last_day(add_months('${ST9}',-1,'yyyy-MM-dd'))),should_repay_principal,0))                               as should_repay_principal,
            sum(if(should_repay_date='${ST9}' and (paid_out_date is null or paid_out_date>last_day(add_months('${ST9}',-1,'yyyy-MM-dd'))),should_repay_interest,0))                                as should_repay_interest,
            sum(if(date_format(should_repay_date,'yyyy-MM')=date_format('${ST9}','yyyy-MM') and loan_term=1 and paid_out_date is null ,should_repay_principal,0))                                  as first_term_over_principal,
            sum(if(date_format(should_repay_date,'yyyy-MM')=date_format('${ST9}','yyyy-MM') and loan_term=1 ,loan_init_principal,0))                                                               as first_term_loan_amount
        from ods${db_suffix}.repay_schedule
        where "${ST9}" between  s_d_date and date_sub(e_d_date,1) 
        and  product_id in (${product_id_list})
        group by  due_bill_no, product_id
)should_repay
on user_hash_loan_info.due_bill_no=should_repay.due_bill_no
and user_hash_loan_info.product_id =should_repay.product_id
full join
(
    select
        user_hash_no,
        product_id,
        sum(if(loan_apply.biz_date='${ST9}',1,0))                                                                                            as loan_apply_num,
        sum(if(to_date(loan_apply.issue_time)='${ST9}'and apply_status in (1,4),1,0))                                                        as loan_approval_num,
        sum(if(loan_apply.biz_date='${ST9}',nvl(loan_amount_apply,0),0))                                                                     as loan_apply_amount,
        sum(if(to_date(loan_apply.issue_time)='${ST9}' and apply_status in (1,4),nvl(loan_amount_approval,0),0))                             as loan_approval_amount,
        
        sum(if(loan_apply.biz_date<='${ST9}',1,0))                                                                                           as loan_apply_num_accumulate,
        sum(if(to_date(loan_apply.issue_time)<='${ST9}' and apply_status in (1,4),1,0))                                                      as loan_approval_num_accumulate,
        sum(if(loan_apply.biz_date<='${ST9}',nvl(loan_amount_apply,0),0))                                                                    as loan_apply_amount_accumulate,
        sum(if(to_date(loan_apply.issue_time)<='${ST9}' and apply_status in (1,4),nvl(loan_amount_approval,0),0))                            as loan_approval_amount_accumulate
    from
    ods.loan_apply  where (to_date(loan_apply.issue_time)<='${ST9}' or loan_apply.biz_date <='${ST9}') and  product_id in (${product_id_list})
    group by user_hash_no,product_id
    --and due_bill_no="1120060420501247803433"
) loan_apply
on user_hash_loan_info.user_hash_no = loan_apply.user_hash_no
and user_hash_loan_info.product_id = loan_apply.product_id
group by  
coalesce(user_hash_loan_info.user_hash_no,loan_apply.user_hash_no)
,coalesce(user_hash_loan_info.product_id,loan_apply.product_id)
;
