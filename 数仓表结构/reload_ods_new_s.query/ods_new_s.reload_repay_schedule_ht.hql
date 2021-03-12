set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;        -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=true;            -- 关闭自动 MapJoin
set hive.support.quoted.identifiers=None;
set spark.sql.autoBroadcastJoinThreshold=20485760;
--DROP TABLE IF EXISTS `ods_new_s.repay_schedule_tmp`;
--CREATE TABLE IF NOT EXISTS `ods_new_s.repay_schedule_tmp` like `ods_new_s.repay_schedule`;


--汇通修数：
--还款计划更改点：
--1 取最新一天的还款计划和 每个快照日里的 已还期数做对比，时间为空则替换对应的 时间
--2 ecas_repay_schedule_ht_repair 修数表 ,表结构和 ecas_repay_schedule 结构一致  ，修复资产数据晚给的问题
--3 根据还款计划当天的状态值 paid_out_date  更新还款计划状态 schedule_status
--4 根据还款计划快照日 当日期次是否有paid_out_date 更新上面的已还金额









insert overwrite table ods_new_s.repay_schedule_tmp partition(is_settled = 'no',product_id)
select `(is_settled)?+.+`
from ods_new_s.repay_schedule
where is_settled = 'no'
  -- and to_date(effective_time) <= date_add('${ST9}',1)
  and s_d_date < '${ST9}'
  and product_id in ('001601','001602','001603')
  and due_bill_no not in ('DD000230362019111701130061a514','DD000230362019121511470083cc26','DD0002303620200111210700a1ceb3',
  'DD0002303620200206232600de511b','DD0002303620200208195000ae8354','DD0002303620200307225700e545de',
  'DD0002303620200308222600ebbea9','DD00023036202003191421004c1251','DD0002303620200404090400770962',
  'DD0002303620200409104700bfb018')
distribute by loan_active_date,should_repay_date
-- limit 1
;


-- DROP TABLE IF EXISTS `ods_new_s.repay_schedule_intsert`;
-- CREATE TABLE IF NOT EXISTS `ods_new_s.repay_schedule_intsert` like `ods_new_s.repay_schedule`;

with repay_hst_repair as (
select
      repayhst.due_bill_no,repayhst.term,bnp_type,repayhst.d_date,repay_amt,
      if(repair_hst.order_id is not null,repair_hst.paid_out_date,repayhst.txn_date) as txn_date
         from (
                select * from   ods.ecas_repay_hst  where 1 > 0  and d_date ='${d_date}'   and p_type in ('ddht')   and txn_date <= date_add('${ST9}',30)
                --11月修数  删除掉汇通的两笔线下还款的罚息实还数据
                and payment_id not in ('000016043097811admin000068000001','000016043095431admin000068000001')
          )repayhst
           left join (
          select distinct order_id,paid_out_date  from  ods.schedule_repay_order_info_ddht  where biz_date='${d_date}' and product_id in ('001601','001602','001603')
          )repair_hst on repayhst.order_id=repair_hst.order_id

),
ods_new_s_repay_schedule as (
    select
        repay_schedule.product_id,
        repay_schedule.schedule_id,
        repay_schedule.due_bill_no,
        repay_schedule.loan_init_principal,
        repay_schedule.loan_init_term,
        repay_schedule.loan_term,
        repay_schedule.start_interest_date,
        repay_schedule.curr_bal,
        repay_schedule.should_repay_date,
        repay_schedule.should_repay_date_history,
        repay_schedule.grace_date,
        repay_schedule.should_repay_amount,
        repay_schedule.should_repay_principal,
        repay_schedule.should_repay_interest,
        repay_schedule.should_repay_term_fee,
        repay_schedule.should_repay_svc_fee,
        repay_schedule.should_repay_penalty,
        repay_schedule.should_repay_mult_amt,
        repay_schedule.should_repay_penalty_acru,
        repay_schedule.schedule_status,
        repay_schedule.schedule_status_cn,
        repay_schedule.paid_out_date,
        repay_schedule.paid_out_type,
        repay_schedule.paid_out_type_cn,
        repay_schedule.paid_amount,
        repay_schedule.paid_principal,
        repay_schedule.paid_interest,
        repay_schedule.paid_term_fee,
        repay_schedule.paid_svc_fee,
        repay_schedule.paid_penalty,
        repay_schedule.paid_mult,
        repay_schedule.reduce_amount,
        repay_schedule.reduce_principal,
        repay_schedule.reduce_interest,
        repay_schedule.reduce_term_fee,
        repay_schedule.reduce_svc_fee,
        repay_schedule.reduce_penalty,
        repay_schedule.reduce_mult_amt,
        repay_schedule.d_date,
        '3000-12-31' as e_d_date,
        cast(datefmt(repay_schedule.create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as create_time,
        cast(datefmt(repay_schedule.update_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
        cast('3000-12-31 00:00:00' as timestamp) as expire_time
    from (
             select
                 product_code                                                                             as product_id,
                 schedule_id                                                                              as schedule_id,
                 out_side_schedule_no                                                                     as out_side_schedule_no,
                 due_bill_no                                                                              as due_bill_no,
                 loan_init_prin                                                                           as loan_init_principal,
                 loan_init_term                                                                           as loan_init_term,
                 curr_term                                                                                as loan_term,
                 start_interest_date                                                                      as start_interest_date,
                 curr_bal                                                                                 as curr_bal,
                 pmt_due_date                                                                             as should_repay_date,
                 origin_pmt_due_date                                                                      as should_repay_date_history,
                 grace_date                                                                               as grace_date,
                 (nvl(due_term_prin,0) +nvl(due_term_int,0)+ nvl(due_term_fee,0) + nvl(due_svc_fee,0) + nvl(due_penalty,0) + nvl(due_mult_amt,0)) as should_repay_amount,
                 nvl(due_term_prin,0)                                                                            as should_repay_principal,
                 nvl(due_term_int,0)                                                                             as should_repay_interest,
                 nvl(due_term_fee,0)                                                                             as should_repay_term_fee,
                 nvl(due_svc_fee,0)                                                                              as should_repay_svc_fee,
                 nvl(due_penalty ,0)                                                                             as should_repay_penalty,
                 nvl(due_mult_amt,0)                                                                             as should_repay_mult_amt,
                 nvl(penalty_acru,0)                                                                             as should_repay_penalty_acru,
                 case
                     when due_bill_no="1000004836" and d_date >='2021-11-23' then 'F'
                     when due_bill_no="1000000381" and d_date >='2020-08-17' then 'F'
                     when due_bill_no="1000000163" and d_date >='2020-09-29' and (curr_term=0 or curr_term between 6 and 36) then 'F'
                     when due_bill_no="1000000403" and d_date >='2020-11-09' and (curr_term=0 or curr_term between 3 and 36) then 'F'
                     when  due_bill_no='1000000275' and d_date >='2020-02-12' then 'F'
                     when paid_out_date is not null and due_bill_no!='1000000275' then 'F'
                     when paid_out_date is null and '${ST9}' >=nvl(origin_pmt_due_date,pmt_due_date) and curr_term!=0 then 'O'
                     when nvl(repayhst.repayhst_paid_principal,0) >= due_term_prin and due_term_prin!=0 then 'F'
                     when '${ST9}' < pmt_due_date and curr_term!=0 then 'N'
                     when '${ST9}' >= pmt_due_date and curr_term!=0 then 'O'
                    when '${ST9}' < paid_out_date and repayhst_paid_principal=0 and curr_term!=0 then 'N'
                     else schedule_status end                                                                 as schedule_status,
                 case
                        case
                            when due_bill_no="1000004836" and d_date >='2021-11-23' then 'F'
                             when due_bill_no="1000000381" and d_date >='2020-08-17' then 'F'
                             when  due_bill_no='1000000275' and d_date >='2020-02-12' then 'F'
                             when due_bill_no="1000000403" and d_date >='2020-11-09' and (curr_term=0 or curr_term between 3 and 36) then 'F'
                             when  due_bill_no='1000000163' and d_date >='2020-09-29' and (curr_term=0 or curr_term between 6 and 36) then 'F'
                             when paid_out_date is not null and due_bill_no!='1000000275' then 'F'
                             when paid_out_date is null and '${ST9}' >=nvl(origin_pmt_due_date,pmt_due_date) and curr_term!=0 then 'O'
                             when nvl(repayhst.repayhst_paid_principal,0) >= due_term_prin and due_term_prin!=0 then 'F'
                             when '${ST9}' < pmt_due_date and curr_term!=0 then 'N'
                             when '${ST9}' >= pmt_due_date and curr_term!=0 then 'O'
                             when '${ST9}' < paid_out_date and repayhst_paid_principal=0 and curr_term!=0 then 'N'
                         else schedule_status end
                     when 'N' then '正常'
                     when 'O' then '逾期'
                     when 'F' then '已还清'
                     else schedule_status end                                                                  as schedule_status_cn,
                 case when due_bill_no="1000000275" and d_date >='2020-01-21' and  d_date<'2020-02-12' then null
                 when due_bill_no="1000000275" and d_date >='2020-02-12' then '2020-02-12'
                 when due_bill_no="1000004836" and d_date >='2021-11-23' then '2020-11-23'
                  else paid_out_date        end                                                                 as paid_out_date,
                 case
                     when product_code = '001801' and due_bill_no = '1120060510300559326682' and d_date between '2020-06-06' and '${ST9}' then 'PRE_SETTLE'
                     when due_bill_no="1000004836" and d_date >='2020-11-23'  then 'REDEMPTION'
                     when due_bill_no="1000000163" and d_date >='2020-09-29' and (curr_term=0 or curr_term between 6 and 36) then "REDEMPTION"
                     when due_bill_no="1000000403" and d_date >='2020-11-09' and (curr_term=0 or curr_term between 3 and 36) then "REDEMPTION"
                     when due_bill_no="1000000275" and d_date >='2020-01-21' and  d_date<'2020-02-12' then null
                     else if(paid_out_type = 'BUY_BACKAV','BUY_BACK',paid_out_type) end                          as paid_out_type,
                 case
                     case
                         when product_code = '001801' and due_bill_no = '1120060602543338694112' and d_date between '2020-06-07' and '${ST9}' then 'PRE_SETTLE'
                         when due_bill_no="1000000275" and d_date >='2020-01-21' and  d_date<'2020-02-12' then null
                         when due_bill_no="1000004836" and d_date >='2020-11-23'  then 'REDEMPTION'
                         when due_bill_no="1000000163" and d_date >='2020-09-29' and (curr_term=0 or curr_term between 6 and 36)  then "REDEMPTION"
                         when due_bill_no="1000000403" and d_date >='2020-11-09' and (curr_term=0 or curr_term between 3 and 36) then "REDEMPTION"
                         else if(paid_out_type = 'BUY_BACKAV','BUY_BACK',paid_out_type) end
                     when 'BANK_REF'          then '退票结清'
                     when 'BUY_BACK'          then '资产回购'
                     when 'CAPITAL_VERI'      then '资产核销'
                     when 'DISPOSAL'          then '处置结束'
                     when 'NORMAL_SETTLE'     then '正常结清'
                     when 'OVER_COMP'         then '逾期代偿'
                     when 'OVERDUE_SETTLE'    then '逾期结清'
                     when 'PRE_SETTLE'        then '提前结清'
                     when 'REDEMPTION'        then '赎回'
                     when 'REFUND'            then '退车'
                     when 'REFUND_SETTLEMENT' then '退票结清'
                     else if(paid_out_type = 'BUY_BACKAV','BUY_BACK',paid_out_type) end                           as paid_out_type_cn,
                case when   due_bill_no='1000000275' and d_date >='2020-02-12' then due_term_prin
                else
                 nvl(repayhst_paid_principal,0) +
                 nvl(repayhst_paid_interest, 0) +
                 nvl(repayhst_paid_term_fee,  0) +
                 nvl(repayhst_paid_svc_fee,   0) +
                 nvl(repayhst_paid_penalty,    0) +
                 nvl(repayhst_paid_mult,     0)           end                                                        as paid_amount,
                 case when   due_bill_no='1000000275' and d_date >='2020-02-12' then due_term_prin
                 else
                 nvl(repayhst.repayhst_paid_principal,0)                                     end                     as paid_principal,
                 nvl(repayhst.repayhst_paid_interest,0)                                                              as paid_interest,
                 nvl(repayhst_paid_term_fee,  0)                                                                     as paid_term_fee,
                 nvl(repayhst_paid_svc_fee,    0)                                                                     as paid_svc_fee,
                 nvl(repayhst_paid_penalty,    0)                                                                     as paid_penalty,
                 nvl(repayhst_paid_mult,      0)                                                                     as paid_mult,
                 least(nvl(reduce_term_prin,0),nvl(due_term_prin,0))+least(nvl(due_term_int,0),nvl(reduce_term_int,0))+least(nvl(reduce_term_fee,0),nvl(due_term_fee,0) )
                 +least(nvl(reduce_svc_fee,0) ,nvl(due_svc_fee,0)  )+least(if(due_bill_no='1000000222' ,0,nvl(reduce_penalty,0)), nvl(due_penalty,0) )+least(nvl(reduce_mult_amt,0), nvl(due_mult_amt,0)) as reduce_amount,
                 least(nvl(reduce_term_prin,0),nvl(due_term_prin,0))                                                    as reduce_principal,
                 least(nvl(reduce_term_int,0) ,nvl(due_term_int,0))                                                     as reduce_interest,
                 least(nvl(reduce_term_fee,0),nvl(due_term_fee,0) )                                                     as reduce_term_fee,
                 least(nvl(reduce_svc_fee,0) ,nvl(due_svc_fee,0)  )                                                     as reduce_svc_fee,
                 least(if(due_bill_no='1000000222',0,nvl(reduce_penalty,0)), nvl(due_penalty,0) )                       as reduce_penalty,
                 least(nvl(reduce_mult_amt,0), nvl(due_mult_amt,0))                                                     as reduce_mult_amt,
                 is_empty(create_time,create_user)                                                                      as create_time,
                 is_empty(lst_upd_time,lst_upd_user)                                                                    as update_time,
                 d_date                                                                                                 as d_date
             from (
             select
                    tmp.product_code,tmp.schedule_id,tmp.out_side_schedule_no,tmp.due_bill_no,tmp.loan_init_prin,tmp.loan_init_term,tmp.curr_term,tmp.start_interest_date,tmp.curr_bal,tmp.pmt_due_date,tmp.origin_pmt_due_date,
                    tmp.grace_date,tmp.due_term_prin,tmp.due_term_int,tmp.due_term_fee,
                    case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 600
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0 then 600
                         else tmp.due_svc_fee end                                                       as due_svc_fee,
                    case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 1559.51
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0  then 1245
                         else tmp.due_penalty end                                                        as due_penalty,
                    tmp.due_mult_amt,tmp.penalty_acru,tmp.d_date,

                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.paid_out_type
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then "REDEMPTION"
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then "REDEMPTION"
                    else tmp.paid_out_type end as paid_out_type,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduced_amt else tmp.reduced_amt end as reduced_amt,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_term_prin else tmp.reduce_term_prin end as reduce_term_prin,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_term_int else tmp.reduce_term_int end as reduce_term_int,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_term_fee else tmp.reduce_term_fee end as reduce_term_fee,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_svc_fee
                         when tmp.due_bill_no="1000000061" then 0
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 600
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0 then 600
                        else tmp.reduce_svc_fee                                                                                                      end as reduce_svc_fee,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_penalty
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 728.51
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0  then 48.56 else tmp.reduce_penalty end                           as reduce_penalty,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_mult_amt else tmp.reduce_mult_amt end as reduce_mult_amt,
                    tmp.create_time,tmp.create_user,tmp.lst_upd_time,tmp.lst_upd_user,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then '2020-08-17'
                         when  tmp.due_bill_no="1000004836" and  tmp.d_date >='2021-11-23' then '2020-11-23'
                        when tmp.due_bill_no="1000001858" and tmp.d_date ='2020-09-21' and tmp.curr_term=0 then null
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                         when tmp.paid_out_date >tmp.d_date then null
                         when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                         when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                        else tmp.paid_out_date end as paid_out_date,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then 'F'
                         when tmp.paid_out_date=new_schedule.paid_out_date then new_schedule.schedule_status
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' then 'F'
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' then 'F'
                         when  tmp.due_bill_no="1000004836" and  tmp.d_date >='2021-11-23' then 'F'
                        else tmp.schedule_status end as schedule_status,
                    concat(tmp.due_bill_no,'::',tmp.curr_term) as due_bill_no_curr_term
             from (
             select  * from ods.ecas_repay_schedule  where d_date = '${ST9}' and p_type in ('ddht')   and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
             union all
             select  * from ods.ecas_repay_schedule_ht_repair  where d_date = '${ST9}' and p_type in ('ddht')   and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
             )tmp
            left join
                (
                    select due_bill_no,curr_term,paid_out_date,schedule_status,paid_out_type,reduced_amt,reduce_term_prin,reduce_term_int,reduce_term_fee,reduce_svc_fee,reduce_penalty,reduce_mult_amt
                    from ods.ecas_repay_schedule where d_date='${d_date}' and p_type='ddht' and product_code in ('001601','001602','001603')
                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10) and schedule_id not in ('000016006898691admin000068000001')
                )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
             ) as schedule
             left join (
                select
                    schedule.due_bill_no_curr_term as repayhst_due_bill_no_term,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_principal end ) as repayhst_paid_principal,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_interest end ) as repayhst_paid_interest,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_term_fee end ) as repayhst_paid_term_fee,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_svc_fee end ) as repayhst_paid_svc_fee,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_penalty end ) as repayhst_paid_penalty,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_mult end ) as repayhst_paid_mult
                    from
                     (
                     select
                     due_bill_no,
                     curr_term,
                     due_bill_no_curr_term,
                     d_date
                     from
                     (
                        select
                        tmp.due_bill_no,
                        tmp.curr_term,
                        concat(tmp.due_bill_no,'::',cast(tmp.curr_term as string)) as due_bill_no_curr_term,
                        tmp.d_date,
                        case when  tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then '2020-08-17'
                              when  tmp.due_bill_no="1000004836" and  tmp.d_date >='2021-11-23' then '2020-11-23'
                            when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                             when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                              when tmp.due_bill_no="1000001858" and tmp.d_date ='2020-09-21' and tmp.curr_term=0 then null
                              when tmp.paid_out_date >tmp.d_date then null
                              when  tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                             when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                          else tmp.paid_out_date end as paid_out_date
                        from
                         (
                         select  * from ods.ecas_repay_schedule where p_type="ddht" and d_date= '${ST9}'  and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                         union all
                         --汇通修数表
                         select  * from ods.ecas_repay_schedule_ht_repair where p_type="ddht" and d_date= '${ST9}' and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                         )tmp
                        left join (
                        -- 最新一天的还款计划 更新还款计划上的结清日期
                         select due_bill_no,curr_term,paid_out_date,schedule_status from
                           ods.ecas_repay_schedule where d_date='${d_date}' and p_type='ddht' and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                          and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10)
                        )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
                     )tmp1
                     where (tmp1.paid_out_date is not null or curr_term=0)
                     )schedule
                     left join
                     (
                     select
                        due_bill_no,term,
                        concat(due_bill_no,'::',cast(term as string)) as due_bill_no_curr_term,
                        sum(if(txn_date <='${ST9}' and bnp_type = 'Pricinpal',        repay_amt,0)) as paid_principal,
                        sum(if(txn_date <='${ST9}' and bnp_type = 'Interest',         repay_amt,0)) as paid_interest,
                        sum(if(txn_date <='${ST9}' and bnp_type = 'TERMFee',          repay_amt,0)) as paid_term_fee,
                        sum(if(txn_date <='${ST9}' and bnp_type = 'SVCFee',           repay_amt,0)) as paid_svc_fee,
                        sum(if(txn_date <='${ST9}' and bnp_type = 'Penalty',          repay_amt,0))  as paid_penalty,
                        sum(if(txn_date <='${ST9}' and bnp_type = 'LatePaymentCharge',repay_amt,0)) as paid_mult,
                        sum(if(txn_date <='${ST9}' and  term!=0 and bnp_type = 'Pricinpal',1,0)) as paid_term
                        from repay_hst_repair
                      group by due_bill_no,term,d_date,concat(due_bill_no,'::',term)
                     )repay_hst on  schedule.due_bill_no_curr_term =repay_hst.due_bill_no_curr_term
                     group by schedule.due_bill_no_curr_term
             ) as repayhst
             on  due_bill_no_curr_term = repayhst_due_bill_no_term
         ) as repay_schedule
        left join (
        select
            product_code                                              as product_id,
            schedule_id                                               as schedule_id,
            due_bill_no                                               as due_bill_no,
            loan_init_prin                                            as loan_init_principal,
            loan_init_term                                            as loan_init_term,
            curr_term                                                 as loan_term,
            start_interest_date                                       as start_interest_date,
            curr_bal                                                  as curr_bal,
            pmt_due_date                                              as should_repay_date,
            origin_pmt_due_date                                       as should_repay_date_history,
            grace_date                                                as grace_date,
            nvl(due_term_prin,0)                                      as should_repay_principal,
            nvl(due_term_int,0)                                       as should_repay_interest,
            nvl(due_term_fee,0)                                       as should_repay_term_fee,
            nvl(due_svc_fee,0)                                        as should_repay_svc_fee,
            nvl(due_penalty ,0)                                       as should_repay_penalty,
            nvl(due_mult_amt,0)                                       as should_repay_mult_amt,
            penalty_acru                                              as should_repay_penalty_acru,
            case    when  due_bill_no='1000000381' and d_date >='2020-08-17' then 'F'
                    when  due_bill_no='1000000275' and d_date >='2020-02-12' then 'F'
                    when  due_bill_no="1000004836" and  d_date >='2021-11-23' then 'F'
                    when due_bill_no="1000000163" and d_date >='2020-09-29' and (curr_term=0 or curr_term between 6 and 36) then 'F'
                    when due_bill_no="1000000403" and d_date >='2020-11-09' and (curr_term=0 or curr_term between 3 and 36) then 'F'
                    when paid_out_date is not null and due_bill_no!='1000000275' then 'F'
                    when paid_out_date is null and date_sub('${ST9}',1) >=nvl(origin_pmt_due_date,pmt_due_date) and curr_term!=0 then 'O'
                    when nvl(repayhst_yest.repayhst_paid_principal,0) >= due_term_prin and due_term_prin!=0 then 'F'
                    when date_sub('${ST9}',1) < pmt_due_date and curr_term!=0 then 'N'
                    when date_sub('${ST9}',1) < paid_out_date and curr_term!=0 and  repayhst_yest.repayhst_paid_principal=0 then 'N'
                    when date_sub('${ST9}',1) >= pmt_due_date and curr_term!=0  then 'O'
                    else schedule_status end                                                                 as schedule_status,
            case
                when  due_bill_no='1000000381' and d_date >='2020-08-17' then '2020-08-17'
                when  due_bill_no="1000004836" and  d_date >='2021-11-23' then '2020-11-23'
                when due_bill_no="1000000163" and d_date >='2020-09-29' and (curr_term=0 or curr_term between 6 and 36) then '2020-09-29'
                when due_bill_no="1000000403" and d_date >='2020-11-09' and (curr_term=0 or curr_term between 3 and 36)  then '2020-11-09'
                when due_bill_no="1000000275" and   d_date<'2020-02-12' then null
               when due_bill_no="1000000275" and d_date >='2020-02-12' then '2020-02-12'
                else paid_out_date        end                                                            as paid_out_date,
            case
                 when due_bill_no="1000000275" and  d_date<'2020-02-12' then null
                 when due_bill_no="1000004836" and d_date >='2020-11-23'  then 'REDEMPTION'
                 when due_bill_no="1000000163" and d_date >='2020-09-29' and (curr_term=0 or curr_term between 6 and 36) then "REDEMPTION"
                 when due_bill_no="1000000403" and d_date >='2020-11-09' and (curr_term=0 or curr_term between 3 and 36) then "REDEMPTION"
                 else if(paid_out_type = 'BUY_BACKAV','BUY_BACK',paid_out_type) end                       as paid_out_type,
           case when  due_bill_no='1000000275' and d_date >='2020-02-12' then due_term_prin
           else  nvl(repayhst_yest.repayhst_paid_principal,0)                                     end    as paid_principal,
            nvl(repayhst_yest.repayhst_paid_interest,0)                                                  as paid_interest,
            nvl(repayhst_paid_term_fee,  0)        as paid_term_fee,
            nvl(repayhst_paid_svc_fee,    0)        as paid_svc_fee,
            nvl(repayhst_paid_penalty,    0)        as paid_penalty,
            nvl(repayhst_paid_mult,      0)        as paid_mult,
         least(nvl(reduce_term_prin,0),nvl(due_term_prin,0))+least(nvl(due_term_int,0),nvl(reduce_term_int,0))+least(nvl(reduce_term_fee,0),nvl(due_term_fee,0) )
                 +least(nvl(reduce_svc_fee,0) ,nvl(due_svc_fee,0)  )+least(if(due_bill_no='1000000222' ,0,nvl(reduce_penalty,0)), nvl(due_penalty,0) )+least(nvl(reduce_mult_amt,0), nvl(due_mult_amt,0)) as reduce_amount,
           least(nvl(reduce_term_prin,0),nvl(due_term_prin,0))                                                                                                               as reduce_principal,
           least(nvl(reduce_term_int,0) ,nvl(due_term_int,0))                                                                                                                as reduce_interest,
           least(nvl(reduce_term_fee,0),nvl(due_term_fee,0) )                                                                                                                as reduce_term_fee,
           least(nvl(reduce_svc_fee,0) ,nvl(due_svc_fee,0)  )                                                                                                                as reduce_svc_fee,
           least(if(due_bill_no='1000000222',0,nvl(reduce_penalty,0)), nvl(due_penalty,0) )                                                                                  as reduce_penalty,
           least(nvl(reduce_mult_amt,0), nvl(due_mult_amt,0))                                                                                                                as reduce_mult_amt
        from
        (select
                    tmp.product_code,tmp.schedule_id,tmp.out_side_schedule_no,tmp.due_bill_no,tmp.loan_init_prin,tmp.loan_init_term,tmp.curr_term,tmp.start_interest_date,tmp.curr_bal,tmp.pmt_due_date,tmp.origin_pmt_due_date,
                    tmp.grace_date,tmp.due_term_prin,tmp.due_term_int,tmp.due_term_fee,
                      case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 600
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0 then 600
                         else tmp.due_svc_fee end                                                       as due_svc_fee,
                    case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 1559.51
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0  then 1245
                         else tmp.due_penalty end                                                        as due_penalty,
                    tmp.due_mult_amt,tmp.penalty_acru,tmp.d_date,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.paid_out_type
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36) then "REDEMPTION"
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then "REDEMPTION"
                    else tmp.paid_out_type end                                                           as paid_out_type,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduced_amt else tmp.reduced_amt end as reduced_amt,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_term_prin else tmp.reduce_term_prin end as reduce_term_prin,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_term_int else tmp.reduce_term_int end as reduce_term_int,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_term_fee else tmp.reduce_term_fee end as reduce_term_fee,
                       case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_svc_fee
                         when tmp.due_bill_no="1000000061" then 0
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 600
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0 then 600
                        else tmp.reduce_svc_fee                                                                                                      end as reduce_svc_fee,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_penalty
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 728.51
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0  then 48.56 else tmp.reduce_penalty end                           as reduce_penalty,

                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_mult_amt else tmp.reduce_mult_amt end as reduce_mult_amt,
                    tmp.create_time,tmp.create_user,tmp.lst_upd_time,tmp.lst_upd_user,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then '2020-08-17'
                         when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                         when tmp.paid_out_date >tmp.d_date then null
                         when tmp.due_bill_no="1000001858" and tmp.d_date ='2020-09-21' and tmp.curr_term=0 then null
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                         when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                         when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                        else tmp.paid_out_date end as paid_out_date,
                    case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then 'F'
                         when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' then 'F'
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' then 'F'
                          when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then 'F'
                         when tmp.paid_out_date=new_schedule.paid_out_date then new_schedule.schedule_status
                        else tmp.schedule_status end as schedule_status,
                    concat(tmp.due_bill_no,'::',tmp.curr_term) as due_bill_no_curr_term
        from
          (select * from  ods.ecas_repay_schedule where d_date = date_sub('${ST9}',1) and p_type in ('ddht')  and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
          union all
          select * from  ods.ecas_repay_schedule_ht_repair where d_date = date_sub('${ST9}',1) and p_type in ('ddht')  and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
          )tmp
           left join (
                    select due_bill_no,curr_term,paid_out_date,schedule_status,paid_out_type,reduced_amt,reduce_term_prin,reduce_term_int,reduce_term_fee,reduce_svc_fee,reduce_penalty,reduce_mult_amt
                    from ods.ecas_repay_schedule where d_date='${d_date}' and p_type='ddht' and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10)
            )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
         ) as schedule_yest
         left join
         (
          select
                    schedule.due_bill_no_curr_term as repayhst_due_bill_no_term,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_principal end ) as repayhst_paid_principal,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_interest end ) as repayhst_paid_interest,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_term_fee end ) as repayhst_paid_term_fee,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_svc_fee end ) as repayhst_paid_svc_fee,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_penalty end ) as repayhst_paid_penalty,
                    sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_mult end ) as repayhst_paid_mult
                    from
                     (
                     select
                     due_bill_no,
                     curr_term,
                     due_bill_no_curr_term,
                     d_date
                     from
                     (
                        select
                        tmp.due_bill_no,
                        tmp.curr_term,
                        concat(tmp.due_bill_no,'::',cast(tmp.curr_term as string)) as due_bill_no_curr_term,
                        tmp.d_date,
                        case when  tmp.due_bill_no='1000000381' and tmp.d_date >='2020-08-17' then '2020-08-17'
                              when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                             when tmp.due_bill_no="1000001858" and tmp.d_date ='2020-09-21' and tmp.curr_term=0 then null
                             when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                             when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                             when tmp.paid_out_date >tmp.d_date then null
                             when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                             when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                          else tmp.paid_out_date end as paid_out_date
                        from
                         (
                         select  * from ods.ecas_repay_schedule where p_type="ddht" and d_date= date_sub('${ST9}',1)  and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                         union all
                         --汇通修数表
                         select  * from ods.ecas_repay_schedule_ht_repair where p_type="ddht" and d_date= date_sub('${ST9}',1) and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                         )tmp
                        left join (
                        -- 最新一天的还款计划 更新还款计划上的结清日期
                         select due_bill_no,curr_term,paid_out_date,schedule_status from
                          ods.ecas_repay_schedule where d_date='${d_date}' and p_type='ddht' and product_code in ('001601','001602','001603')
                          and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10) and schedule_id not in ('000016006898691admin000068000001')
                        )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
                     )tmp1
                     where (tmp1.paid_out_date is not null or curr_term=0)
                     )schedule
                     left join
                     (
                     select
                        due_bill_no,term,
                        concat(due_bill_no,'::',cast(term as string)) as due_bill_no_curr_term,
                        sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'Pricinpal',        repay_amt,0)) as paid_principal,
                        sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'Interest',         repay_amt,0)) as paid_interest,
                        sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'TERMFee',          repay_amt,0)) as paid_term_fee,
                        sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'SVCFee',           repay_amt,0)) as paid_svc_fee,
                        sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'Penalty',          repay_amt,0))  as paid_penalty,
                        sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'LatePaymentCharge',repay_amt,0)) as paid_mult,
                        sum(if(txn_date <=date_sub('${ST9}',1) and term!=0 and bnp_type = 'Pricinpal',1,0)) as paid_term
                       from repay_hst_repair
                      group by due_bill_no,term,d_date,concat(due_bill_no,'::',term)
                     )repay_hst on  schedule.due_bill_no_curr_term =repay_hst.due_bill_no_curr_term
                     group by schedule.due_bill_no_curr_term
        ) as repayhst_yest
        on due_bill_no_curr_term = repayhst_due_bill_no_term
    ) as repay_schedule_tmp
    on concat_ws('::',
                   is_empty(repay_schedule.product_id,                   'a'),
                   is_empty(repay_schedule.due_bill_no,                  'a'),
                   is_empty(repay_schedule.loan_init_term,               'a'),
                   is_empty(repay_schedule.loan_term,                    'a'),
                   is_empty(repay_schedule.start_interest_date,          'a'),
                   is_empty(repay_schedule.should_repay_date,            'a'),
                   is_empty(repay_schedule.should_repay_date_history,    'a'),
                   is_empty(repay_schedule.grace_date,                   'a'),
                   is_empty(repay_schedule.schedule_status,              'a'),
                   is_empty(repay_schedule.paid_out_date,                'a'),
                   is_empty(repay_schedule.paid_out_type,                'a')
         ) =
         concat_ws('::',
                   is_empty(repay_schedule_tmp.product_id,               'a'),
                   is_empty(repay_schedule_tmp.due_bill_no,              'a'),
                   is_empty(repay_schedule_tmp.loan_init_term,           'a'),
                   is_empty(repay_schedule_tmp.loan_term,                'a'),
                   is_empty(repay_schedule_tmp.start_interest_date,      'a'),
                   is_empty(repay_schedule_tmp.should_repay_date,        'a'),
                   is_empty(repay_schedule_tmp.should_repay_date_history,'a'),
                   is_empty(repay_schedule_tmp.grace_date,               'a'),
                   is_empty(repay_schedule_tmp.schedule_status,          'a'),
                   is_empty(repay_schedule_tmp.paid_out_date,            'a'),
                   is_empty(repay_schedule_tmp.paid_out_type,            'a')
         )
          and is_empty(repay_schedule.loan_init_principal,      'a') = is_empty(repay_schedule_tmp.loan_init_principal,      'a')
          and is_empty(repay_schedule.curr_bal,                 'a') = is_empty(repay_schedule_tmp.curr_bal,                 'a')
          and is_empty(repay_schedule.should_repay_principal,   'a') = is_empty(repay_schedule_tmp.should_repay_principal,   'a')
          and is_empty(repay_schedule.should_repay_interest,    'a') = is_empty(repay_schedule_tmp.should_repay_interest,    'a')
          and is_empty(repay_schedule.should_repay_term_fee,    'a') = is_empty(repay_schedule_tmp.should_repay_term_fee,    'a')
          and is_empty(repay_schedule.should_repay_svc_fee,     'a') = is_empty(repay_schedule_tmp.should_repay_svc_fee,     'a')
          and is_empty(repay_schedule.should_repay_penalty,     'a') = is_empty(repay_schedule_tmp.should_repay_penalty,     'a')
          and is_empty(repay_schedule.should_repay_mult_amt,    'a') = is_empty(repay_schedule_tmp.should_repay_mult_amt,    'a')
          and is_empty(repay_schedule.should_repay_penalty_acru,'a') = is_empty(repay_schedule_tmp.should_repay_penalty_acru,'a')
          and is_empty(repay_schedule.paid_principal,           'a') = is_empty(repay_schedule_tmp.paid_principal,           'a')
          and is_empty(repay_schedule.paid_interest,            'a') = is_empty(repay_schedule_tmp.paid_interest,            'a')
          and is_empty(repay_schedule.paid_term_fee,            'a') = is_empty(repay_schedule_tmp.paid_term_fee,            'a')
          and is_empty(repay_schedule.paid_svc_fee,             'a') = is_empty(repay_schedule_tmp.paid_svc_fee,             'a')
          and is_empty(repay_schedule.paid_penalty,             'a') = is_empty(repay_schedule_tmp.paid_penalty,             'a')
          and is_empty(repay_schedule.paid_mult,                'a') = is_empty(repay_schedule_tmp.paid_mult,                'a')
          and is_empty(repay_schedule.reduce_amount,            'a') = is_empty(repay_schedule_tmp.reduce_amount,            'a')
          and is_empty(repay_schedule.reduce_principal,         'a') = is_empty(repay_schedule_tmp.reduce_principal,         'a')
          and is_empty(repay_schedule.reduce_interest,          'a') = is_empty(repay_schedule_tmp.reduce_interest,          'a')
          and is_empty(repay_schedule.reduce_term_fee,          'a') = is_empty(repay_schedule_tmp.reduce_term_fee,          'a')
          and is_empty(repay_schedule.reduce_svc_fee,           'a') = is_empty(repay_schedule_tmp.reduce_svc_fee,           'a')
          and is_empty(repay_schedule.reduce_penalty,           'a') = is_empty(repay_schedule_tmp.reduce_penalty,           'a')
          and is_empty(repay_schedule.reduce_mult_amt,          'a') = is_empty(repay_schedule_tmp.reduce_mult_amt,          'a')
    where repay_schedule_tmp.due_bill_no is null
)
-- insert overwrite table ods_new_s.repay_schedule_intsert partition(is_settled = 'no',product_id)
insert overwrite table ods_new_s.repay_schedule partition(is_settled = 'no',product_id)
select
    repay_schedule.schedule_id,
    repay_schedule.due_bill_no,
    repay_schedule.loan_active_date,
    repay_schedule.loan_init_principal,
    repay_schedule.loan_init_term,
    repay_schedule.loan_term,
    repay_schedule.start_interest_date,
    repay_schedule.curr_bal,
    repay_schedule.should_repay_date,
    repay_schedule.should_repay_date_history,
    repay_schedule.grace_date,
    repay_schedule.should_repay_amount,
    repay_schedule.should_repay_principal,
    repay_schedule.should_repay_interest,
    repay_schedule.should_repay_term_fee,
    repay_schedule.should_repay_svc_fee,
    repay_schedule.should_repay_penalty,
    repay_schedule.should_repay_mult_amt,
    repay_schedule.should_repay_penalty_acru,
    repay_schedule.schedule_status,
    repay_schedule.schedule_status_cn,
    repay_schedule.paid_out_date,
    repay_schedule.paid_out_type,
    repay_schedule.paid_out_type_cn,
    repay_schedule.paid_amount,
    repay_schedule.paid_principal,
    repay_schedule.paid_interest,
    repay_schedule.paid_term_fee,
    repay_schedule.paid_svc_fee,
    repay_schedule.paid_penalty,
    repay_schedule.paid_mult,
    repay_schedule.reduce_amount,
    repay_schedule.reduce_principal,
    repay_schedule.reduce_interest,
    repay_schedule.reduce_term_fee,
    repay_schedule.reduce_svc_fee,
    repay_schedule.reduce_penalty,
    repay_schedule.reduce_mult_amt,
    repay_schedule.s_d_date,
    if(repay_schedule.e_d_date > '${ST9}' and ods_new_s_repay_schedule.schedule_id is not null,ods_new_s_repay_schedule.d_date,if(repay_schedule.e_d_date = '${ST9}' and ods_new_s_repay_schedule.loan_term is null,'3000-12-31',repay_schedule.e_d_date)) as e_d_date,
    repay_schedule.effective_time,
    cast(if(to_date(repay_schedule.expire_time) > '${ST9}' and ods_new_s_repay_schedule.schedule_id is not null,ods_new_s_repay_schedule.update_time,repay_schedule.expire_time) as timestamp) as expire_time,
    repay_schedule.product_id
from (select * from ods_new_s.repay_schedule_tmp where 1 > 0 and product_id in ('001601','001602','001603')) as repay_schedule
left join ods_new_s_repay_schedule
--on repay_schedule.schedule_id = ods_new_s_repay_schedule.schedule_id
on concat(repay_schedule.due_bill_no,repay_schedule.loan_term) = concat(ods_new_s_repay_schedule.due_bill_no,ods_new_s_repay_schedule.loan_term)
union all
select
    repay_schedule.schedule_id,
    repay_schedule.due_bill_no,
    ecas_loan.loan_active_date,
    repay_schedule.loan_init_principal,
    repay_schedule.loan_init_term,
    repay_schedule.loan_term,
    repay_schedule.start_interest_date,
    repay_schedule.curr_bal,
    repay_schedule.should_repay_date,
    repay_schedule.should_repay_date_history,
    repay_schedule.grace_date,
    repay_schedule.should_repay_amount,
    repay_schedule.should_repay_principal,
    repay_schedule.should_repay_interest,
    repay_schedule.should_repay_term_fee,
    repay_schedule.should_repay_svc_fee,
    repay_schedule.should_repay_penalty,
    repay_schedule.should_repay_mult_amt,
    repay_schedule.should_repay_penalty_acru,
    repay_schedule.schedule_status,
    repay_schedule.schedule_status_cn,
    repay_schedule.paid_out_date,
    repay_schedule.paid_out_type,
    repay_schedule.paid_out_type_cn,
    repay_schedule.paid_amount,
    repay_schedule.paid_principal,
    repay_schedule.paid_interest,
    repay_schedule.paid_term_fee,
    repay_schedule.paid_svc_fee,
    repay_schedule.paid_penalty,
    repay_schedule.paid_mult,
    repay_schedule.reduce_amount,
    repay_schedule.reduce_principal,
    repay_schedule.reduce_interest,
    repay_schedule.reduce_term_fee,
    repay_schedule.reduce_svc_fee,
    repay_schedule.reduce_penalty,
    repay_schedule.reduce_mult_amt,
    repay_schedule.d_date as s_d_date,
    repay_schedule.e_d_date,
    repay_schedule.update_time as effective_time,
    repay_schedule.expire_time,
    repay_schedule.product_id
from ods_new_s_repay_schedule as repay_schedule
left join (
    select distinct
        due_bill_no  as ecas_loan_due_bill_no,
        active_date  as loan_active_date,
        product_code as product_id
    from ods.ecas_loan
    where p_type='ddht' and d_date = '${d_date}'
) as ecas_loan
on  repay_schedule.product_id  = ecas_loan.product_id
and repay_schedule.due_bill_no = ecas_loan.ecas_loan_due_bill_no
left join (
    select
        cust_id,
        user_hash_no,
        product_id,
        due_bill_no
    from ods_new_s.loan_apply
    where product_id in ('001601','001602','001603')
) as loan_apply
on  repay_schedule.product_id  = loan_apply.product_id
and repay_schedule.due_bill_no = loan_apply.due_bill_no
-- limit 1
;


-- DROP TABLE IF EXISTS `ods_new_s.repay_schedule`;
-- ALTER TABLE ods_new_s.repay_schedule_intsert RENAME TO ods_new_s.repay_schedule;












-- union all
-- select
--   ecif_id,
--   product_id,
--   due_bill_no,
--   schedule_id,
--   out_side_schedule_no,
--   loan_active_date,
--   loan_init_principal,
--   loan_init_term,
--   loan_term,
--   start_interest_date,
--   should_repay_date,
--   should_repay_date_history,
--   grace_date,
--   should_repay_principal,
--   should_repay_interest,
--   should_repay_penalty,
--   should_repay_term_fee,
--   should_repay_svc_fee,
--   should_repay_mult_amt,
--   reduce_amount,
--   reduce_term_principal,
--   reduce_term_interest,
--   reduce_term_fee,
--   reduce_svc_fee,
--   reduce_penalty,
--   reduce_mult_amt,
--   create_time,
--   update_time
-- from (
--   select distinct
--     ref_nbr,
--     schedule_id                           as schedule_id,
--     null                                  as out_side_schedule_no,
--     loan_init_prin                        as loan_init_principal,
--     loan_init_term                        as loan_init_term,
--     curr_term                             as loan_term,
--     null                                  as start_interest_date,
--     loan_pmt_due_date                     as should_repay_date,
--     null                                  as should_repay_date_history,
--     loan_grace_date                       as grace_date,
--     loan_term_prin                        as should_repay_principal,
--     loan_term_int                         as should_repay_interest,
--     0                                     as should_repay_penalty,
--     loan_term_fee                         as should_repay_term_fee,
--     (loan_svc_fee + loan_replace_svc_fee) as should_repay_svc_fee,
--     0                                     as should_repay_mult_amt,
--     0                                     as reduce_amount,
--     0                                     as reduce_term_principal,
--     0                                     as reduce_term_interest,
--     0                                     as reduce_term_fee,
--     0                                     as reduce_svc_fee,
--     0                                     as reduce_penalty,
--     0                                     as reduce_mult_amt,
--     cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)  as create_time,
--     cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time
--   from ods.ccs_repay_schedule
--   where d_date = '${ST9}'
-- ) as ccs_repay_schedule
-- left join (
--   select distinct
--     concat('00',loan_code) as product_id,
--     due_bill_no,
--     active_date as loan_active_date,
--     ref_nbr
--   from ods.ccs_loan
-- ) as ccs_loan
-- on ccs_repay_schedule.ref_nbr = ccs_loan.ref_nbr
-- left join (
--   select attr_value,ecif_no as ecif_id
--   from ( select inner_id,attr_value from ecif_core.ecif_customer_attribute_hive where attr_key = 'application_no' ) as a
--   join ( select inner_id,ecif_no from ecif_core.ecif_inner_id_hive ) as b on a.inner_id = b.inner_id
-- ) as ecif_customer
-- on ccs_loan.due_bill_no = ecif_customer.attr_value
