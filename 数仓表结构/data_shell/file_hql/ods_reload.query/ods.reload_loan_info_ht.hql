set spark.executor.memory=4g;
set spark.executor.memoryOverhead=4g;
set spark.shuffle.memoryFraction=0.6;        -- shuffle操作的内存占比
set spark.maxRemoteBlockSizeFetchToMem=200m;
set hive.auto.convert.join=true;            -- 关闭自动 MapJoin
set hive.support.quoted.identifiers=None;
set spark.sql.autoBroadcastJoinThreshold=20485760;
--DROP TABLE IF EXISTS `ods_new_s.loan_info_tmp`;
--CREATE TABLE IF NOT EXISTS `ods_new_s.loan_info_tmp` like `ods_new_s.loan_info`;

insert overwrite table ods_new_s${db_suffix}.loan_info_tmp partition(is_settled = 'no',product_id)
select
  `(is_settled)?+.+`
from ods_new_s${db_suffix}.loan_info
where 1 > 0
  and is_settled = 'no'
  and s_d_date <'${ST9}'
  and product_id in ('001601','001602','001603')
  -- and to_date(effective_time) <= date_add('${ST9}',1)
distribute by product_id,loan_active_date
;

-- 借据表修数
--1 根据还款计划 重新计算借据逾期状态 逾期天数 逾期金额
--2 根据还款计划计算已还金额






-- DROP TABLE IF EXISTS ods_new_s${db_suffix}.loan_info_intsert;
-- CREATE TABLE IF NOT EXISTS `ods_new_s${db_suffix}.loan_info_intsert` like `ods_new_s${db_suffix}.loan_info`;


with repay_hst_repair as (
select
      repayhst.due_bill_no,repayhst.term,bnp_type,repayhst.d_date,repay_amt,
      if(repair_hst.order_id is not null,repair_hst.paid_out_date,repayhst.txn_date) as txn_date
         from (
                select * from   ods.ecas_repay_hst  where 1 > 0  and d_date ='${d_date}'    and p_type in ('ddht')   and txn_date <= date_add('${ST9}',30)
                --11月修数  删除掉汇通的两笔线下还款的罚息实还数据
                and payment_id not in ('000016043097811admin000068000001','000016043095431admin000068000001')
          )repayhst
           left join (
          select distinct order_id,paid_out_date  from  ods.schedule_repay_order_info_ddht  where biz_date='${d_date}'  and product_id in ('001601','001602','001603')
          )repair_hst on repayhst.order_id=repair_hst.order_id

),
ods_new_s_loan as (
  select
    today.product_id                                                                 as product_id,
    today.due_bill_no                                                                as due_bill_no,
    today.apply_no                                                                   as apply_no,
    today.loan_active_date                                                           as loan_active_date,
    today.loan_init_principal                                                        as loan_init_principal,
    today.loan_init_term                                                             as loan_init_term,
    today.loan_term                                                                  as loan_term,
    today.should_repay_date                                                          as should_repay_date,
    today.loan_term_repaid                                                           as loan_term_repaid,
    today.loan_term_remain                                                           as loan_term_remain,
    today.loan_status                                                                as loan_status,
    today.loan_status_cn                                                             as loan_status_cn,
    today.loan_out_reason                                                            as loan_out_reason,
    today.paid_out_type                                                              as paid_out_type,
    today.paid_out_type_cn                                                           as paid_out_type_cn,
    today.paid_out_date                                                              as paid_out_date,
    today.terminal_date                                                              as terminal_date,
    today.loan_init_interest                                                         as loan_init_interest,
    today.loan_init_term_fee                                                         as loan_init_term_fee,
    today.loan_init_svc_fee                                                          as loan_init_svc_fee,
    today.paid_amount                                                                as paid_amount,
    today.paid_principal                                                             as paid_principal,
    today.paid_interest                                                              as paid_interest,
    today.paid_penalty                                                               as paid_penalty,
    today.paid_svc_fee                                                               as paid_svc_fee,
    today.paid_term_fee                                                              as paid_term_fee,
    today.paid_mult                                                                  as paid_mult,
    today.remain_amount                                                              as remain_amount,
    today.remain_principal                                                           as remain_principal,
    today.remain_interest                                                            as remain_interest,
    today.remain_svc_fee                                                             as remain_svc_fee,
    today.remain_term_fee                                                            as remain_term_fee,
    today.overdue_principal                                                          as overdue_principal,
    today.overdue_interest                                                           as overdue_interest,
    today.overdue_svc_fee                                                            as overdue_svc_fee,
    today.overdue_term_fee                                                           as overdue_term_fee,
    today.overdue_penalty                                                            as overdue_penalty,
    today.overdue_mult_amt                                                           as overdue_mult_amt,
    today.overdue_date                                                               as overdue_date_start,
    today.overdue_days                                                               as overdue_days,
    cast(date_add(today.overdue_date,cast(today.overdue_days as int) - 1) as string) as overdue_date,
    today.collect_out_date                                                           as collect_out_date,
    today.overdue_term                                                               as overdue_term,
    today.overdue_terms_max                                                          as overdue_terms_max,
    today.d_date                                                                     as d_date,
    '3000-12-31'                                                                     as e_d_date,
    today.create_time                                                                as create_time,
    today.update_time                                                                as update_time,
    cast('3000-12-31 00:00:00' as timestamp)                                         as expire_time
  from (
    select
      ecas_loan.product_id,
      ecas_loan.due_bill_no,
      ecas_loan.apply_no,
      ecas_loan.loan_active_date,
      ecas_loan.loan_init_term,
      case
      when ecas_loan.paid_out_date = ecas_loan.loan_active_date then 1
      when ecas_loan.paid_out_date is null                      then repay_schedule.loan_term2
      when '${ST9}' <= ecas_loan.paid_out_date                  then repay_schedule.loan_term2
      else null end as loan_term,
      if(
        (ecas_loan.paid_out_date = ecas_loan.loan_active_date and ecas_loan.loan_term = 1) or ecas_loan.paid_out_date is null or '${ST9}' <= ecas_loan.paid_out_date,
        repay_schedule.should_repay_date,
        null
      ) as should_repay_date,
      case when ecas_loan.due_bill_no ="1000000275" and  ecas_loan.d_date<'2020-02-12' then 0
      when ecas_loan.due_bill_no ="1000000381" and  ecas_loan.d_date>='2020-08-17' then ecas_loan.loan_init_term
      when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date >='2020-02-12'  then ecas_loan.loan_init_term
      else nvl(repay_detail.paid_term,0) end  as loan_term_repaid,
      case when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date<'2020-02-12' then ecas_loan.loan_init_term
      when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date >='2020-02-12'  then 0
       when ecas_loan.due_bill_no ="1000000381" and ecas_loan.d_date >='2020-08-17'  then ecas_loan.loan_init_term
      else ecas_loan.loan_init_term - nvl(repay_detail.paid_term,0) end  as loan_term_remain,
      case   when ecas_loan.due_bill_no="1000004836" and ecas_loan.d_date >='2020-11-23'  then 'F'
             when overdue_day.overdue_date!='9999-12-31' then 'O'
             when ecas_loan.due_bill_no="1000000163" and ecas_loan.d_date >='2020-09-29'  then 'F'
             when ecas_loan.due_bill_no="1000000403" and ecas_loan.d_date >='2020-11-09'  then 'F'
             when ecas_loan.loan_status='O' and overdue_day.overdue_date='9999-12-31' then "N"
      else  ecas_loan.loan_status  end as loan_status,
       case
            when ecas_loan.due_bill_no="1000004836" and ecas_loan.d_date >='2020-11-23'  then 'F'
            when ecas_loan.due_bill_no="1000000163" and ecas_loan.d_date >='2020-09-29'  then '已还清'
            when ecas_loan.due_bill_no="1000000403" and ecas_loan.d_date >='2020-11-09'  then '已还清'
            when overdue_day.overdue_date!='9999-12-31' then '逾期'
            when ecas_loan.loan_status='O' and overdue_day.overdue_date='9999-12-31' then "正常"
      else  ecas_loan.loan_status_cn  end as loan_status_cn,
      ecas_loan.loan_out_reason,
      ecas_loan.paid_out_type,
      ecas_loan.paid_out_type_cn,
      ecas_loan.paid_out_date,
      ecas_loan.terminal_date,
      ecas_loan.loan_init_principal,
      ecas_loan.loan_init_interest_rate,
      nvl(overdue_principal_reload.total_due_int,ecas_loan.loan_init_interest) as loan_init_interest,
      ecas_loan.loan_init_term_fee_rate,
      nvl(overdue_principal_reload.total_due_term_fee,0) as  loan_init_term_fee,
      ecas_loan.loan_init_svc_fee_rate,
      nvl(overdue_principal_reload.total_due_svc_fee,0) as loan_init_svc_fee,
      ecas_loan.loan_init_penalty_rate,
      nvl(repay_detail.paid_principal,0) +
      nvl(repay_detail.paid_interest, 0) +
      nvl(repay_detail.paid_penalty,  0) +
      nvl(repay_detail.paid_svc_fee,  0) +
      nvl(repay_detail.paid_term_fee, 0) +
      nvl(repay_detail.paid_mult,     0) as paid_amount,
      nvl(overdue_principal_reload.should_repay_fee_amount,0) as should_repay_fee_amount,
      case when ecas_loan.due_bill_no ="1000000275"  and ecas_loan.d_date<'2020-02-12' then 0
      when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date >='2020-02-12'  then ecas_loan.loan_init_principal
      else nvl(repay_detail.paid_principal,0)  end  as paid_principal,
      nvl(repay_detail.paid_interest,0) as paid_interest,
      nvl(repay_detail.paid_penalty,0) as paid_penalty,
      nvl(repay_detail.paid_svc_fee,0) as paid_svc_fee,
      nvl(repay_detail.paid_term_fee,0) as paid_term_fee,
      nvl(repay_detail.paid_mult,0) as paid_mult,
      --coalesce(repay_detail.paid_interest, ecas_loan.paid_interest, 0) as paid_interest,
      --coalesce(repay_detail.paid_penalty,  ecas_loan.paid_penalty,  0) as paid_penalty,
     -- coalesce(repay_detail.paid_svc_fee,  ecas_loan.paid_svc_fee,  0) as paid_svc_fee,
     -- coalesce(repay_detail.paid_term_fee, ecas_loan.paid_term_fee, 0) as paid_term_fee,
     -- coalesce(repay_detail.paid_mult,     ecas_loan.paid_mult,     0) as paid_mult,
      case when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date  >='2020-02-12'  then  nvl(overdue_principal_reload.total_due_int,ecas_loan.loan_init_interest)
       when  ecas_loan.due_bill_no ="1000000061" and ecas_loan.d_date  >='2020-02-28'  then 0
      else
      (
        nvl(ecas_loan.loan_init_principal,0) +
        nvl(overdue_principal_reload.total_due_int,ecas_loan.loan_init_interest) +
        nvl(overdue_principal_reload.should_repay_fee_amount,0)
        --nvl(ecas_loan.loan_init_term_fee, 0) +
        --nvl(ecas_loan.loan_init_svc_fee,  0)
      ) - (
            nvl(repay_detail.paid_principal,0)+
            nvl(repay_detail.paid_interest,0)+
            nvl(repay_detail.paid_svc_fee,0)+
            nvl(repay_detail.paid_term_fee,0)+
            nvl(repay_detail.paid_penalty,0)
      ) - cast((case when ecas_loan.due_bill_no="1000000381" and ecas_loan.d_date>='2020-08-17' then 34444.81
                when nvl(overdue_principal_reload.total_reduce_fee_amount,0)>nvl(overdue_principal_reload.should_repay_fee_amount,0) then nvl(overdue_principal_reload.should_repay_fee_amount,0)+nvl(overdue_principal_reload.total_reduce_prin,0)+nvl(overdue_principal_reload.total_reduce_int,0)
                else nvl(overdue_principal_reload.total_reduce_fee_amount,0)+nvl(overdue_principal_reload.total_reduce_prin,0)+nvl(overdue_principal_reload.total_reduce_int,0) end) as decimal(15,2)) end as remain_amount,
       case when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date  >='2020-02-12'  then 0
       else
      cast(ecas_loan.loan_init_principal - nvl(repay_detail.paid_principal,0)-(
          case when ecas_loan.due_bill_no="1000000381" and ecas_loan.d_date>='2020-08-17' then 21740.00
          else nvl(overdue_principal_reload.total_reduce_prin,0)  end
      )     as decimal(15,2))                       end                                                                        as remain_principal,--减去减免本金
       case when ecas_loan.due_bill_no ="1000000275" and  ecas_loan.d_date<'2020-02-12' then nvl(overdue_principal_reload.total_due_int,ecas_loan.loan_init_interest)
       else
        cast( nvl(overdue_principal_reload.total_due_int,ecas_loan.loan_init_interest)   - nvl(repay_detail.paid_interest,0) - (
        case when ecas_loan.due_bill_no="1000000381" and ecas_loan.d_date>='2020-08-17' then 12704.81
        else nvl(overdue_principal_reload.total_reduce_int,0) end
        )   as decimal(15,2))         end                                                                                        as remain_interest ,
     case  when  ecas_loan.due_bill_no ="1000000061" and ecas_loan.d_date  >='2020-02-28'  then 0
      else ecas_loan.loan_init_svc_fee   - nvl(repay_detail.paid_svc_fee, 0) -if(nvl(overdue_principal_reload.total_reduce_svc_fee,0)>nvl(overdue_principal_reload.total_due_svc_fee,0),nvl(overdue_principal_reload.total_due_svc_fee,0),nvl(overdue_principal_reload.total_reduce_svc_fee,0)) end  as remain_svc_fee,
      ecas_loan.loan_init_term_fee  - nvl(repay_detail.paid_term_fee, 0)-if(nvl(overdue_principal_reload.total_reduce_term_fee,0)>nvl(overdue_principal_reload.total_due_term_fee,0),nvl(overdue_principal_reload.total_due_term_fee,0),nvl(overdue_principal_reload.total_reduce_term_fee,0)) as remain_term_fee,
      overdue_principal_reload.overdue_principal,
      overdue_principal_reload.overdue_interest,
      ecas_loan.overdue_svc_fee,
      ecas_loan.overdue_term_fee,
      ecas_loan.overdue_penalty,
      ecas_loan.overdue_mult_amt,
      case when overdue_day.overdue_date!='9999-12-31' then overdue_day.overdue_date
      else   null  end as overdue_date,
      case when overdue_day.overdue_date!='9999-12-31' then abs(datediff(overdue_day.overdue_date,'${ST9}'))+1
      else   0  end as overdue_days,
      ecas_loan.collect_out_date,
      overdue_term.overdue_term,
      nvl(overdue_term.overdue_terms_max,0) as overdue_terms_max,
      ecas_loan.sync_date,
      ecas_loan.create_time,
      ecas_loan.update_time,
      ecas_loan.d_date
    from (
      select
        product_code                      as product_id,
        due_bill_no                       as due_bill_no,
        apply_no                          as apply_no,
        active_date                       as loan_active_date,
        loan_init_term                    as loan_init_term,
        curr_term                         as loan_term,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
        else repay_term end               as loan_term_repaid,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
        else remain_term end              as loan_term_remain,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
         when due_bill_no="1000000275" and d_date >='2020-02-06' and  d_date<'2020-02-12' then 'O'
         when due_bill_no="1000000275" and d_date >='2020-01-21' and  d_date<'2020-02-06' then 'N'
        when due_bill_no="1000000275" and d_date >='2020-02-12' then 'F'
        when due_bill_no="1000000381" and d_date >='2020-08-17' then 'F'
        else loan_status end              as loan_status,
        case
          case
           when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
           when due_bill_no="1000000275" and d_date >='2020-02-06' and  d_date<'2020-02-12' then 'O'
           when due_bill_no="1000000275" and d_date >='2020-01-21' and  d_date<'2020-02-06' then 'N'
           when due_bill_no="1000000275" and d_date  >='2020-02-12' then 'F'
           when due_bill_no="1000000381" and d_date >='2020-08-17' then 'F'
          else loan_status end
        when 'N' then '正常'
        when 'O' then '逾期'
        when 'F' then '已还清'
        else
          case
          when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
          when due_bill_no="1000000275" and d_date >='2020-02-06' and  d_date<'2020-02-12' then 'O'
         when due_bill_no="1000000275" and   d_date<'2020-02-06' then 'N'
           when due_bill_no="1000000275" and d_date  >='2020-02-12' then 'F'
          else loan_status end
        end                               as loan_status_cn,
        terminal_reason_cd                as loan_out_reason,
        case when due_bill_no="1000000275" and  d_date<'2020-02-12' then null
        else loan_settle_reason end  as paid_out_type,
        case when due_bill_no="1000000275" and  d_date<'2020-02-12' then null
        when loan_settle_reason='NORMAL_SETTLE'  then '正常结清'
        when loan_settle_reason='OVERDUE_SETTLE' then '逾期结清'
        when loan_settle_reason='PRE_SETTLE'     then '提前结清'
        when loan_settle_reason='REFUND'         then '退车'
        when loan_settle_reason='REDEMPTION'     then '赎回'
        when loan_settle_reason='BANK_REF'       then '退票结清'
        when loan_settle_reason='BUY_BACK'       then '资产回购'
        when loan_settle_reason='CAPITAL_VERI'   then '资产核销'
        when loan_settle_reason='DISPOSAL'       then '处置结束'
        when loan_settle_reason='OVER_COMP'      then '逾期代偿'
         else loan_settle_reason end             as paid_out_type_cn,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-03'
        when due_bill_no="1000000275" and     d_date<'2020-02-12' then null
        when due_bill_no="1000000275" and d_date >='2020-02-12' then '2020-02-12'
        when due_bill_no="1000000381" and d_date >='2020-08-17' then '2020-08-17'
        when due_bill_no="1000000163" and d_date >='2020-09-29'  then '2020-09-29'
        when due_bill_no="1000000403" and d_date >='2020-11-09'  then '2020-11-09'
         when due_bill_no="1000004836" and d_date >='2020-11-23' then '2020-11-23'
        else paid_out_date end            as paid_out_date,
        terminal_date                     as terminal_date,
        loan_init_prin                    as loan_init_principal,
        interest_rate                     as loan_init_interest_rate,
        totle_int                         as loan_init_interest,
        term_fee_rate                     as loan_init_term_fee_rate,
        totle_term_fee                    as loan_init_term_fee,
        svc_fee_rate                      as loan_init_svc_fee_rate,
        totle_svc_fee                    as loan_init_svc_fee,
        penalty_rate                      as loan_init_penalty_rate,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then cast('1000.00' as decimal(15,2))
        when due_bill_no="1000000275" and   d_date<'2020-02-12' then  0
        when due_bill_no="1000000275" and  d_date>='2020-02-12' then loan_init_prin
        else paid_principal end           as paid_principal,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then cast('1.00' as decimal(15,2))
        else paid_interest end            as paid_interest,
        paid_term_fee                     as paid_term_fee,
        paid_svc_fee                      as paid_svc_fee,
        paid_penalty                      as paid_penalty,
        paid_mult                         as paid_mult,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_prin end             as overdue_principal,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_interest end         as overdue_interest,
        overdue_svc_fee                   as overdue_svc_fee,
        overdue_term_fee                  as overdue_term_fee,
        overdue_penalty                   as overdue_penalty,
        overdue_mult_amt                  as overdue_mult_amt,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
        when due_bill_no='1000000275' and d_date >='2020-02-06' and d_date<'2020-02-12' then '2020-02-06'
        else overdue_date end             as overdue_date,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when due_bill_no='1000000275' and d_date >='2020-02-06' and d_date<'2020-02-12'  then  datediff('${ST9}','2020-02-06')+1
        else overdue_days end             as overdue_days,
        collect_out_date                  as collect_out_date,
        case
        when d_date = '2020-02-21' and sync_date = 'ZhongHang' then capital_plan_no
        when d_date = '2020-02-27' and capital_type = '2020-02-28' then capital_type
        when d_date = '2020-02-28' and capital_type = '2020-02-29' then capital_type
        when d_date = '2020-02-29' and capital_type = '2020-03-01' then capital_type
        else sync_date end                as sync_date,
        cast(datefmt(create_time, 'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as create_time,
        cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp) as update_time,
        d_date                            as d_date
        from
      (
       select * from ods.ecas_loan where 1 > 0 and d_date = '${ST9}' and p_type in ('ddht') and product_code in ('001601','001602','001603')
        union all
      select * from ods.ecas_loan_ht_repair  where 1 > 0 and d_date = '${ST9}' and p_type in ('ddht') and product_code in ('001601','001602','001603')
      )tmp
    ) as ecas_loan
    left join (
        select
        schedule.due_bill_no,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_principal end ) as paid_principal,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_interest end ) as paid_interest,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_term_fee end ) as paid_term_fee,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_svc_fee end ) as paid_svc_fee,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_penalty end ) as paid_penalty,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_mult end ) as paid_mult,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_term end ) as paid_term
        from
         (select
                    due_bill_no,
                     curr_term,
                     paid_out_date,
                     due_bill_no_curr_term,
                     d_date
                 from
                     (
                         select
                         tmp.due_bill_no,
                         tmp.curr_term,
                         case
                            when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                            when tmp.due_bill_no='1000000381' and tmp.d_date >='2020-08-17' then '2020-08-17'
                            when tmp.due_bill_no="1000001858" and tmp.d_date ='2020-09-21' and tmp.curr_term=0 then null
                              when tmp.due_bill_no='1000000275' and tmp.d_date >='2020-01-21' and tmp.d_date<'2020-02-12' then null
                              when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                             when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                              when tmp.paid_out_date >tmp.d_date then null
                              when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                              when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                              else tmp.paid_out_date end as paid_out_date,
                         concat(tmp.due_bill_no,'::',cast(tmp.curr_term as string)) as due_bill_no_curr_term,
                         tmp.d_date
                         from
                         (
                         select  * from  ods.ecas_repay_schedule where p_type="ddht" and d_date='${ST9}' and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                         union all
                         select  * from  ods.ecas_repay_schedule_ht_repair where p_type="ddht" and d_date='${ST9}' and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                         )tmp
                         left join
                          (
                            select due_bill_no,curr_term,paid_out_date,schedule_status
                                    from ods.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and product_code in ('001601','001602','001603')
                                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10) and schedule_id not in ('000016006898691admin000068000001')
                          )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
                    )tmp1
                    where tmp1.paid_out_date is not null or tmp1.curr_term=0
         )schedule
         left join
         (
         select
            due_bill_no,term,
            concat(due_bill_no,'::',cast(term as string)) as due_bill_no_curr_term,
            sum(if(txn_date <='${ST9}' and bnp_type = 'Pricinpal',        repay_amt,0)) - case
            when due_bill_no = '1120070912093993172613' and d_date between '2020-07-10' and '2020-07-13' then 2500
            when due_bill_no = '1120061910384241252747' and d_date between '2020-06-23' and '2020-02-20'     then 200
            else 0 end                                          as paid_principal,
            sum(if(txn_date <='${ST9}' and bnp_type = 'Interest',         repay_amt,0)) as paid_interest,
            sum(if(txn_date <='${ST9}' and bnp_type = 'TERMFee',          repay_amt,0)) as paid_term_fee,
            sum(if(txn_date <='${ST9}' and bnp_type = 'SVCFee',           repay_amt,0)) as paid_svc_fee,
            sum(if(txn_date <='${ST9}' and bnp_type = 'Penalty',          repay_amt,0)) as paid_penalty,
            sum(if(txn_date <='${ST9}' and bnp_type = 'LatePaymentCharge',repay_amt,0)) as paid_mult,
            sum(if(txn_date <='${ST9}' and term!=0 and bnp_type = 'Pricinpal',1,0)) as paid_term
          from repay_hst_repair
           --and txn_date <= '2020-02-20'
          group by due_bill_no,term,d_date,concat(due_bill_no,'::',term)
         )repay_hst on  schedule.due_bill_no_curr_term =repay_hst.due_bill_no_curr_term
        group by schedule.due_bill_no
    ) as repay_detail
    on ecas_loan.due_bill_no = repay_detail.due_bill_no
    left join (
      select
        due_bill_no,
        min(curr_term)    as loan_term2,
        min(pmt_due_date) as should_repay_date,
        product_code      as product_id
      from
      (
      select * from ods.ecas_repay_schedule where d_date='${ST9}' and  p_type in ('ddht') and curr_term > 0  and product_code in('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
    union all
     select * from ods.ecas_repay_schedule_ht_repair where d_date='${ST9}' and  p_type in ('ddht') and curr_term > 0  and product_code in('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
      )tmp
      where 1 > 0
        and d_date = '${ST9}'      -- 取快照日当天的所有还款计划数据
        and p_type in ('ddht')
        and d_date <= nvl(origin_pmt_due_date,pmt_due_date) -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0  and product_code in('001601','001602','001603')        -- 过滤掉汇通的第 0 期的情况
      group by due_bill_no,product_code
    ) as repay_schedule
    on ecas_loan.due_bill_no = repay_schedule.due_bill_no
    left join (
      select
        due_bill_no,
        max(curr_term)   as overdue_term,
        count(curr_term) as overdue_terms_max,
        d_date
      from
      (
      select * from ods.ecas_repay_schedule  where d_date='${ST9}' and product_code in('001601','001602','001603') and pmt_due_date <= d_date and curr_term>0  and schedule_id not in ('000016006898691admin000068000001')
        union all
      select * from ods.ecas_repay_schedule_ht_repair  where d_date='${ST9}' and product_code in('001601','001602','001603') and pmt_due_date <= d_date and curr_term>0 and schedule_id not in ('000016006898691admin000068000001')
      )tmp
      where 1 > 0
        and d_date='${ST9}' and product_code in('001601','001602','001603')
        and pmt_due_date <= d_date and curr_term>0
      group by due_bill_no,d_date
    ) as overdue_term
    on  ecas_loan.d_date      = overdue_term.d_date
    and ecas_loan.due_bill_no = overdue_term.due_bill_no
    left join
    (
        select
            repay_schedule.due_bill_no                                                  as due_bill_no,
            sum(case
                when repay_schedule.paid_out_date <=nvl(origin_pmt_due_date,pmt_due_date)  then 0
                when '${ST9}' < pmt_due_date and curr_term!=0 then 0
                when '${ST9}' >= pmt_due_date and paid_out_date is  null and curr_term!=0 then due_term_prin
                end )                                                                   as overdue_principal,
            sum(case
            when repay_schedule.paid_out_date <=nvl(origin_pmt_due_date,pmt_due_date)  then 0
                when '${ST9}' < pmt_due_date and curr_term!=0 then 0
                when '${ST9}' >= pmt_due_date and paid_out_date is  null and curr_term!=0 then due_term_int
                end )                                                                  as overdue_interest,
                ---应还字段
                sum(nvl(due_term_int,0)) as total_due_int,
                sum(nvl(due_term_fee,0)) as total_due_term_fee,
                sum(nvl(due_svc_fee,0)) as total_due_svc_fee,
                sum(nvl(due_penalty,0)) as total_due_penalty,
                sum(nvl(due_mult_amt,0)) as total_due_mult_amt,
                sum(nvl(due_term_fee,0))+sum(nvl(due_svc_fee,0))+sum(nvl(due_penalty,0))+sum(nvl(due_mult_amt,0)) as should_repay_fee_amount,
                sum(if(nvl(reduce_term_prin,0)>nvl(due_term_prin,0),nvl(due_term_prin,0),nvl(reduce_term_prin,0) )) as total_reduce_prin,
                sum(if(nvl(reduce_term_int,0)>nvl(due_term_int,0),nvl(due_term_int,0),nvl(reduce_term_int,0))) as total_reduce_int,
                --减免部分 防止减免金额大于应还金额加上判断 1000000222 借据减免费用大于应还费用
                sum(if(nvl(reduce_term_fee,0)>nvl(due_term_fee,0),nvl(due_term_fee,0),nvl(reduce_term_fee,0))) as total_reduce_term_fee,
                sum(case when due_bill_no="1000000222" then 0
                         when nvl(reduce_penalty,0)>nvl(due_penalty,0) then nvl(due_penalty,0)
                         else nvl(reduce_penalty,0) end) as total_reduce_penalty,
                sum(if(nvl(reduce_svc_fee,0)>nvl(due_svc_fee,0),nvl(due_svc_fee,0),nvl(reduce_svc_fee,0))) as total_reduce_svc_fee,
                sum(if(nvl(reduce_mult_amt,0)>nvl(due_mult_amt,0),nvl(due_mult_amt,0),nvl(reduce_mult_amt,0))) as total_reduce_mult_amt,
                sum(if(nvl(reduce_term_fee,0)>nvl(due_term_fee,0),nvl(due_term_fee,0),nvl(reduce_term_fee,0)))+
                sum(case when due_bill_no="1000000222" then 0
                         when nvl(reduce_penalty,0)>nvl(due_penalty,0) then nvl(due_penalty,0)
                         else nvl(reduce_penalty,0) end) +
                         +sum(if(nvl(reduce_svc_fee,0)>nvl(due_svc_fee,0),nvl(due_svc_fee,0),nvl(reduce_svc_fee,0)))
                         +sum(if(nvl(reduce_mult_amt,0)>nvl(due_mult_amt,0),nvl(due_mult_amt,0),nvl(reduce_mult_amt,0))) as total_reduce_fee_amount
        from
        (
            select
                tmp.due_bill_no,
                tmp.origin_pmt_due_date,
                tmp.pmt_due_date,
                tmp.due_term_prin,
                tmp.due_term_int,
                tmp.curr_term,
                tmp.due_term_fee,
               case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 600
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0 then 600
                         else tmp.due_svc_fee end                                                       as due_svc_fee,
               case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then cast(1559.51 as decimal(15,4))
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0  then cast(1245 as decimal(15,4))
                         else tmp.due_penalty end                                                        as due_penalty,
                tmp.due_mult_amt,
                tmp.reduce_term_prin,
                tmp.reduce_term_int,
                tmp.reduce_term_fee,
                case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_svc_fee
                when tmp.due_bill_no="1000000061" then 0
                when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 600
                when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0 then 600
                else tmp.reduce_svc_fee                                                                                                      end as reduce_svc_fee,

                case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then cast(728.51 as decimal(15,4))
                     when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0  then cast(48.56 as decimal(15,4)) else tmp.reduce_penalty end  as reduce_penalty,
                tmp.reduce_mult_amt,
                case
                     when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                     when tmp.due_bill_no='1000000381' and tmp.d_date >='2020-08-17' then '2020-08-17'
                     when tmp.due_bill_no="1000001858" and tmp.d_date ='2020-09-21' and tmp.curr_term=0 then null
                     when tmp.paid_out_date >tmp.d_date then null
                      when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                      when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                     when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                     when tmp.due_bill_no='1000000275' and  tmp.d_date<'2020-02-12' then null
                     when tmp.due_bill_no='1000000275' and  tmp.d_date>='2020-02-12' then '2020-02-12'
                     when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                else tmp.paid_out_date end  as paid_out_date
            from (
            select * from ods.ecas_repay_schedule where d_date = '${ST9}' and p_type in ('ddht') and product_code in('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
            union all
            select * from ods.ecas_repay_schedule_ht_repair where d_date = '${ST9}' and p_type in ('ddht') and product_code in('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
            )tmp
            left join
            (
                    select due_bill_no,curr_term,paid_out_date,schedule_status,reduce_svc_fee
                    from ods.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and product_code in ('001601','001602','001603')
                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10) and schedule_id not in ('000016006898691admin000068000001')
            )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term

        ) repay_schedule
        group by repay_schedule.due_bill_no
    ) as overdue_principal_reload
    on ecas_loan.due_bill_no = overdue_principal_reload.due_bill_no
    left join (
        select
        due_bill_no ,
        min(overdue_date) as overdue_date
        from
        (
        select
        loan.due_bill_no,
        case when schedule.paid_out_date is null and '${ST9}'>=pmt_due_date then pmt_due_date
        else '9999-12-31' end as overdue_date
        from
            (
                select
                due_bill_no,
                curr_term
                from
                (
                    select * from ods.ecas_loan where p_type='ddht' and product_code in ('001601','001602','001603') and d_date='${ST9}'
                     union all
                    select * from ods.ecas_loan_ht_repair where p_type='ddht' and product_code in ('001601','001602','001603') and d_date='${ST9}'
                )tmp
            )loan left join
            (
                select tmp.due_bill_no,tmp.curr_term,
                case when tmp.due_bill_no='1000000381' and tmp.d_date >='2020-08-17' then '2020-08-17'
                     when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                     when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                     when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                     when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                     when tmp.paid_out_date >tmp.d_date then null
                     when tmp.due_bill_no='1000000275' and tmp.d_date<'2020-02-12' then null
                     when tmp.due_bill_no="1000001858" and tmp.d_date ='2020-09-21' and tmp.curr_term=0 then null
                     when tmp.due_bill_no='1000000275' and  tmp.d_date>='2020-02-12' then '2020-02-12'

                     when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                else tmp.paid_out_date end as paid_out_date,
                tmp.schedule_status,tmp.pmt_due_date
                from
                (
                    select * from ods.ecas_repay_schedule where d_date='${ST9}' and  p_type='ddht'  and product_code in ('001601','001602','001603') and curr_term>0 and schedule_id not in ('000016006898691admin000068000001')
                    union all
                    select * from ods.ecas_repay_schedule_ht_repair where d_date='${ST9}' and  p_type='ddht' and product_code in ('001601','001602','001603') and curr_term>0 and schedule_id not in ('000016006898691admin000068000001')
                )tmp
                left join
                (
                    select due_bill_no,curr_term,paid_out_date,schedule_status
                    from ods.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and product_code in ('001601','001602','001603')
                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10) and schedule_id not in ('000016006898691admin000068000001')
                )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
             )schedule on loan.due_bill_no=schedule.due_bill_no
             where  schedule.curr_term<=loan.curr_term
        )tmp
        group by due_bill_no
    )overdue_day  on ecas_loan.due_bill_no = overdue_day.due_bill_no
  ) as today
  left join (
    select
      ecas_loan.product_id,
      ecas_loan.due_bill_no,
      ecas_loan.apply_no,
      ecas_loan.loan_active_date,
      ecas_loan.loan_init_term,
      case
      when ecas_loan.paid_out_date = ecas_loan.loan_active_date then 1
      when ecas_loan.paid_out_date is null                      then repay_schedule.loan_term2
      when '${ST9}' <= ecas_loan.paid_out_date                  then repay_schedule.loan_term2
      else null end as loan_term,
      if(
        (ecas_loan.paid_out_date = ecas_loan.loan_active_date and ecas_loan.loan_term = 1) or ecas_loan.paid_out_date is null or '${ST9}' <= ecas_loan.paid_out_date,
        repay_schedule.should_repay_date,
        null
      ) as should_repay_date,
      case when ecas_loan.due_bill_no ="1000000275"  and ecas_loan.d_date<'2020-02-12' then 0
      when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date >='2020-02-12'  then ecas_loan.loan_init_term
      when ecas_loan.due_bill_no ="1000000381" and ecas_loan.d_date >='2020-08-17'  then ecas_loan.loan_init_term
      else nvl(repay_detail.paid_term,0) end  as loan_term_repaid,
      case when ecas_loan.due_bill_no ="1000000275"  and ecas_loan.d_date<'2020-02-12' then ecas_loan.loan_init_term
      when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date >='2020-02-12'  then 0
      when ecas_loan.due_bill_no ="1000000381" and ecas_loan.d_date >='2020-08-17'  then 0
      else ecas_loan.loan_init_term - nvl(repay_detail.paid_term,0) end  as loan_term_remain,
      --ecas_loan.loan_term_repaid,
      --ecas_loan.loan_term_remain,
    case when ecas_loan.due_bill_no="1000000163" and ecas_loan.d_date >='2020-09-29'  then 'F'
         when ecas_loan.due_bill_no="1000000403" and ecas_loan.d_date >='2020-11-09'  then 'F'
         when ecas_loan.due_bill_no="1000004836" and ecas_loan.d_date >='2020-11-23'  then 'F'
         when overdue_day.overdue_date!='9999-12-31' then 'O'
         when ecas_loan.loan_status='O' and overdue_day.overdue_date='9999-12-31' then "N"
      else  ecas_loan.loan_status  end as loan_status,
      ecas_loan.loan_out_reason,
      ecas_loan.paid_out_type,
      ecas_loan.paid_out_date,
      ecas_loan.terminal_date,
      ecas_loan.loan_init_principal,
      ecas_loan.loan_init_interest_rate,
      nvl(overdue_principal_reload.total_due_int,ecas_loan.loan_init_interest) as  loan_init_interest,
      ecas_loan.loan_init_term_fee_rate,
      nvl(overdue_principal_reload.total_due_term_fee,0) as  loan_init_term_fee,
      ecas_loan.loan_init_svc_fee_rate,
      nvl(overdue_principal_reload.total_due_svc_fee,0) as loan_init_svc_fee,
      ecas_loan.loan_init_penalty_rate,
      case when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date between '2020-01-05' and  date_sub('2020-02-12',1)   then 0
      when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date >= '2020-02-12'  then ecas_loan.loan_init_principal
      else nvl(repay_detail.paid_principal,0)  end  as paid_principal,
            case when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date  >='2020-02-12'  then nvl(overdue_principal_reload.total_due_int,ecas_loan.loan_init_interest)
       when  ecas_loan.due_bill_no ="1000000061" and ecas_loan.d_date  >='2020-02-28'  then 0
      else
      (
        nvl(ecas_loan.loan_init_principal,0) +
        nvl(overdue_principal_reload.total_due_int,ecas_loan.loan_init_interest) +
        nvl(overdue_principal_reload.should_repay_fee_amount,0)
        --nvl(ecas_loan.loan_init_term_fee, 0) +
        --nvl(ecas_loan.loan_init_svc_fee,  0)
      ) - (
            nvl(repay_detail.paid_principal,0)+
            nvl(repay_detail.paid_interest,0)+
            nvl(repay_detail.paid_svc_fee,0)+
            nvl(repay_detail.paid_term_fee,0)+
            nvl(repay_detail.paid_penalty,0)
      ) - cast((case when ecas_loan.due_bill_no="1000000381" and ecas_loan.d_date>='2020-08-17' then 34444.81
                when nvl(overdue_principal_reload.total_reduce_fee_amount,0)>nvl(overdue_principal_reload.should_repay_fee_amount,0) then nvl(overdue_principal_reload.should_repay_fee_amount,0)+nvl(overdue_principal_reload.total_reduce_prin,0)+nvl(overdue_principal_reload.total_reduce_int,0)
                else nvl(overdue_principal_reload.should_repay_fee_amount,0)+nvl(overdue_principal_reload.total_reduce_prin,0)+nvl(overdue_principal_reload.total_reduce_int,0) end) as decimal(15,2)) end as remain_amount,
      case when ecas_loan.due_bill_no ="1000000275" and ecas_loan.d_date >= '2020-02-12' then ecas_loan.loan_init_principal
      else
      nvl(repay_detail.paid_principal,0) +
      nvl(repay_detail.paid_interest, 0) +
      nvl(repay_detail.paid_penalty,  0) +
      nvl(repay_detail.paid_svc_fee,  0) +
     nvl(repay_detail.paid_term_fee, 0) +
      nvl(repay_detail.paid_mult,     0) end as paid_amount,
      nvl(overdue_principal_reload.should_repay_fee_amount,0) as should_repay_fee_amount,
      --nvl(repay_detail.paid_principal,0)  as paid_principal,
      overdue_principal_reload.overdue_principal,
      overdue_principal_reload.overdue_interest,
      ecas_loan.overdue_svc_fee,
      ecas_loan.overdue_term_fee,
      ecas_loan.overdue_penalty,
      ecas_loan.overdue_mult_amt,
      case when overdue_day.overdue_date!='9999-12-31' then overdue_day.overdue_date
      else   null  end as overdue_date,
      case when overdue_day.overdue_date!='9999-12-31' then abs(datediff(overdue_day.overdue_date,date_sub('${ST9}',1)))+1
      else   0  end as overdue_days
    from (
      select
        product_code                      as product_id,
        due_bill_no                       as due_bill_no,
        contract_no                       as contract_no,
        apply_no                          as apply_no,
        active_date                       as loan_active_date,
        loan_init_term                    as loan_init_term,
        curr_term                         as loan_term,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 6
        else repay_term end               as loan_term_repaid,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 0
        else remain_term end              as loan_term_remain,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then 'F'
        when due_bill_no='1000000275' and d_date between '2020-01-21' and  date_sub('2020-02-06',1) then 'N'
        when due_bill_no='1000000275' and d_date between  '2020-02-06' and date_sub('2020-02-12',1) then 'O'
        when due_bill_no='1000000275' and d_date >='2020-02-12' then 'F'
        else loan_status end              as loan_status,
        terminal_reason_cd                as loan_out_reason,

        case when due_bill_no="1000000275" and d_date  between  '2020-01-05' and  date_sub('2020-02-12',1) then null
        else loan_settle_reason end  as paid_out_type,
         case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then '2020-06-03'
        when due_bill_no="1000000275" and d_date  between  '2020-01-05' and  date_sub('2020-02-12',1) then null
        when due_bill_no="1000000275" and d_date >='2020-02-12' then '2020-02-12'
        when due_bill_no="1000000163" and d_date >='2020-09-29'  then '2020-09-29'
        when due_bill_no="1000000403" and d_date >='2020-11-09'  then '2020-11-09'
        when due_bill_no="1000004836" and d_date >='2020-11-23' then '2020-11-23'
        else paid_out_date end            as paid_out_date,
        terminal_date                     as terminal_date,
        loan_init_prin                    as loan_init_principal,
        interest_rate                     as loan_init_interest_rate,
        totle_int                         as loan_init_interest,
        term_fee_rate                     as loan_init_term_fee_rate,
        totle_term_fee                    as loan_init_term_fee,
        svc_fee_rate                      as loan_init_svc_fee_rate,
        totle_svc_fee                     as loan_init_svc_fee,
        penalty_rate                      as loan_init_penalty_rate,
        case
        when product_code = '001802' and due_bill_no = '1120060318015544273567' and d_date in ('2020-06-05','2020-06-06','2020-06-14') then cast('1000.00' as decimal(15,2))
        when due_bill_no="1000000275" and  d_date  between  '2020-01-21' and date_sub('2020-02-12',1) then 0
        when due_bill_no="1000000275" and  d_date  >= '2020-02-12' then loan_init_prin
        else paid_principal end           as paid_principal,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_prin end             as overdue_principal,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        else overdue_interest end         as overdue_interest,
        overdue_svc_fee                   as overdue_svc_fee,
        overdue_term_fee                  as overdue_term_fee,
        overdue_penalty                   as overdue_penalty,
        overdue_mult_amt                  as overdue_mult_amt,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then null
        when due_bill_no='1000000275' and d_date between  '2020-02-06' and date_sub('2020-02-12',1) then '2020-02-06'
        else overdue_date end             as overdue_date,
        case
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when product_code = '001802' and due_bill_no = '1120061017361786522786' and d_date between '2020-07-08' and '2020-07-09' then 0
        when due_bill_no='1000000275' and d_date between  '2020-02-06' and date_sub('2020-02-12',1) then  datediff(date_sub('${ST9}',1),'2020-02-06')+1
        else overdue_days end             as overdue_days,
         d_date                            as d_date
      from
      (select * from ods.ecas_loan where 1 > 0 and d_date = date_sub('${ST9}',1) and p_type in ('ddht') and product_code in('001601','001602','001603')
       union all
       select * from ods.ecas_loan_ht_repair where 1 > 0 and d_date = date_sub('${ST9}',1) and p_type in ('ddht') and product_code in('001601','001602','001603')
       )tmp
    ) as ecas_loan
    left join (
    select
        schedule.due_bill_no,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_principal end ) as paid_principal,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_interest end ) as paid_interest,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_term_fee end ) as paid_term_fee,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_svc_fee end ) as paid_svc_fee,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_penalty end ) as paid_penalty,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_mult end ) as paid_mult,
        sum(case when schedule.due_bill_no='1000000275' and d_date >='2020-01-21' and d_date<'2020-02-12' then 0 else paid_term end ) as paid_term
        from
         (select
                    due_bill_no,
                     curr_term,
                     paid_out_date,
                     due_bill_no_curr_term,
                     d_date
                 from
                     (
                         select
                          tmp.due_bill_no,
                         tmp.curr_term,
                         case
                              when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                              when tmp.due_bill_no='1000000275' and tmp.d_date >='2020-01-21' and tmp.d_date<'2020-02-12' then null
                              when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then '2020-08-17'
                              when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                              when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                              when tmp.paid_out_date >tmp.d_date then null
                              when tmp.due_bill_no="1000001858" and tmp.d_date ='2020-09-21' and tmp.curr_term=0 then null
                              when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                              when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                              else tmp.paid_out_date end as paid_out_date,
                         concat(tmp.due_bill_no,'::',cast(tmp.curr_term as string)) as due_bill_no_curr_term,
                         tmp.d_date
                         from
                         (
                         select  * from  ods.ecas_repay_schedule where p_type="ddht" and d_date=date_sub('${ST9}',1) and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                         union all
                         select  * from  ods.ecas_repay_schedule_ht_repair where p_type="ddht" and d_date=date_sub('${ST9}',1) and product_code in ('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
                         )tmp
                         left join
                          (
                            select due_bill_no,curr_term,paid_out_date,schedule_status
                                    from ods.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and product_code in ('001601','001602','001603')
                                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10) and schedule_id not in ('000016006898691admin000068000001')
                          )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
                    )tmp1
                    where tmp1.paid_out_date is not null or tmp1.curr_term=0
         )schedule
         left join
         (
         select
            due_bill_no,term,
            concat(due_bill_no,'::',cast(term as string)) as due_bill_no_curr_term,
            sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'Pricinpal',        repay_amt,0)) - case
            when due_bill_no = '1120070912093993172613' and d_date between '2020-07-10' and '2020-07-13' then 2500
            when due_bill_no = '1120061910384241252747' and d_date between '2020-06-23' and '2020-02-20'     then 200
            else 0 end                                          as paid_principal,
            sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'Interest',         repay_amt,0)) as paid_interest,
            sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'TERMFee',          repay_amt,0)) as paid_term_fee,
            sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'SVCFee',           repay_amt,0)) as paid_svc_fee,
            sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'Penalty',          repay_amt,0)) as paid_penalty,
            sum(if(txn_date <=date_sub('${ST9}',1) and bnp_type = 'LatePaymentCharge',repay_amt,0)) as paid_mult,
            sum(if(txn_date <=date_sub('${ST9}',1) and term!=0 and bnp_type = 'Pricinpal',1,0)) as paid_term
          from repay_hst_repair
           --and txn_date <= '2020-02-20'
          group by due_bill_no,term,d_date,concat(due_bill_no,'::',term)
         )repay_hst on  schedule.due_bill_no_curr_term =repay_hst.due_bill_no_curr_term
        group by schedule.due_bill_no
    ) as repay_detail
    on ecas_loan.due_bill_no = repay_detail.due_bill_no
    left join (
      select
        due_bill_no,
        min(curr_term)    as loan_term2,
        min(pmt_due_date) as should_repay_date,
        product_code      as product_id
      from
      (select  * from ods.ecas_repay_schedule   where 1 > 0 and d_date = date_sub('${ST9}',1) and d_date <= nvl(origin_pmt_due_date,pmt_due_date)        -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0    and product_code in('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
        union all
        select  * from ods.ecas_repay_schedule_ht_repair   where 1 > 0 and d_date = date_sub('${ST9}',1) -- 取快照日当天的所有还款计划数据
        and d_date <= nvl(origin_pmt_due_date,pmt_due_date)        -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0    and product_code in('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
        )tmp
      group by due_bill_no,product_code
    ) as repay_schedule
    on ecas_loan.due_bill_no = repay_schedule.due_bill_no
    left join
    (
      select
            repay_schedule.due_bill_no                                                  as due_bill_no,
            sum(case
                when repay_schedule.paid_out_date <=nvl(origin_pmt_due_date,pmt_due_date)  then 0
                when date_sub('${ST9}' ,1)< pmt_due_date and curr_term!=0 then 0
                when date_sub('${ST9}' ,1) >= pmt_due_date and paid_out_date is  null and curr_term!=0 then due_term_prin
                end )                                                                   as overdue_principal,
            sum(case
            when repay_schedule.paid_out_date <=nvl(origin_pmt_due_date,pmt_due_date)  then 0
                when date_sub('${ST9}' ,1) < pmt_due_date and curr_term!=0 then 0
                when date_sub('${ST9}' ,1) >= pmt_due_date and paid_out_date is  null and curr_term!=0 then due_term_int
                end )                                                                  as overdue_interest,
                ---应还字段
                sum(nvl(due_term_int,0)) as total_due_int,
                sum(nvl(due_term_fee,0)) as total_due_term_fee,
                sum(nvl(due_svc_fee,0)) as total_due_svc_fee,
                sum(nvl(due_penalty,0)) as total_due_penalty,
                sum(nvl(due_mult_amt,0)) as total_due_mult_amt,
                sum(nvl(due_term_fee,0))+sum(nvl(due_svc_fee,0))+sum(nvl(due_penalty,0))+sum(nvl(due_mult_amt,0)) as should_repay_fee_amount,
                sum(if(nvl(reduce_term_prin,0)>nvl(due_term_prin,0),nvl(due_term_prin,0),nvl(reduce_term_prin,0) )) as total_reduce_prin,
                sum(if(nvl(reduce_term_int,0)>nvl(due_term_int,0),nvl(due_term_int,0),nvl(reduce_term_int,0))) as total_reduce_int,
                --减免部分 防止减免金额大于应还金额加上判断 1000000222 借据减免费用大于应还费用
                sum(if(nvl(reduce_term_fee,0)>nvl(due_term_fee,0),nvl(due_term_fee,0),nvl(reduce_term_fee,0))) as total_reduce_term_fee,
                sum(case when due_bill_no="1000000222" then 0
                         when nvl(reduce_penalty,0)>nvl(due_penalty,0) then nvl(due_penalty,0)
                         else nvl(reduce_penalty,0) end) as total_reduce_penalty,
                sum(if(nvl(reduce_svc_fee,0)>nvl(due_svc_fee,0),nvl(due_svc_fee,0),nvl(reduce_svc_fee,0))) as total_reduce_svc_fee,
                sum(if(nvl(reduce_mult_amt,0)>nvl(due_mult_amt,0),nvl(due_mult_amt,0),nvl(reduce_mult_amt,0))) as total_reduce_mult_amt,
                sum(if(nvl(reduce_term_fee,0)>nvl(due_term_fee,0),nvl(due_term_fee,0),nvl(reduce_term_fee,0)))+
                sum(case when due_bill_no="1000000222" then 0
                         when nvl(reduce_penalty,0)>nvl(due_penalty,0) then nvl(due_penalty,0)
                         else nvl(reduce_penalty,0) end) +
                         +sum(if(nvl(reduce_svc_fee,0)>nvl(due_svc_fee,0),nvl(due_svc_fee,0),nvl(reduce_svc_fee,0)))
                         +sum(if(nvl(reduce_mult_amt,0)>nvl(due_mult_amt,0),nvl(due_mult_amt,0),nvl(reduce_mult_amt,0))) as total_reduce_fee_amount
        from
        (
            select
                tmp.due_bill_no,
                tmp.origin_pmt_due_date,
                tmp.pmt_due_date,
                tmp.due_term_prin,
                tmp.due_term_int,
                tmp.curr_term,
                tmp.due_term_fee,
               case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 600
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0 then 600
                         else tmp.due_svc_fee end                                                       as due_svc_fee,
               case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 1559.51
                         when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0  then 1245
                         else tmp.due_penalty end                                                        as due_penalty,
                tmp.due_mult_amt,
                tmp.reduce_term_prin,
                tmp.reduce_term_int,
                tmp.reduce_term_fee,
                   case when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then new_schedule.reduce_svc_fee
                when tmp.due_bill_no="1000000061" then 0
                when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 600
                when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0 then 600
                else tmp.reduce_svc_fee                                                                                                      end as reduce_svc_fee,

                case when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and tmp.curr_term=0 then 728.51
                     when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and tmp.curr_term=0  then 48.56 else tmp.reduce_penalty end  as reduce_penalty,
                tmp.reduce_mult_amt,
                case
                     when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                     when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then '2020-08-17'
                     when tmp.paid_out_date >tmp.d_date then null
                     when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                     when tmp.due_bill_no='1000000275' and  tmp.d_date<'2020-02-12' then null
                     when tmp.due_bill_no='1000000275' and  tmp.d_date>='2020-02-12' then '2020-02-12'
                     when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                else tmp.paid_out_date end  as paid_out_date
            from (
            select * from ods.ecas_repay_schedule where d_date =date_sub('${ST9}',1) and p_type in ('ddht') and product_code in('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
            union all
            select * from ods.ecas_repay_schedule_ht_repair where d_date = date_sub('${ST9}',1) and p_type in ('ddht') and product_code in('001601','001602','001603') and schedule_id not in ('000016006898691admin000068000001')
            )tmp
            left join
            (
                    select due_bill_no,curr_term,paid_out_date,schedule_status,reduce_svc_fee
                    from ods.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and product_code in ('001601','001602','001603')
                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10) and schedule_id not in ('000016006898691admin000068000001')
            )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term

        ) repay_schedule
        group by repay_schedule.due_bill_no
    ) as overdue_principal_reload
    on ecas_loan.due_bill_no = overdue_principal_reload.due_bill_no
    left join (
         select
        due_bill_no ,
        min(overdue_date) as overdue_date
        from
        (
        select
        loan.due_bill_no,
        case when schedule.paid_out_date is null and date_sub('${ST9}',1)>=pmt_due_date then pmt_due_date
        else '9999-12-31' end as overdue_date
        from
            (
                select
                due_bill_no,
                curr_term
                from
                (
                    select * from ods.ecas_loan where p_type='ddht' and product_code in ('001601','001602','001603') and d_date=date_sub('${ST9}',1)
                     union all
                    select * from ods.ecas_loan_ht_repair where p_type='ddht' and product_code in ('001601','001602','001603') and d_date=date_sub('${ST9}',1)
                )tmp
            )loan left join
            (
                select tmp.due_bill_no,tmp.curr_term,
                case
                    when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                    when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then '2020-08-17'
                    when tmp.due_bill_no="1000000163" and tmp.d_date >='2020-09-29' and (tmp.curr_term=0 or tmp.curr_term between 6 and 36) then '2020-09-29'
                    when tmp.due_bill_no="1000000403" and tmp.d_date >='2020-11-09' and (tmp.curr_term=0 or tmp.curr_term between 3 and 36)  then '2020-11-09'
                    when tmp.paid_out_date >tmp.d_date then null
                    when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                    when tmp.due_bill_no='1000000275' and tmp.d_date<'2020-02-12' then null
                    when tmp.due_bill_no='1000000275' and  tmp.d_date>='2020-02-12' then '2020-02-12'
                    when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                else tmp.paid_out_date end as paid_out_date,
                tmp.schedule_status,tmp.pmt_due_date
                from
                (
                select * from ods.ecas_repay_schedule where d_date=date_sub('${ST9}',1) and  p_type='ddht'  and product_code in ('001601','001602','001603') and curr_term>0
                union all
                select * from ods.ecas_repay_schedule_ht_repair where d_date=date_sub('${ST9}',1) and  p_type='ddht' and product_code in ('001601','001602','001603') and curr_term>0
                )tmp
                left join
                (
                    select due_bill_no,curr_term,paid_out_date,schedule_status
                    from ods.ecas_repay_schedule where d_date='${d_date}'  and p_type='ddht' and product_code in ('001601','001602','001603')
                    and paid_out_date is not null and paid_out_date<=date_add('${ST9}',10) and schedule_id not in ('000016006898691admin000068000001')
                )new_schedule on tmp.due_bill_no=new_schedule.due_bill_no and tmp.curr_term=new_schedule.curr_term
             )schedule on loan.due_bill_no=schedule.due_bill_no
             where  schedule.curr_term<=loan.curr_term
        )tmp
        group by due_bill_no
    )overdue_day  on ecas_loan.due_bill_no = overdue_day.due_bill_no
  ) as yesterday
  on  is_empty(today.product_id             ,'a') = is_empty(yesterday.product_id             ,'a')
  and is_empty(today.due_bill_no            ,'a') = is_empty(yesterday.due_bill_no            ,'a')
  and is_empty(today.apply_no               ,'a') = is_empty(yesterday.apply_no               ,'a')
  and is_empty(today.loan_active_date       ,'a') = is_empty(yesterday.loan_active_date       ,'a')
  and is_empty(today.loan_init_term         ,'a') = is_empty(yesterday.loan_init_term         ,'a')
  and is_empty(today.loan_term              ,'a') = is_empty(yesterday.loan_term              ,'a')
  and is_empty(today.should_repay_date      ,'a') = is_empty(yesterday.should_repay_date      ,'a')
  and is_empty(today.loan_term_repaid       ,'a') = is_empty(yesterday.loan_term_repaid       ,'a')
  and is_empty(today.loan_term_remain       ,'a') = is_empty(yesterday.loan_term_remain       ,'a')
  and is_empty(today.loan_status            ,'a') = is_empty(yesterday.loan_status            ,'a')
  and is_empty(today.loan_out_reason        ,'a') = is_empty(yesterday.loan_out_reason        ,'a')
  and is_empty(today.paid_out_type          ,'a') = is_empty(yesterday.paid_out_type          ,'a')
  and is_empty(today.paid_out_date          ,'a') = is_empty(yesterday.paid_out_date          ,'a')
  and is_empty(today.terminal_date          ,'a') = is_empty(yesterday.terminal_date          ,'a')
  and is_empty(today.loan_init_principal    ,'a') = is_empty(yesterday.loan_init_principal    ,'a')
  and is_empty(today.loan_init_interest_rate,'a') = is_empty(yesterday.loan_init_interest_rate,'a')
  and is_empty(today.loan_init_interest     ,'a') = is_empty(yesterday.loan_init_interest     ,'a')
  and is_empty(today.loan_init_term_fee_rate,'a') = is_empty(yesterday.loan_init_term_fee_rate,'a')
  and is_empty(today.loan_init_term_fee     ,'a') = is_empty(yesterday.loan_init_term_fee     ,'a')
  and is_empty(today.loan_init_svc_fee_rate ,'a') = is_empty(yesterday.loan_init_svc_fee_rate ,'a')
  and is_empty(today.loan_init_svc_fee      ,'a') = is_empty(yesterday.loan_init_svc_fee      ,'a')
  and is_empty(today.loan_init_penalty_rate ,'a') = is_empty(yesterday.loan_init_penalty_rate ,'a')
  and is_empty(today.paid_amount         ,'a') = is_empty(yesterday.paid_amount         ,'a') --汇通单独还罚息
    and is_empty(today.should_repay_fee_amount         ,'a') = is_empty(yesterday.should_repay_fee_amount         ,'a') --汇通罚息变化 导致 借据表和还款计划两张表计算剩余金额出现问题
  and is_empty(today.overdue_principal      ,'a') = is_empty(yesterday.overdue_principal      ,'a')
  and is_empty(today.overdue_interest       ,'a') = is_empty(yesterday.overdue_interest       ,'a')
  and is_empty(today.overdue_svc_fee        ,'a') = is_empty(yesterday.overdue_svc_fee        ,'a')
  and is_empty(today.overdue_term_fee       ,'a') = is_empty(yesterday.overdue_term_fee       ,'a')
  and is_empty(today.overdue_penalty        ,'a') = is_empty(yesterday.overdue_penalty        ,'a')
  and is_empty(today.overdue_mult_amt       ,'a') = is_empty(yesterday.overdue_mult_amt       ,'a')
  and is_empty(today.overdue_date           ,'a') = is_empty(yesterday.overdue_date           ,'a')
  and is_empty(today.overdue_days           ,'a') = is_empty(yesterday.overdue_days           ,'a')
  where yesterday.due_bill_no is null
)
-- insert overwrite table ods_new_s${db_suffix}.loan_info_intsert partition(is_settled = 'no',product_id)
insert overwrite table ods_new_s.loan_info partition(is_settled = 'no',product_id)
select
  due_bill_no,
  apply_no,
  loan_active_date,
  loan_init_principal,
  loan_init_term,
  max(loan_term)         over(partition by due_bill_no order by s_d_date) as loan_term,
  max(should_repay_date) over(partition by due_bill_no order by s_d_date) as should_repay_date,
  loan_term_repaid,
  loan_term_remain,
  loan_init_interest,
  loan_init_term_fee,
  loan_init_svc_fee,
  loan_status,
  loan_status_cn,
  loan_out_reason,
  paid_out_type,
  paid_out_type_cn,
  paid_out_date,
  terminal_date,
  paid_amount,
  paid_principal,
  paid_interest,
  paid_penalty,
  paid_svc_fee,
  paid_term_fee,
  paid_mult,
  remain_amount,
  remain_principal,
  remain_interest,
  remain_svc_fee,
  remain_term_fee,
  overdue_principal,
  overdue_interest,
  overdue_svc_fee,
  overdue_term_fee,
  overdue_penalty,
  overdue_mult_amt,
  min(overdue_date_start) over(partition by due_bill_no order by s_d_date) as overdue_date_first,
  overdue_date_start,
  overdue_days,
  overdue_date,
  overdue_date_start as dpd_begin_date,
  overdue_days as dpd_days,
  0 as dpd_days_count,
  max(overdue_days) over(partition by due_bill_no order by s_d_date) as dpd_days_max,
  collect_out_date as collect_out_date,
  overdue_term,
  count(distinct if(overdue_days > 0,overdue_term,null)) over(partition by due_bill_no order by s_d_date)    as overdue_terms_count,
  max(overdue_terms_max)                                 over(partition by due_bill_no order by s_d_date)    as overdue_terms_max,
  nvl(sum(distinct overdue_principal)                    over(partition by due_bill_no order by s_d_date),0) as overdue_principal_accumulate,
  nvl(max(overdue_principal)                             over(partition by due_bill_no order by s_d_date),0) as overdue_principal_max,
  s_d_date,
  e_d_date,
  effective_time,
  expire_time,
  product_id
from (
  select
    loan_info.due_bill_no,
    loan_info.apply_no,
    loan_info.loan_active_date,
    loan_info.loan_init_principal,
    loan_info.loan_init_term,
    loan_info.loan_term,
    loan_info.should_repay_date,
    loan_info.loan_term_repaid,
    loan_info.loan_term_remain,
    loan_info.loan_status,
    loan_info.loan_status_cn,
    loan_info.loan_out_reason,
    loan_info.paid_out_type,
    loan_info.paid_out_type_cn,
    loan_info.paid_out_date,
    loan_info.terminal_date,
    loan_info.loan_init_interest,
    loan_info.loan_init_term_fee,
    loan_info.loan_init_svc_fee,
    loan_info.paid_amount,
    loan_info.paid_principal,
    loan_info.paid_interest,
    loan_info.paid_penalty,
    loan_info.paid_svc_fee,
    loan_info.paid_term_fee,
    loan_info.paid_mult,
    loan_info.remain_amount,
    loan_info.remain_principal,
    loan_info.remain_interest,
    loan_info.remain_svc_fee,
    loan_info.remain_term_fee,
    loan_info.overdue_principal,
    loan_info.overdue_interest,
    loan_info.overdue_svc_fee,
    loan_info.overdue_term_fee,
    loan_info.overdue_penalty,
    loan_info.overdue_mult_amt,
    loan_info.overdue_date_start,
    loan_info.overdue_days,
    loan_info.overdue_date,
    loan_info.collect_out_date,
    loan_info.overdue_term,
    loan_info.overdue_terms_max,
    loan_info.s_d_date,
    if(loan_info.e_d_date > '${ST9}' and ods_new_s_loan.due_bill_no is not null,ods_new_s_loan.d_date,if(loan_info.e_d_date = '${ST9}' and ods_new_s_loan.due_bill_no is null,'3000-12-31',loan_info.e_d_date)) as e_d_date,
    loan_info.effective_time,
    if(to_date(loan_info.expire_time) > '${ST9}' and ods_new_s_loan.due_bill_no is not null,ods_new_s_loan.update_time,loan_info.expire_time) as expire_time,
    loan_info.product_id
  from (select * from ods_new_s${db_suffix}.loan_info_tmp where 1 > 0 and product_id in ('001601','001602','001603')) as loan_info
  left join ods_new_s_loan
  on loan_info.due_bill_no = ods_new_s_loan.due_bill_no
  union all
  select
    ods_new_s_loan.due_bill_no,
    ods_new_s_loan.apply_no,
    ods_new_s_loan.loan_active_date,
    ods_new_s_loan.loan_init_principal,
    ods_new_s_loan.loan_init_term,
    ods_new_s_loan.loan_term,
    ods_new_s_loan.should_repay_date,
    ods_new_s_loan.loan_term_repaid,
    ods_new_s_loan.loan_term_remain,
    ods_new_s_loan.loan_status,
    ods_new_s_loan.loan_status_cn,
    ods_new_s_loan.loan_out_reason,
    ods_new_s_loan.paid_out_type,
    ods_new_s_loan.paid_out_type_cn,
    ods_new_s_loan.paid_out_date,
    ods_new_s_loan.terminal_date,
    ods_new_s_loan.loan_init_interest,
    ods_new_s_loan.loan_init_term_fee,
    ods_new_s_loan.loan_init_svc_fee,
    ods_new_s_loan.paid_amount,
    ods_new_s_loan.paid_principal,
    ods_new_s_loan.paid_interest,
    ods_new_s_loan.paid_penalty,
    ods_new_s_loan.paid_svc_fee,
    ods_new_s_loan.paid_term_fee,
    ods_new_s_loan.paid_mult,
    ods_new_s_loan.remain_amount,
    ods_new_s_loan.remain_principal,
    ods_new_s_loan.remain_interest,
    ods_new_s_loan.remain_svc_fee,
    ods_new_s_loan.remain_term_fee,
    ods_new_s_loan.overdue_principal,
    ods_new_s_loan.overdue_interest,
    ods_new_s_loan.overdue_svc_fee,
    ods_new_s_loan.overdue_term_fee,
    ods_new_s_loan.overdue_penalty,
    ods_new_s_loan.overdue_mult_amt,
    ods_new_s_loan.overdue_date_start,
    ods_new_s_loan.overdue_days,
    ods_new_s_loan.overdue_date,
    ods_new_s_loan.collect_out_date,
    ods_new_s_loan.overdue_term,
    ods_new_s_loan.overdue_terms_max,
    ods_new_s_loan.d_date as s_d_date,
    ods_new_s_loan.e_d_date,
    ods_new_s_loan.update_time as effective_time,
    ods_new_s_loan.expire_time,
    ods_new_s_loan.product_id
  from ods_new_s_loan
) as tmp
;


-- DROP TABLE IF EXISTS `ods_new_s${db_suffix}.loan_info`;
-- ALTER TABLE ods_new_s${db_suffix}.loan_info_intsert RENAME TO ods_new_s${db_suffix}.loan_info;











-- 凤金
-- select distinct
--   concat('00',loan_code)                                              as product_id,
--   loan_id                                                             as loan_id,
--   due_bill_no                                                         as due_bill_no,
--   contr_nbr                                                           as contract_no,
--   due_bill_no                                                         as apply_no,
--   purpose                                                             as loan_usage,
--   register_date                                                       as register_date,
--   request_time                                                        as request_time,
--   active_date                                                         as loan_active_date,
--   cast(cycle_day as decimal(2,0))                                     as cycle_day,
--   loan_expire_date                                                    as loan_expire_date,
--   loan_type                                                           as loan_type,
--   case loan_type
--   when 'R'    then '消费转分期'
--   when 'C'    then '现金分期'
--   when 'B'    then '账单分期'
--   when 'P'    then 'POS分期'
--   when 'M'    then '大额分期（专项分期）'
--   when 'MCAT' then '随借随还'
--   when 'MCEP' then '等额本金'
--   when 'MCEI' then '等额本息'
--   else loan_type
--   end                                                                 as loan_type_cn,
--   loan_init_term                                                      as loan_init_term,
--   curr_term                                                           as loan_term,
--   0                                                                   as loan_term_repaid,
--   remain_term                                                         as loan_term_remain,
--   loan_status                                                         as loan_status,
--   case loan_status
--   when 'N' then '正常'
--   when 'O' then '逾期'
--   when 'F' then '已还清'
--   else loan_status
--   end                                                                 as loan_status_cn,
--   terminal_reason_cd                                                  as loan_out_reason,
--   force_flag                                                          as paid_out_type,
--   paid_out_date                                                       as paid_out_date,
--   terminal_date                                                       as terminal_date,
--   loan_init_prin                                                      as loan_init_principal,
--   interest_rate                                                       as loan_init_interest_rate,
--   pay_interest                                                        as loan_init_interest,
--   fee_rate                                                            as loan_init_term_fee_rate,
--   loan_init_fee                                                       as loan_init_term_fee,
--   installment_fee_rate                                                as loan_init_svc_fee_rate,
--   tol_svc_fee                                                         as loan_init_svc_fee,
--   penalty_rate                                                        as loan_init_penalty_rate,
--   paid_fee                                                            as paid_amount,
--   paid_principal                                                      as paid_principal,
--   paid_interest                                                       as paid_interest,
--   paid_penalty                                                        as paid_penalty,
--   paid_svc_fee                                                        as paid_svc_fee,
--   0                                                                   as paid_term_fee,
--   0                                                                   as paid_mult,
--   remain_amount                                                       as remain_amount,
--   remain_principal                                                    as remain_principal,
--   remain_interest                                                     as remain_interest,
--   prin                                                                as overdue_principal,
--   interest                                                            as overdue_interest,
--   0                                                                   as overdue_svc_fee,
--   0                                                                   as overdue_term_fee,
--   penalty                                                             as overdue_penalty,
--   0                                                                   as overdue_mult_amt,
--   overdue_date                                                        as overdue_date,
--   overdue_days                                                        as overdue_days,
--   if(overdue_date is null,null,first_value(overdue_date) over(partition by due_bill_no order by cast(overdue_date as timestamp))) as first_overdue_date,
--   null                                                                as dpd_begin_date,
--   0                                                                   as dpd_days,
--   0                                                                   as dpd_days_count,
--   max_dpd                                                             as dpd_days_max,
--   collect_out_date                                                    as collect_out_date,
--   0                                                                   as overdue_term,
--   0                                                                   as overdue_terms_count,
--   0                                                                   as overdue_terms_max,
--   0                                                                   as overdue_principal_accumulate,
--   0                                                                   as overdue_principal_max,
--   null                                                                as mob,
--   null                                                                as sync_date,
--   create_time                                                         as create_time,
--   update_time                                                         as update_time
-- from (
--   select distinct
--     ref_nbr,
--     d_date,
--     loan_code,
--     loan_id,
--     contr_nbr,
--     due_bill_no,
--     purpose,
--     register_date,
--     cast(datefmt(request_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as request_time,
--     active_date,
--     loan_expire_date,
--     loan_type,
--     loan_init_term,
--     curr_term,
--     remain_term,
--     loan_status,
--     terminal_reason_cd,
--     force_flag,
--     paid_out_date,
--     terminal_date,
--     loan_init_prin,
--     interest_rate,
--     pay_interest,
--     fee_rate,
--     loan_init_fee,
--     installment_fee_rate,
--     tol_svc_fee,
--     penalty_rate,
--     paid_fee,
--     paid_principal,
--     paid_interest,
--     paid_penalty,
--     paid_svc_fee,
--     (loan_init_prin + pay_interest + loan_init_fee + tol_svc_fee - paid_fee) as remain_amount,
--     (loan_init_prin - paid_principal)                                        as remain_principal,
--     (pay_interest - paid_interest)                                           as remain_interest,
--     overdue_date,
--     if(overdue_date is null,0,datediff('${curr_date}',overdue_date))           as overdue_days,
--     max_dpd,
--     collect_out_date,
--     cast(datefmt(create_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)       as create_time,
--     cast(datefmt(lst_upd_time,'ms','yyyy-MM-dd HH:mm:ss') as timestamp)      as update_time
--   from ods.ccs_loan
--   where d_date = '${ST9}'
-- ) as ccs_loan
-- left join (
--   select distinct
--     ref_nbr,
--     day(loan_pmt_due_date) as cycle_day
--   from ods.ccs_repay_schedule
-- ) as ccs_repay_schedule
-- on ccs_loan.ref_nbr = ccs_repay_schedule.ref_nbr
-- left join (
--   select
--     sum(past_principal + ctd_principal)            as prin,
--     sum(past_interest + ctd_interest)              as interest,
--     sum(penalty_acru + past_penalty + ctd_penalty) as penalty,
--     ref_nbr,
--     d_date as ccs_plan_d_date
--   from ods.ccs_plan
--   group by ref_nbr,d_date
-- ) as ccs_plan
-- on ccs_loan.ref_nbr = ccs_plan.ref_nbr and ccs_loan.d_date = ccs_plan.ccs_plan_d_date
