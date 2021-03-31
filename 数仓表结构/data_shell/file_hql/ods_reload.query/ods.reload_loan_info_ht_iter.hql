
set hive.support.quoted.identifiers=None;
-- 设置 Container 大小
set hive.tez.container.size=4096;
set tez.am.resource.memory.mb=4096;
-- 合并小文件
set hive.merge.tezfiles=true;
set hive.merge.size.per.task=128000000; -- 128M
set hive.merge.smallfiles.avgsize=128000000; -- 128M
-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 禁用 Hive 矢量执行
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;
set hivevar:p_types='ddht','htgy';
set hivevar:product_id_list='001601','001602','001603','002201','002202','002203';
--set hivevar:ST9=2021-01-01;
--set hivevar:d_date=2021-01-01;

with repay_hst_repair as (
select
      repayhst.due_bill_no,repayhst.term,bnp_type,repayhst.d_date,repay_amt,
      if(repair_hst.order_id is not null,repair_hst.paid_out_date,repayhst.txn_date) as txn_date
         from (
                select * from   stage.ecas_repay_hst  where 1 > 0  and d_date ='${d_date}'    and p_type in (${p_types})   and txn_date <= date_add('${ST9}',30)
                --11月修数  删除掉汇通的两笔线下还款的罚息实还数据
                and payment_id not in ('000016043097811admin000068000001','000016043095431admin000068000001')
          )repayhst
           left join (
          select distinct order_id,paid_out_date  from  stage.schedule_repay_order_info_ddht  where biz_date='${d_date}'  and product_id in (${product_id_list})
          )repair_hst on repayhst.order_id=repair_hst.order_id

)
insert overwrite table ods.loan_info_inter partition(biz_date,product_id)
  select
  today.due_bill_no                                           as due_bill_no,
  today.apply_no                                              as apply_no,
  today.loan_active_date                                      as loan_active_date,
  today.loan_init_term                                        as loan_init_term,
  today.loan_init_principal                                   as loan_init_principal,
  today.loan_init_interest                                    as loan_init_interest,
  today.loan_init_term_fee                                    as loan_init_term_fee,
  today.loan_init_svc_fee                                     as loan_init_svc_fee,
  today.loan_term                                             as loan_term,
  today.loan_term                                             as account_age,
  today.should_repay_date                                     as should_repay_date,
  today.loan_term_repaid                                      as loan_term_repaid,
  today.loan_term_remain                                      as loan_term_remain,
  today.loan_status                                           as loan_status,
  today.loan_status_cn                                        as loan_status_cn,
  today.loan_out_reason                                       as loan_out_reason,
  today.paid_out_type                                         as paid_out_type,
  today.paid_out_type_cn                                      as paid_out_type_cn,
  today.paid_out_date                                         as paid_out_date,
  today.terminal_date                                         as terminal_date,
  nvl(today.paid_amount,0)                                    as paid_amount,
  nvl(today.paid_principal,0)                                 as paid_principal,
  nvl(today.paid_interest,0)                                  as paid_interest,
  nvl(today.paid_penalty,0)                                   as paid_penalty,
  nvl( today.paid_svc_fee,0)                                  as paid_svc_fee,
  nvl( today.paid_term_fee,0)                                 as paid_term_fee,
  nvl( today.paid_mult,0)                                     as paid_mult,
  nvl(today.remain_amount,0)                                  as remain_amount,
  nvl( today.remain_principal,0)                              as remain_principal,
  nvl( today.remain_interest,0)                               as remain_interest,
  nvl(today.remain_svc_fee,0)                                 as remain_svc_fee,
  nvl( today.remain_term_fee,0)                               as remain_term_fee,
  0                                                           as remain_othAmounts,
  nvl( today.overdue_principal,0)                             as overdue_principal,
  nvl( today.overdue_interest,0)                              as overdue_interest,
  nvl( today.overdue_svc_fee,0)                               as overdue_svc_fee,
  nvl( today.overdue_term_fee,0)                              as overdue_term_fee,
  nvl( today.overdue_penalty,0)                               as overdue_penalty,
  nvl(  today.overdue_mult_amt,0)                             as overdue_mult_amt,
 today.overdue_date                                           as overdue_date_start,
  today.overdue_days                                          as overdue_days,
  date_add(today.overdue_date,today.overdue_days - 1)         as overdue_date,
  today.collect_out_date                                      as collect_out_date,
  0                                                           as dpd_days_max,
  today.overdue_term                                          as overdue_term,
  0                                                           as overdue_terms_count,
  today.overdue_terms_max                                     as overdue_terms_max,
  0                                                           as overdue_principal_accumulate,
  0                                                           as overdue_principal_max,
  today.create_time                                           as create_time,
  today.update_time                                           as update_time,
  today.d_date                                                as biz_date,
  today.product_id                                            as product_id
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
       select * from stage.ecas_loan where 1 > 0 and d_date = '${ST9}' and p_type in (${p_types})and product_code in (${product_id_list})
        union all
      select * from stage.ecas_loan_ht_repair  where 1 > 0 and d_date = '${ST9}' and p_type in (${p_types}) and product_code in (${product_id_list})
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
                            when tmp.due_bill_no="1000000223" and tmp.d_date >='2020-10-07' and tmp.curr_term=10 then '2020-10-07'
                            when tmp.due_bill_no="1000000720" and tmp.d_date >='2020-10-16' and tmp.curr_term=3 then '2020-10-16'
                            when tmp.due_bill_no="1000000060" and tmp.d_date >='2020-10-04' and tmp.curr_term=10 then '2020-10-04'
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
                         select  * from  stage.ecas_repay_schedule where p_type in (${p_types})  and d_date='${ST9}' and product_code in (${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
                         union all
                         select  * from  stage.ecas_repay_schedule_ht_repair where p_type in (${p_types}) and d_date='${ST9}' and product_code in (${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
                         )tmp
                         left join
                          (
                            select due_bill_no,curr_term,paid_out_date,schedule_status
                                    from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type in (${p_types}) and product_code in (${product_id_list})
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
      select * from stage.ecas_repay_schedule where d_date='${ST9}' and p_type in (${p_types}) and curr_term > 0  and product_code in(${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
    union all
     select * from stage.ecas_repay_schedule_ht_repair where d_date='${ST9}' and  p_type in (${p_types}) and curr_term > 0  and product_code in (${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
      )tmp
      where 1 > 0
        and d_date = '${ST9}'      -- 取快照日当天的所有还款计划数据
        and p_type in (${p_types})
        and d_date <= nvl(origin_pmt_due_date,pmt_due_date) -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0  and product_code in(${product_id_list})       -- 过滤掉汇通的第 0 期的情况
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
      select * from stage.ecas_repay_schedule  where d_date='${ST9}' and product_code in (${product_id_list}) and pmt_due_date <= d_date and curr_term>0  and schedule_id not in ('000016006898691admin000068000001')
        union all
      select * from stage.ecas_repay_schedule_ht_repair  where d_date='${ST9}' and product_code in (${product_id_list}) and pmt_due_date <= d_date and curr_term>0 and schedule_id not in ('000016006898691admin000068000001')
      )tmp
      where 1 > 0
        and d_date='${ST9}' and product_code in (${product_id_list})
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
                     when tmp.due_bill_no="1000000223" and tmp.d_date >='2020-10-07' and tmp.curr_term=10 then '2020-10-07'
                     when tmp.due_bill_no="1000000720" and tmp.d_date >='2020-10-16' and tmp.curr_term=3 then '2020-10-16'
                     when tmp.due_bill_no="1000000060" and tmp.d_date >='2020-10-04' and tmp.curr_term=10 then '2020-10-04'
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
            select * from stage.ecas_repay_schedule where d_date = '${ST9}' and p_type in (${p_types}) and product_code in (${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
            union all
            select * from stage.ecas_repay_schedule_ht_repair where d_date = '${ST9}' and p_type in (${p_types}) and product_code in(${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
            )tmp
            left join
            (
                    select due_bill_no,curr_term,paid_out_date,schedule_status,reduce_svc_fee
                    from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type in (${p_types}) and product_code in (${product_id_list})
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
                    select * from stage.ecas_loan where p_type in (${p_types}) and product_code in (${product_id_list}) and d_date='${ST9}'
                     union all
                    select * from stage.ecas_loan_ht_repair where p_type in (${p_types}) and product_code in (${product_id_list}) and d_date='${ST9}'
                )tmp
            )loan left join
            (
                select tmp.due_bill_no,tmp.curr_term,
                case
                     when tmp.due_bill_no="1000000223" and tmp.d_date >='2020-10-07' and tmp.curr_term=10 then '2020-10-07'
                      when tmp.due_bill_no="1000000720" and tmp.d_date >='2020-10-16' and tmp.curr_term=3 then '2020-10-16'
                      when tmp.due_bill_no="1000000060" and tmp.d_date >='2020-10-04' and tmp.curr_term=10 then '2020-10-04'
                    when tmp.due_bill_no='1000000381' and tmp.d_date >='2020-08-17' then '2020-08-17'
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
                    select * from stage.ecas_repay_schedule where d_date='${ST9}' and  p_type in (${p_types})  and product_code in (${product_id_list}) and curr_term>0 and schedule_id not in ('000016006898691admin000068000001')
                    union all
                    select * from stage.ecas_repay_schedule_ht_repair where d_date='${ST9}' and  p_type in (${p_types}) and product_code in (${product_id_list}) and curr_term>0 and schedule_id not in ('000016006898691admin000068000001')
                )tmp
                left join
                (
                    select due_bill_no,curr_term,paid_out_date,schedule_status
                    from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type in (${p_types}) and product_code in (${product_id_list})
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
      (select * from stage.ecas_loan where 1 > 0 and d_date = date_sub('${ST9}',1) and p_type in (${p_types}) and product_code in (${product_id_list})
       union all
       select * from stage.ecas_loan_ht_repair where 1 > 0 and d_date = date_sub('${ST9}',1) and p_type in (${p_types}) and product_code in (${product_id_list})
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
                              when tmp.due_bill_no="1000000223" and tmp.d_date >='2020-10-07' and tmp.curr_term=10 then '2020-10-07'
                              when tmp.due_bill_no="1000000720" and tmp.d_date >='2020-10-16' and tmp.curr_term=3 then '2020-10-16'
                              when tmp.due_bill_no="1000000060" and tmp.d_date >='2020-10-04' and tmp.curr_term=10 then '2020-10-04'
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
                         select  * from  stage.ecas_repay_schedule where p_type in (${p_types}) and d_date=date_sub('${ST9}',1) and product_code in (${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
                         union all
                         select  * from  stage.ecas_repay_schedule_ht_repair where p_type in (${p_types}) and d_date=date_sub('${ST9}',1) and product_code in (${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
                         )tmp
                         left join
                          (
                            select due_bill_no,curr_term,paid_out_date,schedule_status
                                    from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type in (${p_types}) and product_code in (${product_id_list})
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
      (select  * from stage.ecas_repay_schedule   where 1 > 0 and d_date = date_sub('${ST9}',1) and d_date <= nvl(origin_pmt_due_date,pmt_due_date)        -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0    and product_code in(${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
        union all
        select  * from stage.ecas_repay_schedule_ht_repair   where 1 > 0 and d_date = date_sub('${ST9}',1) -- 取快照日当天的所有还款计划数据
        and d_date <= nvl(origin_pmt_due_date,pmt_due_date)        -- 取快照日当天之后的所有还款计划数据（取最小值时，即为当前期数、应还日）
        and curr_term > 0    and product_code in(${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
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
                     when tmp.due_bill_no="1000000223" and tmp.d_date >='2020-10-07' and tmp.curr_term=10 then '2020-10-07'
                     when tmp.due_bill_no="1000000720" and tmp.d_date >='2020-10-16' and tmp.curr_term=3 then '2020-10-16'
                     when tmp.due_bill_no="1000000060" and tmp.d_date >='2020-10-04' and tmp.curr_term=10 then '2020-10-04'
                     when tmp.due_bill_no="1000004836" and tmp.d_date >='2020-11-23' then '2020-11-23'
                     when tmp.due_bill_no="1000000381" and tmp.d_date >='2020-08-17' then '2020-08-17'
                     when tmp.paid_out_date >tmp.d_date then null
                     when tmp.d_date>=new_schedule.paid_out_date then new_schedule.paid_out_date
                     when tmp.due_bill_no='1000000275' and  tmp.d_date<'2020-02-12' then null
                     when tmp.due_bill_no='1000000275' and  tmp.d_date>='2020-02-12' then '2020-02-12'
                     when tmp.paid_out_date!=new_schedule.paid_out_date and tmp.paid_out_date is not null then new_schedule.paid_out_date
                else tmp.paid_out_date end  as paid_out_date
            from (
            select * from stage.ecas_repay_schedule where d_date =date_sub('${ST9}',1) and p_type in (${p_types}) and product_code in (${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
            union all
            select * from stage.ecas_repay_schedule_ht_repair where d_date = date_sub('${ST9}',1) and p_type in (${p_types}) and product_code in (${product_id_list}) and schedule_id not in ('000016006898691admin000068000001')
            )tmp
            left join
            (
                    select due_bill_no,curr_term,paid_out_date,schedule_status,reduce_svc_fee
                    from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type in (${p_types}) and product_code in (${product_id_list})
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
                    select * from stage.ecas_loan where p_type in (${p_types}) and product_code in (${product_id_list}) and d_date=date_sub('${ST9}',1)
                     union all
                    select * from stage.ecas_loan_ht_repair where p_type in (${p_types}) and product_code in (${product_id_list}) and d_date=date_sub('${ST9}',1)
                )tmp
            )loan left join
            (
                select tmp.due_bill_no,tmp.curr_term,
                case
                     when tmp.due_bill_no="1000000223" and tmp.d_date >='2020-10-07' and tmp.curr_term=10 then '2020-10-07'
                     when tmp.due_bill_no="1000000720" and tmp.d_date >='2020-10-16' and tmp.curr_term=3 then '2020-10-16'
                     when tmp.due_bill_no="1000000060" and tmp.d_date >='2020-10-04' and tmp.curr_term=10 then '2020-10-04'
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
                select * from stage.ecas_repay_schedule where d_date=date_sub('${ST9}',1) and  p_type in (${p_types})  and product_code in (${product_id_list}) and curr_term>0
                union all
                select * from stage.ecas_repay_schedule_ht_repair where d_date=date_sub('${ST9}',1) and  p_type in (${p_types}) and product_code in (${product_id_list}) and curr_term>0
                )tmp
                left join
                (
                    select due_bill_no,curr_term,paid_out_date,schedule_status
                    from stage.ecas_repay_schedule where d_date='${d_date}'  and p_type in (${p_types}) and product_code in (${product_id_list})
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
  --limit 1
  ;












