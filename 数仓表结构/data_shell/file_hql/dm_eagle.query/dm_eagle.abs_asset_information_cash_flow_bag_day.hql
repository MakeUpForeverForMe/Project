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



set hivevar:ST9=2020-10-15;

-- insert overwrite table dm_eagle.abs_asset_information_cash_flow_bag_day partition(bag_id)
select
  t1.project_id                                                                                                       as project_id,             --'项目编号'
  t1.bag_date                                                                                                         as bag_date,               --'封包日期'
  max(t4.biz_date) over(partition by t1.project_id,t1.bag_date,t1.bag_id order by t1.biz_date)                        as data_extraction_day,    --'最新数据提取日'

  nvl(t2.should_repay_amount   ,0)                                                                                    as should_repay_amount,    --'应收金额'
  nvl(t2.should_repay_principal,0)                                                                                    as should_repay_principal, --'应收本金'
  nvl(t2.should_repay_interest ,0)                                                                                    as should_repay_interest,  --'应收利息'
  nvl(t2.should_repay_cost     ,0)                                                                                    as should_repay_cost,      --'应收费用'

  nvl(t3.paid_amount           ,0)                                                                                    as paid_amount,            --'实收金额'
  nvl(t3.paid_principal        ,0)                                                                                    as paid_principal,         --'实收本金'
  nvl(t3.paid_interest         ,0)                                                                                    as paid_interest,          --'实收利息'
  nvl(t3.paid_cost             ,0)                                                                                    as paid_cost,              --'实收费用'

  nvl(t3.overdue_paid_amount   ,0)                                                                                    as overdue_paid_amount,    --'逾期还款金额'
  nvl(t3.overdue_paid_principal,0)                                                                                    as overdue_paid_principal, --'逾期还款本金'
  nvl(t3.overdue_paid_interest ,0)                                                                                    as overdue_paid_interest,  --'逾期还款利息'
  nvl(t3.overdue_paid_cost     ,0)                                                                                    as overdue_paid_cost,      --'逾期还款费用'

  nvl(t3.prepayment_amount     ,0)                                                                                    as prepayment_amount,      --'提前还款金额'
  nvl(t3.prepayment_principal  ,0)                                                                                    as prepayment_principal,   --'提前还款本金'
  nvl(t3.prepayment_interest   ,0)                                                                                    as prepayment_interest,    --'提前还款利息'
  nvl(t3.prepayment_cost       ,0)                                                                                    as prepayment_cost,        --'提前还款费用'

  nvl(t3.normal_paid_amount    ,0)                                                                                    as normal_paid_amount,     --'正常还款金额'
  nvl(t3.normal_paid_principal ,0)                                                                                    as normal_paid_principal,  --'正常还款本金'
  nvl(t3.normal_paid_interest  ,0)                                                                                    as normal_paid_interest,   --'正常还款利息'
  nvl(t3.normal_paid_cost      ,0)                                                                                    as normal_paid_cost,       --'正常还款费用'

  t1.biz_date                                                                                                         as biz_date,               --'观察日期（应还日/实还日）'
  t1.bag_id                                                                                                           as bag_id                  --包编号
from (
  select
    t6.project_id,
    t6.bag_id,
    t6.bag_date,
    cast(t3.biz_date as string) as biz_date
  from (
    select
      date_add(s_d_date,pos) as biz_date,
      '1' as rn
    from (
      select
        t1.s_d_date,
        t2.e_d_date
      from (
        select
          min(bag_date) as s_d_date,
          '1' as rn
        from dim.bag_info
      ) as t1
      join (
        select
          max(should_repay_date) as e_d_date,
          '1' as rn
        from ods.repay_schedule_abs
        where '${ST9}' between s_d_date and date_sub(e_d_date,1)
      ) as t2
      on t1.rn = t2.rn
    ) as t
    lateral view posexplode(split(space(datediff(e_d_date,s_d_date)),' ')) tf as pos,val
  ) as t3
  join (
    select
      t6.bag_id,
      t6.project_id,
      t6.bag_date,
      max(to_date(t4.should_repay_date)) as should_repay_date_max,
      '1' as rn
    from (
      select
        due_bill_no,
        should_repay_date,
        paid_out_date
      from ods.repay_schedule_abs
      where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    ) as t4
    join dim.bag_due_bill_no as t5
    on t4.due_bill_no = t5.due_bill_no
    join dim.bag_info as t6
    on t5.bag_id = t6.bag_id
    group by t6.bag_id,t6.project_id,t6.bag_date
  ) as t6
  on t3.rn = t6.rn
  where t6.bag_date < t3.biz_date
    and t6.should_repay_date_max >= t3.biz_date
) as t1
left join (
  select
    t.project_id,
    t1.bag_id,
    t.should_repay_date,
    sum(nvl(t.should_repay_amount,0))    as should_repay_amount,
    sum(nvl(t.should_repay_principal,0)) as should_repay_principal,
    sum(nvl(t.should_repay_interest,0))  as should_repay_interest,
    sum(nvl(t.should_repay_term_fee,0) + nvl(t.should_repay_svc_fee,0) + nvl(t.should_repay_penalty,0) + nvl(t.should_repay_mult_amt,0)) as should_repay_cost
  from ods.repay_schedule_abs as t
  join dim.bag_due_bill_no as t1
  on t.due_bill_no = t1.due_bill_no
  where '${ST9}' between s_d_date and date_sub(e_d_date,1)
    and if(paid_out_type_cn is NULL,'A',paid_out_type_cn) != '提前结清'
  group by t.project_id,t1.bag_id,t.should_repay_date
) as t2
on  t1.project_id = t2.project_id
and t1.bag_id     = t2.bag_id
and t1.biz_date   = t2.should_repay_date
left join (
  select
    t.project_id                                                     as project_id,
    t1.bag_id                                                        as bag_id,
    t2.biz_date                                                      as biz_date,
    sum(nvl(t2.paid_amount    ,0))                                   as paid_amount,
    sum(nvl(t2.paid_principal ,0))                                   as paid_principal,
    sum(nvl(t2.paid_interest  ,0))                                   as paid_interest,
    sum(nvl(t2.paid_cost,0))                                         as paid_cost,

    sum(if(paid_out_type_cn='逾期结清',nvl(t2.paid_amount    ,0),0)) as overdue_paid_amount,
    sum(if(paid_out_type_cn='逾期结清',nvl(t2.paid_principal ,0),0)) as overdue_paid_principal,
    sum(if(paid_out_type_cn='逾期结清',nvl(t2.paid_interest  ,0),0)) as overdue_paid_interest,
    sum(if(paid_out_type_cn='逾期结清',nvl(t2.paid_cost      ,0),0)) as overdue_paid_cost,

    sum(if(paid_out_type_cn='提前结清',nvl(t2.paid_amount    ,0),0)) as prepayment_amount,
    sum(if(paid_out_type_cn='提前结清',nvl(t2.paid_principal ,0),0)) as prepayment_principal,
    sum(if(paid_out_type_cn='提前结清',nvl(t2.paid_interest  ,0),0)) as prepayment_interest,
    sum(if(paid_out_type_cn='提前结清',nvl(t2.paid_cost      ,0),0)) as prepayment_cost,

    sum(if(paid_out_type_cn='正常结清',nvl(t2.paid_amount    ,0),0)) as normal_paid_amount,
    sum(if(paid_out_type_cn='正常结清',nvl(t2.paid_principal ,0),0)) as normal_paid_principal,
    sum(if(paid_out_type_cn='正常结清',nvl(t2.paid_interest  ,0),0)) as normal_paid_interest,
    sum(if(paid_out_type_cn='正常结清',nvl(t2.paid_cost      ,0),0)) as normal_paid_cost
  from (
    select
      due_bill_no,
      loan_term,
      paid_out_type_cn,
      project_id
    from ods.repay_schedule_abs
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
  ) as t
  join dim.bag_due_bill_no as t1
  on t.due_bill_no = t1.due_bill_no
  join (
    select
      due_bill_no                                                                       as due_bill_no,
      repay_term                                                                        as repay_term,
      max(biz_date)                                                                     as biz_date,
      sum(nvl(repay_amount,0))                                                          as paid_amount,
      sum(nvl(if(bnp_type = 'Pricinpal',repay_amount,0),0))                             as paid_principal,
      sum(nvl(if(bnp_type = 'Interest',repay_amount,0),0))                              as paid_interest,
      sum(nvl(if(bnp_type != 'Pricinpal' and bnp_type != 'Interest',repay_amount,0),0)) as paid_cost
    from ods.repay_detail_abs
    group by due_bill_no,repay_term
  ) as t2
  on  t.due_bill_no = t2.due_bill_no
  and t.loan_term = t2.repay_term
  group by t.project_id,t1.bag_id,t2.biz_date
) as t3
on  t1.project_id = t3.project_id
and t1.bag_id     = t3.bag_id
and t1.biz_date   = t3.biz_date
left join (
  select distinct
    t3.bag_id,
    t3.project_id,
    t1.biz_date
  from ods.repay_detail_abs as t1
  join dim.bag_due_bill_no as t2
  on t1.due_bill_no = t2.due_bill_no
  join dim.bag_info as t3
  on t2.bag_id = t3.bag_id
) as t4
on  t1.project_id = t4.project_id
and t1.bag_id     = t4.bag_id
and t1.biz_date   = t4.biz_date
limit 10
;
