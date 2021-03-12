====================================================================================================================================================================================================================
-- ods 与 ods_new_s 数据对比
====================================================================================================================================================================================================================



-- ods 与 ods_new_s 借据 借据数、放款金额 对比




set var:ST9=2020-10-02;

-- set var:db_suffix=;set var:tb_suffix=_asset;

set var:db_suffix=_cps;set var:tb_suffix=;

invalidate metadata ods.ecas_loan${var:tb_suffix};
invalidate metadata ods_new_s${var:db_suffix}.loan_lending;
-- ods 借据 与 ods_new_s 放款表 对比 借据数、放款金额
select
  'ods 借据 与 ods_new_s 放款表 对比 借据数、放款金额'        as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')               as cps,
  nvl(ods.loan_active_date,new.loan_active_date)              as active_date,
  nvl(ods.due_bill_no,new.due_bill_no)                        as due_bill_no,
  nvl(ods.loan_num,0)                                         as num_ods,
  nvl(new.loan_num,0)                                         as num_new,
  nvl(ods.loan_num,0) - nvl(new.loan_num,0)                   as num_diff,
  nvl(ods.loan_num_distinct,0)                                as num_ods_distinct,
  nvl(new.loan_num_distinct,0)                                as num_new_distinct,
  nvl(ods.loan_num_distinct,0) - nvl(new.loan_num_distinct,0) as num_distinct_diff,
  nvl(ods.loan_principal,0)                                   as principal_ods,
  nvl(new.loan_principal,0)                                   as principal_new,
  nvl(ods.loan_principal,0) - nvl(new.loan_principal,0)       as principal_diff,
  nvl(ods.product_id,new.product_id)                          as product_id
from (
  select
    due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_prin)         as loan_principal,
    active_date                 as loan_active_date,
    product_code                as product_id
  from ods.ecas_loan${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and product_code in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by loan_active_date,product_id
  ,due_bill_no
) as ods
full join (
  select
    due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_principal)    as loan_principal,
    loan_active_date            as loan_active_date,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.loan_lending
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and biz_date <= '${var:ST9}'
  group by loan_active_date,product_id
  ,due_bill_no
) as new
on  ods.product_id       = new.product_id
and ods.loan_active_date = new.loan_active_date
and ods.due_bill_no      = new.due_bill_no
having nvl(ods.loan_num,0) - nvl(new.loan_num,0) != 0 or nvl(ods.loan_principal,0) - nvl(new.loan_principal,0) != 0 or nvl(ods.loan_num_distinct,0) - nvl(new.loan_num_distinct,0) != 0
order by active_date,product_id
limit 10
;








set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-10-02;

invalidate metadata ods.ecas_loan${var:tb_suffix};
invalidate metadata ods_new_s${var:db_suffix}.loan_info;
-- ods 借据 与 ods_new_s 借据表现 对比 借据数、放款金额
select
  'ods 借据 与 ods_new_s 借据表现 对比 借据数、放款金额'      as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')               as cps,
  nvl(ods.loan_active_date,new.loan_active_date)              as active_date,
  nvl(ods.due_bill_no,new.due_bill_no)                        as due_bill_no,
  nvl(ods.loan_num,0)                                         as num_ods,
  nvl(new.loan_num,0)                                         as num_new,
  nvl(ods.loan_num,0) - nvl(new.loan_num,0)                   as num_diff,
  nvl(ods.loan_num_distinct,0)                                as num_ods_distinct,
  nvl(new.loan_num_distinct,0)                                as num_new_distinct,
  nvl(ods.loan_num_distinct,0) - nvl(new.loan_num_distinct,0) as num_distinct_diff,
  nvl(ods.loan_principal,0)                                   as principal_ods,
  nvl(new.loan_principal,0)                                   as principal_new,
  nvl(ods.loan_principal,0) - nvl(new.loan_principal,0)       as principal_diff,
  nvl(ods.loan_active_date,new.loan_active_date)              as active_date,
  nvl(ods.product_id,new.product_id)                          as product_id
from (
  select
    due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_prin)         as loan_principal,
    active_date                 as loan_active_date,
    product_code                as product_id
  from ods.ecas_loan${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and product_code in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by loan_active_date,product_id
  ,due_bill_no
) as ods
full join (
  select
    due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_principal)    as loan_principal,
    loan_active_date            as loan_active_date,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.loan_info
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by loan_active_date,product_id
  ,due_bill_no
) as new
on  ods.product_id       = new.product_id
and ods.loan_active_date = new.loan_active_date
and ods.due_bill_no      = new.due_bill_no
having nvl(ods.loan_num,0) - nvl(new.loan_num,0) != 0 or nvl(ods.loan_principal,0) - nvl(new.loan_principal,0) != 0 or nvl(ods.loan_num_distinct,0) - nvl(new.loan_num_distinct,0) != 0
order by active_date,product_id
limit 10
;




====================================================================================================================================================================================================================



-- set var:db_suffix=;set var:tb_suffix=_asset;

set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-09-13;

invalidate metadata ods.ecas_repay_schedule${var:tb_suffix};
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule_rerun;
-- ods 应还 与 ods_new_s 应还 对比 借据数、应还金额
select
  'ods 应还 与 ods_new_s 应还 对比 借据数、应还金额'        as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')             as cps,
  nvl(ods.should_repay_date,new.should_repay_date)          as should_date,
  nvl(ods.due_bill_no,new.due_bill_no)                      as due_bill_no,
  nvl(ods.should_principal,0)                               as should_principal_ods,
  nvl(new.should_principal,0)                               as should_principal_new,
  nvl(ods.should_principal,0) - nvl(new.should_principal,0) as should_principal_diff,
  nvl(ods.product_id,new.product_id)                        as product_id
from (
  select
    due_bill_no   as due_bill_no,
    due_term_prin as should_principal,
    pmt_due_date  as should_repay_date,
    product_code  as product_id
  from ods.ecas_repay_schedule${var:tb_suffix}
  where 1 > 0
    and d_date = '${var:ST9}'
    and product_code in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
) as ods
full join (
  select
    due_bill_no            as due_bill_no,
    should_repay_principal as should_principal,
    should_repay_date      as should_repay_date,
    product_id             as product_id
  from ods_new_s${var:db_suffix}.repay_schedule
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
) as new
on  ods.product_id        = new.product_id
and ods.should_repay_date = new.should_repay_date
and ods.due_bill_no       = new.due_bill_no
having nvl(ods.should_principal,0) - nvl(new.should_principal,0) != 0
order by should_date,product_id
limit 10
;


====================================================================================================================================================================================================================



set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-09-30;

invalidate metadata ods.ecas_loan${var:tb_suffix};
invalidate metadata ods.ecas_repay_hst${var:tb_suffix};
invalidate metadata ods_new_s${var:db_suffix}.repay_detail;
-- ods 实还 与 ods_new_s 实还 对比 借据数、实还金额   以 交易日期 分组
select
  'ods 实还 与 ods_new_s 实还 对比 借据数、实还金额   以 交易日期 分组' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                   as cps,
  nvl(ods.txn_date,new.txn_date)                                  as txn_date,
  nvl(ods.product_id,new.product_id)                              as product_id,
  nvl(ods.due_bill_no,new.due_bill_no)                            as due_bill_no,
  nvl(ods.bnp_type,new.bnp_type)                                  as bnp_type,
  nvl(ods.repaid_num,0)                                           as repy_num_ods,
  nvl(new.repaid_num,0)                                           as repy_num_new,
  nvl(ods.repaid_num,0) - nvl(new.repaid_num,0)                   as repy_num_his,
  nvl(ods.repaid_num_distinct,0)                                  as repy_num_ods_dis,
  nvl(new.repaid_num_distinct,0)                                  as repy_num_new_dis,
  nvl(ods.repaid_num_distinct,0) - nvl(new.repaid_num_distinct,0) as repy_num_his_dis,
  nvl(ods.repaid_principal,0)                                     as repy_principal_ods,
  nvl(new.repaid_principal,0)                                     as repy_principal_new,
  nvl(ods.repaid_principal,0) - nvl(new.repaid_principal,0)       as repy_principal
from (
  select
    repay.due_bill_no,
    count(distinct repay.due_bill_no) as repaid_num,
    count(repay.due_bill_no) as repaid_num_distinct,
    sum(repay_amt)
    - case when ecas_loan.product_id = '001802' and repay.d_date between '2020-07-10' and '2020-07-13' and repay.due_bill_no = '1120070912093993172613' then if('${var:tb_suffix}' = '_asset',2500,0) else 0 end
    as repaid_principal,
    bnp_type,
    repay.txn_date as txn_date,
    repay.d_date as d_date,
    ecas_loan.product_id
  from (
    select
      due_bill_no,
      term,
      repay_amt,
      bnp_type,
      txn_date,
      d_date
    from ods.ecas_repay_hst${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and txn_date <= d_date
      and if('${var:tb_suffix}' = '_asset',true,
        payment_id not in ( -- 代偿后  在 2020-07-10 到 2020-07-13 这四天有六笔，最新分区只有一笔。
          '000015945007161admin000083000002',
          '000015945007161admin000083000003',
          '000015945007161admin000083000004',
          '000015945007161admin000083000005',
          '000015945007161admin000083000006'
        )
      )
  ) as repay
  join (
    select product_code as product_id,due_bill_no
    from ods.ecas_loan${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and p_type in ('lx','lxzt','lx2')
  ) as ecas_loan
  on repay.due_bill_no = ecas_loan.due_bill_no
  group by repay.d_date,repay.txn_date,ecas_loan.product_id,bnp_type
  ,repay.due_bill_no
) as ods
full join (
  select
    due_bill_no,
    count(distinct due_bill_no) as repaid_num,
    count(due_bill_no) as repaid_num_distinct,
    sum(repay_amount) as repaid_principal,
    bnp_type,
    to_date(txn_time) as txn_date,
    product_id
  from ods_new_s${var:db_suffix}.repay_detail
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and to_date(txn_time) <= biz_date
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id,to_date(txn_time),bnp_type
  ,due_bill_no
) as new
on  ods.product_id  = new.product_id
and ods.txn_date    = new.txn_date
and ods.bnp_type    = new.bnp_type
and ods.due_bill_no = new.due_bill_no
having nvl(ods.repaid_num,0) - nvl(new.repaid_num,0) != 0 or nvl(ods.repaid_principal,0) - nvl(new.repaid_principal,0) != 0
order by txn_date,product_id,
due_bill_no
-- limit 10
;






set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-10-17;

invalidate metadata ods.ecas_loan${var:tb_suffix};
invalidate metadata ods.ecas_repay_hst${var:tb_suffix};
invalidate metadata ods_new_s${var:db_suffix}.repay_detail;

-- ods 实还 与 ods_new_s 实还 对比 借据数、实还金额   以 快照日期 分组
select
  'ods 实还 与 ods_new_s 实还 对比 借据数、实还金额   以 快照日期 分组' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')             as cps,
  nvl(ods.txn_date,new.txn_date)                            as txn_date,
  nvl(ods.due_bill_no,new.due_bill_no)                      as due_bill_no,
  nvl(ods.repaid_num,0)                                     as repy_num_ods,
  nvl(new.repaid_num,0)                                     as repy_num_new,
  nvl(ods.repaid_num,0) - nvl(new.repaid_num,0)             as repy_num_his,
  nvl(ods.repaid_principal,0)                               as repy_principal_ods,
  nvl(new.repaid_principal,0)                               as repy_principal_new,
  nvl(ods.repaid_principal,0) - nvl(new.repaid_principal,0) as repy_principal,
  nvl(ods.product_id,new.product_id)                        as product_id
from (
  select
    repay.due_bill_no                 as due_bill_no,
    count(repay.due_bill_no)          as repaid_num,
    count(distinct repay.due_bill_no) as repaid_num_distinct,
    sum(repay_amt)
    - case when ecas_loan.product_id = '001802' and repay.d_date = '2020-07-10' and repay.due_bill_no = '1120070912093993172613' then if('${var:tb_suffix}' = '_asset',2500,0) else 0 end
                                      as repaid_principal,
    repay.d_date                      as txn_date,
    ecas_loan.product_id              as product_id
  from (
    select
      due_bill_no,
      term,
      repay_amt,
      d_date
    from ods.ecas_repay_hst${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and if('${var:tb_suffix}' = '_asset',
        payment_id not in ( -- 代偿前
          '000015943861031admin000083000003'
        ),
        payment_id not in ( -- 代偿后
          '000015943861031admin000083000002',
          '000015945007161admin000083000002',
          '000015945007161admin000083000003',
          '000015945007161admin000083000004',
          '000015945007161admin000083000005',
          '000015945007161admin000083000006'
        )
      )
  ) as repay
  join (
    select due_bill_no,term,min(d_date) as d_date
    from ods.ecas_repay_hst${var:tb_suffix}
    where 1 > 0
      and d_date <= '${var:ST9}'
    group by due_bill_no,term
  ) as repay_min_d_date
  on  repay.due_bill_no = repay_min_d_date.due_bill_no
  and repay.term        = repay_min_d_date.term
  and repay.d_date      = repay_min_d_date.d_date
  join (
    select product_code as product_id,due_bill_no
    from ods.ecas_loan${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and p_type in ('lx','lxzt','lx2')
  ) as ecas_loan
  on repay.due_bill_no = ecas_loan.due_bill_no
  group by repay.d_date,ecas_loan.product_id
  ,repay.due_bill_no
) as ods
full join (
  select
    due_bill_no                 as due_bill_no,
    count(due_bill_no)          as repaid_num,
    count(distinct due_bill_no) as repaid_num_distinct,
    sum(repay_amount)           as repaid_principal,
    biz_date                    as txn_date,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.repay_detail
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id,biz_date
  ,due_bill_no
) as new
on  ods.product_id  = new.product_id
and ods.txn_date    = new.txn_date
and ods.due_bill_no = new.due_bill_no
having nvl(ods.repaid_num,0) - nvl(new.repaid_num,0) != 0 or nvl(ods.repaid_principal,0) - nvl(new.repaid_principal,0) != 0
order by txn_date,product_id
limit 10
;



====================================================================================================================================================================================================================


set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-06-10;

invalidate metadata ods.ecas_order${var:tb_suffix};
invalidate metadata ods_new_s${var:db_suffix}.order_info;
-- ods 流水 与 ods_new_s 流水 对比 借据数、实还金额
select
  'ods 流水 与 ods_new_s 流水 对比 借据数、实还金额' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后') as cps,
  nvl(ods.biz_date,new.biz_date)                as biz_date,
  nvl(ods.due_bill_no,new.due_bill_no)          as due_bill_no,
  nvl(ods.txn_amt,0)                            as txn_amt_ods,
  nvl(new.txn_amt,0)                            as txn_amt_new,
  nvl(ods.txn_amt,0) - nvl(new.txn_amt,0)       as txn_amt_diff,
  nvl(ods.product_id,new.product_id)            as product_id
from (
  select
    count(distinct due_bill_no)                                       as due_bill_no,
    sum(txn_amt)                                                      as txn_amt,
    nvl(txn_date,datefmt(cast(txn_time as string),'ms','yyyy-MM-dd')) as biz_date,
    product_code                                                      as product_id
  from (
    select *
    from ods.ecas_order${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
  ) as order
  join (
    select product_code as product_id,due_bill_no
    from ods.ecas_loan${var:tb_suffix}
    where 1 > 0
      and d_date = '${var:ST9}'
      and p_type in ('lx','lxzt','lx2')
  ) as ecas_loan
  on order.due_bill_no = ecas_loan.due_bill_no
) as ods
full join (
  select
    count(distinct due_bill_no) as due_bill_no,
    sum(txn_amt)                as txn_amt,
    biz_date                    as biz_date,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.order_info
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
) as new
on  ods.product_id  = new.product_id
and ods.biz_date    = new.biz_date
and ods.due_bill_no = new.due_bill_no
having nvl(ods.should_principal,0) - nvl(new.should_principal,0) != 0
order by biz_date,product_id
limit 10
;
















====================================================================================================================================================================================================================
-- ods_new_s 各表间交叉验证
====================================================================================================================================================================================================================










set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-10-17;

invalidate metadata ods_new_s${var:db_suffix}.loan_lending;
invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
-- ods_new_s  放款、借据、应还 借据数、应还金额 对比
select
  'ods_new_s  放款、借据、应还 借据数、应还金额 对比'                                                as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                                                      as cps,
  coalesce(loan_lending.loan_active_date,loan_info.loan_active_date,repay_schedule.loan_active_date) as active_date,
  coalesce(loan_lending.due_bill_no,loan_info.due_bill_no,repay_schedule.due_bill_no)                as due_bill_no,
  nvl(loan_lending.loan_num,0)                                                                       as num_lengding,
  nvl(loan_info.loan_num,0)                                                                          as num_loan,
  nvl(loan_lending.loan_num,0) - nvl(loan_info.loan_num,0)                                           as num_diff1,
  nvl(repay_schedule.loan_num,0)                                                                     as num_schedule,
  nvl(loan_lending.loan_num,0) - nvl(repay_schedule.loan_num,0)                                      as num_diff2,
  nvl(loan_lending.loan_num_distinct,0)                                                              as num_dis_lengding,
  nvl(loan_info.loan_num_distinct,0)                                                                 as num_dis_loan,
  nvl(loan_lending.loan_num_distinct,0) - nvl(loan_info.loan_num_distinct,0)                         as num_dis_diff1,
  nvl(repay_schedule.loan_num_distinct,0)                                                            as num_dis_schedule,
  nvl(loan_lending.loan_num_distinct,0) - nvl(repay_schedule.loan_num_distinct,0)                    as num_dis_diff2,
  nvl(loan_lending.loan_principal,0)                                                                 as prin_lengding,
  nvl(loan_info.loan_principal,0)                                                                    as prin_loan,
  nvl(loan_lending.loan_principal,0) - nvl(loan_info.loan_principal,0)                               as prin_diff1,
  nvl(repay_schedule.loan_principal,0)                                                               as prin_schedule,
  nvl(loan_lending.loan_principal,0) - nvl(repay_schedule.loan_principal,0)                          as prin_diff2,
  coalesce(loan_lending.product_id,loan_info.product_id,repay_schedule.product_id)                   as product_id
from (
  select
    due_bill_no                 as due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_principal)    as loan_principal,
    loan_active_date            as loan_active_date,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.loan_lending
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by loan_active_date,product_id
  ,due_bill_no
) as loan_lending
full join (
  select
    due_bill_no                 as due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_principal)    as loan_principal,
    loan_active_date            as loan_active_date,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.loan_info
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by loan_active_date,product_id
  ,due_bill_no
) as loan_info
on  loan_lending.product_id       = loan_info.product_id
and loan_lending.loan_active_date = loan_info.loan_active_date
and loan_lending.due_bill_no      = loan_info.due_bill_no
full join(
  select
    due_bill_no                 as due_bill_no,
    count(due_bill_no)          as loan_num,
    count(distinct due_bill_no) as loan_num_distinct,
    sum(loan_init_principal)    as loan_principal,
    loan_active_date            as loan_active_date,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.repay_schedule
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and loan_term = 1 -- 所有的借据都有第一期，取第一期是取还款计划中每笔借据只取一条数据
  group by loan_active_date,product_id
  ,due_bill_no
) as repay_schedule
on  loan_lending.product_id       = repay_schedule.product_id
and loan_lending.loan_active_date = repay_schedule.loan_active_date
and loan_lending.due_bill_no      = repay_schedule.due_bill_no
having
nvl(loan_lending.loan_num,0)          - nvl(loan_info.loan_num,0)               != 0 or
nvl(loan_lending.loan_num,0)          - nvl(repay_schedule.loan_num,0)          != 0 or
nvl(loan_lending.loan_num_distinct,0) - nvl(loan_info.loan_num_distinct,0)      != 0 or
nvl(loan_lending.loan_num_distinct,0) - nvl(repay_schedule.loan_num_distinct,0) != 0 or
nvl(loan_lending.loan_principal,0)    - nvl(loan_info.loan_principal,0)         != 0 or
nvl(loan_lending.loan_principal,0)    - nvl(repay_schedule.loan_principal,0)    != 0
order by active_date,product_id
limit 10
;



====================================================================================================================================================================================================================



set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-06-10;

invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata ods_new_s${var:db_suffix}.repay_detail;
-- ods_new_s  应还、实还 借据数、已还金额 对比
select
  'ods_new_s  应还、实还 借据数、已还金额 对比'                                       as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                                       as cps,
  '${var:ST9}'                                                                        as biz_date,
  nvl(repay_schedule.due_bill_no,repay_detail.due_bill_no)                            as due_bill_no,
  nvl(repay_schedule.repaid_num,0)                                                    as repay_num_schedule,
  nvl(repay_detail.repaid_num,0)                                                      as repay_num_detail,
  nvl(repay_schedule.repaid_num,0) - nvl(repay_detail.repaid_num,0)                   as repay_num,
  nvl(repay_schedule.repaid_principal,0)                                              as repay_principal_schedule,
  nvl(repay_detail.repaid_principal,0)                                                as repay_principal_detail,
  nvl(repay_schedule.repaid_principal,0) - nvl(repay_detail.repaid_principal,0)       as repay_principal,
  nvl(repay_schedule.product_id,repay_detail.product_id)                              as product_id
from (
  select
    due_bill_no                 as due_bill_no,
    count(distinct due_bill_no) as repaid_num,
    sum(paid_principal)         as repaid_principal,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.repay_schedule
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and paid_principal != 0
  group by product_id
  ,due_bill_no
) as repay_schedule
full join (
  select
    due_bill_no                 as due_bill_no,
    count(distinct due_bill_no) as repaid_num,
    sum(repay_amount)           as repaid_principal,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.repay_detail
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and to_date(txn_time) <= biz_date
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and bnp_type = 'Pricinpal'
  group by product_id
  ,due_bill_no
) as repay_detail
on  repay_schedule.product_id  = repay_detail.product_id
and repay_schedule.due_bill_no = repay_detail.due_bill_no
having
nvl(repay_schedule.repaid_num,0)       - nvl(repay_detail.repaid_num,0)       != 0 or
nvl(repay_schedule.repaid_principal,0) - nvl(repay_detail.repaid_principal,0) != 0
order by product_id,due_bill_no
-- limit 10
;


====================================================================================================================================================================================================================




set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-09-30;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_detail;
-- ods_new_s  借据、实还 借据数、已还金额 对比
select
  'ods_new_s  借据、实还 借据数、已还金额 对比'                            as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                            as cps,
  '${var:ST9}'                                                             as biz_date,
  nvl(loan_info.due_bill_no,repay_detail.due_bill_no)                      as due_bill_no,
  nvl(loan_info.repaid_num,0)                                              as repay_num_loan,
  nvl(repay_detail.repaid_num,0)                                           as repay_num_detail,
  nvl(loan_info.repaid_num,0) - nvl(repay_detail.repaid_num,0)             as repay_num,
  nvl(loan_info.repaid_principal,0)                                        as repay_principal_loan,
  nvl(repay_detail.repaid_principal,0)                                     as repay_principal_detail,
  nvl(loan_info.repaid_principal,0) - nvl(repay_detail.repaid_principal,0) as repay_principal,
  nvl(loan_info.product_id,repay_detail.product_id)                        as product_id
from (
  select
    due_bill_no                 as due_bill_no,
    count(distinct due_bill_no) as repaid_num,
    sum(paid_principal)         as repaid_principal,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.loan_info
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and paid_principal != 0
  group by product_id
  ,due_bill_no
) as loan_info
full join (
  select
    due_bill_no                 as due_bill_no,
    count(distinct due_bill_no) as repaid_num,
    sum(repay_amount)           as repaid_principal,
    product_id                  as product_id
  from ods_new_s${var:db_suffix}.repay_detail
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and to_date(txn_time) <= biz_date
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and bnp_type = 'Pricinpal'
  group by product_id
  ,due_bill_no
) as repay_detail
on  loan_info.product_id  = repay_detail.product_id
and loan_info.due_bill_no = repay_detail.due_bill_no
having
nvl(loan_info.repaid_num,0)       - nvl(repay_detail.repaid_num,0)       != 0 or
nvl(loan_info.repaid_principal,0) - nvl(repay_detail.repaid_principal,0) != 0
order by product_id,due_bill_no
limit 10
;



====================================================================================================================================================================================================================




set var:db_suffix=;set var:tb_suffix=_asset;

-- set var:db_suffix=_cps;set var:tb_suffix=;

set var:ST9=2020-10-26;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
-- ods_new_s  借据、应还 本金余额 = 逾期金额 + 未出账本金
select
  'ods_new_s  借据、应还 本金余额 = 逾期金额 + 未出账本金'                                                            as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                                                                       as cps,
  '${var:ST9}'                                                                                                        as biz_date,
  nvl(loan_info.product_id,repay_schedule.product_id)                                                                 as product_id,
  nvl(loan_info.due_bill_no,repay_schedule.due_bill_no)                                                               as due_bill_no,
  nvl(loan_info.remain_principal,0)                                                                                   as remain_principal,
  nvl(loan_info.overdue_principal,0) + nvl(repay_schedule.unposted_principal,0)                                       as remain_principal_compute,
  nvl(loan_info.overdue_principal,0)                                                                                  as overdue_principal,
  nvl(repay_schedule.unposted_principal,0)                                                                            as unposted_principal,
  nvl(loan_info.remain_principal,0) - (nvl(loan_info.overdue_principal,0) + nvl(repay_schedule.unposted_principal,0)) as remain_principal_diff
from (
  select
    product_id             as product_id,
    due_bill_no            as due_bill_no,
    sum(remain_principal)  as remain_principal,
    sum(overdue_principal) as overdue_principal
  from ods_new_s${var:db_suffix}.loan_info
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and due_bill_no != '1120061910384241252747'
  group by product_id,due_bill_no
) as loan_info
full join (
  select
    product_id                                   as product_id,
    due_bill_no                                  as due_bill_no,
    sum(should_repay_principal - paid_principal) as unposted_principal
  from ods_new_s${var:db_suffix}.repay_schedule
  where 1 > 0
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and schedule_status = 'N'
    and due_bill_no != '1120061910384241252747'
  group by product_id,due_bill_no
) as repay_schedule
on  loan_info.product_id  = repay_schedule.product_id
and loan_info.due_bill_no = repay_schedule.due_bill_no
where nvl(loan_info.remain_principal,0) - (nvl(loan_info.overdue_principal,0) + nvl(repay_schedule.unposted_principal,0)) != 0
order by product_id,due_bill_no
limit 10
;
