====================================================================================================================================================================================================================
-- ods_new_s 与 dw_new 数据对比
====================================================================================================================================================================================================================


set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-27;

invalidate metadata ods_new_s${var:db_suffix}.loan_lending;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_loan_num_day;

-- ods_new_s 放款 与 dw_new 放款 对比 借据数、放款金额 （每天对比全量）
select
  'ods_new_s 放款 与 dw_new 放款 对比 借据数、放款金额' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  nvl(dws.loan_active_date,new.loan_active_date)        as active_date,
  nvl(new.loan_num,0)                                   as num_new,
  nvl(dws.loan_num,0)                                   as num_dw,
  nvl(new.loan_num,0) - nvl(dws.loan_num,0)             as num_diff2,
  nvl(new.loan_principal,0)                             as principal_new,
  nvl(dws.loan_principal,0)                             as principal_dw,
  nvl(new.loan_principal,0) - nvl(dws.loan_principal,0) as principal_diff2,
  nvl(dws.product_id,new.product_id)                    as product_id
from (
  select
    count(due_bill_no)       as loan_num,
    sum(loan_init_principal) as loan_principal,
    biz_date                 as loan_active_date,
    product_id               as product_id
  from ods_new_s${var:db_suffix}.loan_lending
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by loan_active_date,product_id
) as new
full join (
  select
    sum(loan_num)       as loan_num,
    sum(loan_principal) as loan_principal,
    biz_date            as loan_active_date,
    product_id
  from dw_new${var:db_suffix}.dw_loan_base_stat_loan_num_day
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by biz_date,product_id
) as dws
on  new.product_id       = dws.product_id
and new.loan_active_date = dws.loan_active_date
where nvl(new.loan_num,0) - nvl(dws.loan_num,0) != 0 or nvl(new.loan_principal,0) - nvl(dws.loan_principal,0) != 0
order by active_date,product_id
limit 10
;



set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-27;

invalidate metadata ods_new_s${var:db_suffix}.loan_lending;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_loan_num_day;

-- ods_new_s 放款 与 dw_new 放款 对比 累计借据数、累计放款金额
select
  'ods_new_s 放款 与 dw_new 放款 对比 累计借据数、累计放款金额' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  '${var:ST9}'                                          as biz_date,
  nvl(new.loan_num,0)                                   as num_new,
  nvl(dws.loan_num,0)                                   as num_dw,
  nvl(new.loan_num,0) - nvl(dws.loan_num,0)             as num_diff2,
  nvl(new.loan_principal,0)                             as principal_new,
  nvl(dws.loan_principal,0)                             as principal_dw,
  nvl(new.loan_principal,0) - nvl(dws.loan_principal,0) as principal_diff2,
  nvl(dws.product_id,new.product_id)                    as product_id
from (
  select
    count(due_bill_no)       as loan_num,
    sum(loan_init_principal) as loan_principal,
    product_id               as product_id
  from ods_new_s${var:db_suffix}.loan_lending
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id
) as new
full join (
  select
    sum(loan_num_count)       as loan_num,
    sum(loan_principal_count) as loan_principal,
    product_id
  from dw_new${var:db_suffix}.dw_loan_base_stat_loan_num_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id
) as dws
on new.product_id = dws.product_id
where nvl(new.loan_num,0) - nvl(dws.loan_num,0) != 0 or nvl(new.loan_principal,0) - nvl(dws.loan_principal,0) != 0
order by product_id
limit 10
;


====================================================================================================================================================================================================================


set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-27;

invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_should_repay_day;
-- ods_new_s 应还 与 dw_new 应还 对比 放款金额
select
  'ods_new_s 应还 与 dw_new 应还 对比 放款金额'         as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  '${var:ST9}'                                          as biz_date,
  nvl(dws.loan_active_date,new.loan_active_date)        as active_date,
  nvl(dws.product_id,new.product_id)                    as product_id,
  nvl(dws.should_repay_date,new.should_repay_date)      as should_repay_date,
  nvl(new.loan_principal,0)                             as principal_new,
  nvl(dws.loan_principal,0)                             as principal_dw,
  nvl(new.loan_principal,0) - nvl(dws.loan_principal,0) as principal_diff
from (
  select
    product_id,
    loan_active_date,
    should_repay_date,
    sum(loan_init_principal) as loan_principal
  from (
    select distinct
      product_id,
      due_bill_no,
      loan_active_date,
      should_repay_date,
      loan_init_principal
    from ods_new_s${var:db_suffix}.repay_schedule -- 必须经过去重。原因：滴滴多期的应还日可能会变成同一天，这样的放款金额就会加倍
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and should_repay_date <= '${var:ST9}'
  ) as tmp
  group by product_id,loan_active_date,should_repay_date
) as new
full join (
  select
    product_id,
    loan_active_date,
    biz_date as should_repay_date,
    sum(loan_principal) as loan_principal
  from dw_new${var:db_suffix}.dw_loan_base_stat_should_repay_day
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by loan_active_date,product_id,should_repay_date
) as dws
on  new.product_id        = dws.product_id
and new.loan_active_date  = dws.loan_active_date
and new.should_repay_date = dws.should_repay_date
where nvl(new.loan_principal,0) - nvl(dws.loan_principal,0) != 0
order by should_repay_date,active_date,product_id
-- limit 10
;




set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-27;

invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_should_repay_day;
-- ods_new_s 应还 与 dw_new 应还 对比 应还金额
select
  'ods_new_s 应还 与 dw_new 应还 对比 应还金额'                         as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                         as cps,
  '${var:ST9}'                                                          as biz_date,
  nvl(dws.should_repay_date,new.should_repay_date)                      as should_repay_date,
  nvl(dws.product_id,new.product_id)                                    as product_id,
  nvl(new.should_repay_amount,0)                                        as amount_new,
  nvl(dws.should_repay_amount,0)                                        as amount_dw,
  nvl(new.should_repay_amount,0)    - nvl(dws.should_repay_amount,0)    as diff_amount,
  nvl(new.should_repay_principal,0)                                     as principal_new,
  nvl(dws.should_repay_principal,0)                                     as principal_dw,
  nvl(new.should_repay_principal,0) - nvl(dws.should_repay_principal,0) as diff_principal,
  nvl(new.should_repay_interest,0)                                      as interest_new,
  nvl(dws.should_repay_interest,0)                                      as interest_dw,
  nvl(new.should_repay_interest,0)  - nvl(dws.should_repay_interest,0)  as diff_interest,
  nvl(new.should_repay_term_fee,0)                                      as term_fee_new,
  nvl(dws.should_repay_term_fee,0)                                      as term_fee_dw,
  nvl(new.should_repay_term_fee,0)  - nvl(dws.should_repay_term_fee,0)  as diff_term_fee,
  nvl(new.should_repay_svc_fee,0)                                       as svc_fee_new,
  nvl(dws.should_repay_svc_fee,0)                                       as svc_fee_dw,
  nvl(new.should_repay_svc_fee,0)   - nvl(dws.should_repay_svc_fee,0)   as diff_svc_fee,
  nvl(new.should_repay_penalty,0)                                       as penalty_new,
  nvl(dws.should_repay_penalty,0)                                       as penalty_dw,
  nvl(new.should_repay_penalty,0)   - nvl(dws.should_repay_penalty,0)   as diff_penalty
from (
  select
    product_id                  as product_id,
    should_repay_date           as should_repay_date,
    sum(should_repay_amount)    as should_repay_amount,
    sum(should_repay_principal) as should_repay_principal,
    sum(should_repay_interest)  as should_repay_interest,
    sum(should_repay_term_fee)  as should_repay_term_fee,
    sum(should_repay_svc_fee)   as should_repay_svc_fee,
    sum(should_repay_penalty)   as should_repay_penalty
  from ods_new_s${var:db_suffix}.repay_schedule -- 必须经过去重。原因：滴滴多期的应还日可能会变成同一天，这样的放款金额就会加倍
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and should_repay_date <= '${var:ST9}'
  group by product_id,should_repay_date
) as new
full join (
  select
    product_id                  as product_id,
    biz_date                    as should_repay_date,
    sum(should_repay_amount)    as should_repay_amount,
    sum(should_repay_principal) as should_repay_principal,
    sum(should_repay_interest)  as should_repay_interest,
    sum(should_repay_term_fee)  as should_repay_term_fee,
    sum(should_repay_svc_fee)   as should_repay_svc_fee,
    sum(should_repay_penalty)   as should_repay_penalty
  from dw_new${var:db_suffix}.dw_loan_base_stat_should_repay_day
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id,biz_date
) as dws
on  new.product_id        = dws.product_id
and new.should_repay_date = dws.should_repay_date
where
nvl(new.should_repay_amount,0)    - nvl(dws.should_repay_amount,0)    != 0 or
nvl(new.should_repay_principal,0) - nvl(dws.should_repay_principal,0) != 0 or
nvl(new.should_repay_interest,0)  - nvl(dws.should_repay_interest,0)  != 0 or
nvl(new.should_repay_term_fee,0)  - nvl(dws.should_repay_term_fee,0)  != 0 or
nvl(new.should_repay_svc_fee,0)   - nvl(dws.should_repay_svc_fee,0)   != 0 or
nvl(new.should_repay_penalty,0)   - nvl(dws.should_repay_penalty,0)   != 0
order by should_repay_date,product_id
limit 10
;


====================================================================================================================================================================================================================


set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-27;

invalidate metadata ods_new_s${var:db_suffix}.repay_detail;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_repay_detail_day;

-- ods_new_s 实还 与 dw_new 实还 对比 实还金额
select
  'ods_new_s 实还 与 dw_new 实还 对比 实还金额'         as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  nvl(dws.biz_date,new.biz_date)                        as biz_date,
  nvl(dws.product_id,new.product_id)                    as product_id,
  nvl(new.paid_amount,0)                                as amount_ods,
  nvl(dws.paid_amount,0)                                as amount_dws,
  nvl(new.paid_amount,0)    - nvl(dws.paid_amount,0)    as diff_amount,
  nvl(new.paid_principal,0)                             as principal_ods,
  nvl(dws.paid_principal,0)                             as principal_dws,
  nvl(new.paid_principal,0) - nvl(dws.paid_principal,0) as diff_principal,
  nvl(new.paid_interest,0)                              as interest_ods,
  nvl(dws.paid_interest,0)                              as interest_dws,
  nvl(new.paid_interest,0)  - nvl(dws.paid_interest,0)  as diff_interest,
  nvl(new.paid_term_fee,0)                              as term_fee_ods,
  nvl(dws.paid_term_fee,0)                              as term_fee_dws,
  nvl(new.paid_term_fee,0)  - nvl(dws.paid_term_fee,0)  as diff_term_fee,
  nvl(new.paid_svc_fee,0)                               as svc_fee_ods,
  nvl(dws.paid_svc_fee,0)                               as svc_fee_dws,
  nvl(new.paid_svc_fee,0)   - nvl(dws.paid_svc_fee,0)   as diff_svc_fee,
  nvl(new.paid_penalty,0)                               as penalty_ods,
  nvl(dws.paid_penalty,0)                               as penalty_dws,
  nvl(new.paid_penalty,0)   - nvl(dws.paid_penalty,0)   as diff_penalty
from (
  select
    biz_date,
    product_id,
    sum(repay_amount)                               as paid_amount,    -- 金额
    sum(if(bnp_type = 'Pricinpal', repay_amount,0)) as paid_principal, -- 本金
    sum(if(bnp_type = 'Interest',  repay_amount,0)) as paid_interest,  -- 利息
    sum(if(bnp_type = 'TERMFee',   repay_amount,0)) as paid_term_fee,  -- 手续费
    sum(if(bnp_type = 'SVCFee',    repay_amount,0)) as paid_svc_fee,   -- 服务费
    sum(if(bnp_type = 'Penalty',   repay_amount,0)) as paid_penalty    -- 罚息
  from ods_new_s${var:db_suffix}.repay_detail
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and bnp_type in ('Pricinpal','Interest','TERMFee','SVCFee','Penalty')
  group by product_id,biz_date
  -- order by biz_date,product_id
) as new
full join (
  select
    biz_date,
    product_id,
    sum(repaid_amount)         as paid_amount,    -- 金额
    sum(repaid_principal)      as paid_principal, -- 本金
    sum(repaid_interest)       as paid_interest,  -- 利息
    sum(repaid_repay_term_fee) as paid_term_fee,  -- 手续费
    sum(repaid_repay_svc_fee)  as paid_svc_fee,   -- 服务费
    sum(repaid_penalty)        as paid_penalty    -- 罚息
  from dw_new${var:db_suffix}.dw_loan_base_stat_repay_detail_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id,biz_date
  -- order by biz_date,product_id
) as dws
on  new.product_id = dws.product_id
and new.biz_date   = dws.biz_date
where
nvl(new.paid_amount,0)    - nvl(dws.paid_amount,0)    != 0 or
nvl(new.paid_principal,0) - nvl(dws.paid_principal,0) != 0 or
nvl(new.paid_interest,0)  - nvl(dws.paid_interest,0)  != 0 or
nvl(new.paid_term_fee,0)  - nvl(dws.paid_term_fee,0)  != 0 or
nvl(new.paid_svc_fee,0)   - nvl(dws.paid_svc_fee,0)   != 0 or
nvl(new.paid_penalty,0)   - nvl(dws.paid_penalty,0)   != 0
order by biz_date,product_id
-- limit 10
;



set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-27;

invalidate metadata ods_new_s${var:db_suffix}.repay_detail;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_repay_detail_day;

-- ods_new_s 实还 与 dw_new 实还 对比 累计实还金额
select
  'ods_new_s 实还 与 dw_new 实还 对比 累计实还金额'     as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')         as cps,
  '${var:ST9}'                                          as biz_date,
  nvl(dws.product_id,new.product_id)                    as product_id,
  nvl(new.paid_amount,0)                                as amount_ods,
  nvl(dws.paid_amount,0)                                as amount_dws,
  nvl(new.paid_amount,0)    - nvl(dws.paid_amount,0)    as diff_amount,
  nvl(new.paid_principal,0)                             as principal_ods,
  nvl(dws.paid_principal,0)                             as principal_dws,
  nvl(new.paid_principal,0) - nvl(dws.paid_principal,0) as diff_principal,
  nvl(new.paid_interest,0)                              as interest_ods,
  nvl(dws.paid_interest,0)                              as interest_dws,
  nvl(new.paid_interest,0)  - nvl(dws.paid_interest,0)  as diff_interest,
  nvl(new.paid_term_fee,0)                              as term_fee_ods,
  nvl(dws.paid_term_fee,0)                              as term_fee_dws,
  nvl(new.paid_term_fee,0)  - nvl(dws.paid_term_fee,0)  as diff_term_fee,
  nvl(new.paid_svc_fee,0)                               as svc_fee_ods,
  nvl(dws.paid_svc_fee,0)                               as svc_fee_dws,
  nvl(new.paid_svc_fee,0)   - nvl(dws.paid_svc_fee,0)   as diff_svc_fee,
  nvl(new.paid_penalty,0)                               as penalty_ods,
  nvl(dws.paid_penalty,0)                               as penalty_dws,
  nvl(new.paid_penalty,0)   - nvl(dws.paid_penalty,0)   as diff_penalty
from (
  select
    product_id,
    sum(repay_amount)                               as paid_amount,    -- 金额
    sum(if(bnp_type = 'Pricinpal', repay_amount,0)) as paid_principal, -- 本金
    sum(if(bnp_type = 'Interest',  repay_amount,0)) as paid_interest,  -- 利息
    sum(if(bnp_type = 'TERMFee',   repay_amount,0)) as paid_term_fee,  -- 手续费
    sum(if(bnp_type = 'SVCFee',    repay_amount,0)) as paid_svc_fee,   -- 服务费
    sum(if(bnp_type = 'Penalty',   repay_amount,0)) as paid_penalty    -- 罚息
  from ods_new_s${var:db_suffix}.repay_detail
  where 1 > 0
    and biz_date <= '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and bnp_type in ('Pricinpal','Interest','TERMFee','SVCFee','Penalty')
  group by product_id
  -- order by product_id
) as new
full join (
  select
    product_id,
    sum(repaid_amount_count)         as paid_amount,    -- 金额
    sum(repaid_principal_count)      as paid_principal, -- 本金
    sum(repaid_interest_count)       as paid_interest,  -- 利息
    sum(repaid_repay_term_fee_count) as paid_term_fee,  -- 手续费
    sum(repaid_repay_svc_fee_count)  as paid_svc_fee,   -- 服务费
    sum(repaid_penalty_count)        as paid_penalty    -- 罚息
  from dw_new${var:db_suffix}.dw_loan_base_stat_repay_detail_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id
  -- order by biz_date,product_id
) as dws
on new.product_id = dws.product_id
where
nvl(new.paid_amount,0)    - nvl(dws.paid_amount,0)    != 0 or
nvl(new.paid_principal,0) - nvl(dws.paid_principal,0) != 0 or
nvl(new.paid_interest,0)  - nvl(dws.paid_interest,0)  != 0 or
nvl(new.paid_term_fee,0)  - nvl(dws.paid_term_fee,0)  != 0 or
nvl(new.paid_svc_fee,0)   - nvl(dws.paid_svc_fee,0)   != 0 or
nvl(new.paid_penalty,0)   - nvl(dws.paid_penalty,0)   != 0
order by product_id
-- limit 10
;


====================================================================================================================================================================================================================



set var:ST9=2020-10-27;

set var:db_suffix=;

-- set var:db_suffix=_cps;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day;

-- ods_new_s 借据 与 dw_new 逾期 对比 本金余额、已还本金、逾期本金
select
  'ods_new_s 借据 与 dw_new 逾期 对比 本金余额、已还本金、逾期本金' as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                     as cps,
  '${var:ST9}'                                                      as biz_date,
  nvl(new.product_id,dws.product_id)                                as product_id,
  nvl(new.repaid_principal,0)                                       as repaid_ods,
  nvl(dws.repaid_principal,0)                                       as repaid_dws,
  nvl(new.repaid_principal,0)  - nvl(dws.repaid_principal,0)        as diff_repaid,
  nvl(new.remain_principal,0)                                       as remain_ods,
  nvl(dws.remain_principal,0)                                       as remain_dws,
  nvl(new.remain_principal,0)  - nvl(dws.remain_principal,0)        as diff_remain,
  nvl(new.overdue_principal,0)                                      as overdue_ods,
  nvl(dws.overdue_principal,0)                                      as overdue_dws,
  nvl(new.overdue_principal,0) - nvl(dws.overdue_principal,0)       as diff_overdue
from (
  select
    product_id             as product_id,
    sum(paid_principal)    as repaid_principal,
    sum(remain_principal)  as remain_principal,
    sum(overdue_principal) as overdue_principal
  from ods_new_s${var:db_suffix}.loan_info
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
  group by product_id
  -- order by product_id
) as new
full join (
  select
    product_id             as product_id,
    sum(paid_principal)    as repaid_principal,
    sum(remain_principal)  as remain_principal,
    sum(overdue_principal) as overdue_principal
  from dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id
  -- order by product_id
) as dws
on new.product_id = dws.product_id
where
nvl(new.repaid_principal,0)  - nvl(dws.repaid_principal,0)  != 0 or
nvl(new.remain_principal,0)  - nvl(dws.remain_principal,0)  != 0 or
nvl(new.overdue_principal,0) - nvl(dws.overdue_principal,0) != 0
order by product_id
;




set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-27;

invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day;
-- ods_new_s 应还 与 dw_new 逾期 对比 未出账本金
select
  'ods_new_s 应还 与 dw_new 逾期 对比 未出账本金'               as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                 as cps,
  '${var:ST9}'                                                  as biz_date,
  new.product_id                                                as product_id,
  nvl(new.unposted_principal,0)                                 as principal_new,
  nvl(dws.unposted_principal,0)                                 as principal_dws,
  nvl(new.unposted_principal,0) - nvl(dws.unposted_principal,0) as diff_principal
from (
  select
    sum(should_repay_principal) as unposted_principal,
    product_id
  from ods_new_s${var:db_suffix}.repay_schedule
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and schedule_status = 'N'
  group by product_id
) as new
full join (
  select
    sum(unposted_principal) as unposted_principal,
    product_id
  from dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id
) as dws
on new.product_id = dws.product_id
where new.unposted_principal - nvl(dws.unposted_principal,0) != 0
order by product_id
;





set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-31;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day;
-- ods_new_s 应还 与 dw_new 逾期 对比 当期计划应还本金
select
  'ods_new_s 应还 与 dw_new 逾期 对比 当期计划应还本金'                 as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                         as cps,
  nvl(new.should_repay_date,dws.should_repay_date)                      as biz_date,
  nvl(new.product_id,dws.product_id)                                    as product_id,
  nvl(new.should_repay_loan_num,0)                                      as loan_num_new,
  nvl(dws.should_repay_loan_num,0)                                      as loan_num_dws,
  nvl(new.should_repay_loan_num,0)  - nvl(dws.should_repay_loan_num,0)  as diff_loan_num,
  nvl(new.should_repay_principal,0)                                     as principal_new,
  nvl(dws.should_repay_principal,0)                                     as principal_dws,
  nvl(new.should_repay_principal,0) - nvl(dws.should_repay_principal,0) as diff_principal
from (
  select
    product_id                  as product_id,
    should_repay_date           as should_repay_date,
    count(due_bill_no)          as should_repay_loan_num,
    sum(should_repay_principal) as should_repay_principal
  from (
    select
      product_id        as product_id,
      due_bill_no       as due_bill_no,
      should_repay_date as should_repay_date
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and s_d_date >= loan_active_date
  ) as loan
  join (
    select
      product_id             as product_id_all,
      should_repay_date      as should_repay_date_all,
      due_bill_no            as due_bill_no_all,
      should_repay_principal as should_repay_principal
    from ods_new_s${var:db_suffix}.repay_schedule
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and s_d_date >= loan_active_date
  ) as tmp
  on  product_id        = product_id_all
  and should_repay_date = should_repay_date_all
  and due_bill_no       = due_bill_no_all
  group by product_id,should_repay_date
  -- order by should_repay_date,product_id
) as new
full join (
  select
    product_id                       as product_id,
    should_repay_date_curr           as should_repay_date,
    sum(should_repay_loan_num_curr)  as should_repay_loan_num,
    sum(should_repay_principal_curr) as should_repay_principal
  from dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id,should_repay_date_curr
  -- order by should_repay_date,product_id
) as dws
where
nvl(new.should_repay_loan_num,0)  - nvl(dws.should_repay_loan_num,0)  != 0 or
nvl(new.should_repay_principal,0) - nvl(dws.should_repay_principal,0) != 0
order by biz_date,product_id
limit 10
;



set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-01;

invalidate metadata ods_new_s${var:db_suffix}.loan_info;
invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day;
-- ods_new_s 应还 与 dw_new 逾期 对比 实际应还本金
select
  'ods_new_s 应还 与 dw_new 逾期 对比 实际应还本金'                     as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                         as cps,
  nvl(new.should_repay_date,dws.should_repay_date)                      as biz_date,
  nvl(new.product_id,dws.product_id)                                    as product_id,
  nvl(new.should_repay_loan_num,0)                                      as loan_num_new,
  nvl(dws.should_repay_loan_num,0)                                      as loan_num_dws,
  nvl(new.should_repay_loan_num,0)  - nvl(dws.should_repay_loan_num,0)  as diff_loan_num,
  nvl(new.should_repay_principal,0)                                     as principal_new,
  nvl(dws.should_repay_principal,0)                                     as principal_dws,
  nvl(new.should_repay_principal,0) - nvl(dws.should_repay_principal,0) as diff_principal
from (
  select
    product_id                                             as product_id,
    should_repay_date                                      as should_repay_date,
    count(if(should_repay_principal = 0,null,due_bill_no)) as should_repay_loan_num,
    sum(should_repay_principal)                            as should_repay_principal
  from (
    select
      product_id        as product_id,
      due_bill_no       as due_bill_no,
      should_repay_date as should_repay_date
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and s_d_date >= loan_active_date
  ) as loan
  join (
    select
      product_id                              as product_id_all,
      should_repay_date                       as should_repay_date_all,
      due_bill_no                             as due_bill_no_all,
      should_repay_principal - paid_principal as should_repay_principal
    from ods_new_s${var:db_suffix}.repay_schedule
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and s_d_date >= loan_active_date
  ) as tmp
  on  product_id        = product_id_all
  and should_repay_date = should_repay_date_all
  and due_bill_no       = due_bill_no_all
  group by product_id,should_repay_date
  -- order by should_repay_date,product_id
) as new
full join (
  select
    product_id                              as product_id,
    should_repay_date_curr                  as should_repay_date,
    sum(should_repay_loan_num_curr_actual)  as should_repay_loan_num,
    sum(should_repay_principal_curr_actual) as should_repay_principal
  from dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id,should_repay_date_curr
  -- order by should_repay_date,product_id
) as dws
on  new.product_id        = dws.product_id
and new.should_repay_date = dws.should_repay_date
where nvl(new.should_repay_loan_num,0) - nvl(dws.should_repay_loan_num,0) != 0 or nvl(new.should_repay_principal,0) - nvl(dws.should_repay_principal,0) != 0
order by biz_date,product_id
limit 10
;




set var:db_suffix=;

-- set var:db_suffix=_cps;

set var:ST9=2020-10-01;

invalidate metadata ods_new_s${var:db_suffix}.repay_schedule;
invalidate metadata dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day;
-- ods_new_s 应还 与 dw_new 逾期 对比 上期计划应还本金
select
  'ods_new_s 应还 与 dw_new 逾期 对比 上期计划应还本金'                 as title,
  if('${var:db_suffix}' = '','代偿前','代偿后')                         as cps,
  nvl(new.should_repay_date,dws.should_repay_date)                      as biz_date,
  nvl(new.product_id,dws.product_id)                                    as product_id,
  nvl(new.should_repay_loan_num,0)                                      as loan_num_new,
  nvl(dws.should_repay_loan_num,0)                                      as loan_num_dws,
  nvl(new.should_repay_loan_num,0)  - nvl(dws.should_repay_loan_num,0)  as diff_loan_num,
  nvl(new.should_repay_principal,0)                                     as principal_new,
  nvl(dws.should_repay_principal,0)                                     as principal_dws,
  nvl(new.should_repay_principal,0) - nvl(dws.should_repay_principal,0) as diff_principal
from (
  select
    product_id                  as product_id,
    should_repay_date           as should_repay_date,
    count(due_bill_no)          as should_repay_loan_num,
    sum(should_repay_principal) as should_repay_principal
  from (
    select
      product_id        as product_id,
      due_bill_no       as due_bill_no,
      should_repay_date as should_repay_date
    from ods_new_s${var:db_suffix}.loan_info
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and s_d_date >= loan_active_date
  ) as loan
  join (
    select
      product_id             as product_id_all,
      should_repay_date      as should_repay_date_all,
      due_bill_no            as due_bill_no_all,
      should_repay_principal as should_repay_principal
    from ods_new_s${var:db_suffix}.repay_schedule
    where 1 > 0
      and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
      and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
      and s_d_date >= loan_active_date
  ) as tmp
  on  product_id        = product_id_all
  and should_repay_date = should_repay_date_all
  and due_bill_no       = due_bill_no_all
  group by product_id,should_repay_date
  -- order by should_repay_date,product_id
) as new
full join (
  select
    product_id                  as product_id,
    should_repay_date_curr      as should_repay_date,
    sum(should_repay_loan_num)  as should_repay_loan_num,
    sum(should_repay_principal) as should_repay_principal
  from dw_new${var:db_suffix}.dw_loan_base_stat_overdue_num_day_bak_20201028
  where 1 > 0
    and biz_date = '${var:ST9}'
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  group by product_id,should_repay_date_curr
  order by should_repay_date,product_id
) as dws
on  new.product_id        = dws.product_id
and new.should_repay_date = dws.should_repay_date
where nvl(new.should_repay_loan_num,0) - nvl(dws.should_repay_loan_num,0) != 0 or nvl(new.should_repay_principal,0) - nvl(dws.should_repay_principal,0) != 0
order by biz_date,product_id
limit 10
;




====================================================================================================================================================================================================================












































select
  biz_date,
  loan_terms,
  overdue_dob,
  overdue_mob,
  loan_active_date,
  should_repay_date_curr,
  product_id
from dw_new.dw_loan_base_stat_overdue_num_day
where 1 > 0
  and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
  and product_id = '001802'
  and biz_date < should_repay_date_curr
  and biz_date = '2020-08-02'
  and should_repay_date_curr = '2020-08-03'
  and loan_active_date like '2020-06%'
  and loan_terms > 1
  and overdue_days = 0
  -- and overdue_dob <= 30
  and overdue_mob <= 12
limit 20;


















select
  nvl(repay_schedule_min.product_id,       repay_schedule_max.product_id)        as product_id,
  nvl(repay_schedule_min.due_bill_no,      repay_schedule_max.due_bill_no)       as due_bill_no,
  nvl(repay_schedule_min.should_repay_date,repay_schedule_max.should_repay_date) as should_repay_date
from (
  select
    product_id,
    due_bill_no,
    min(should_repay_date) as should_repay_date
  from ods_new_s.repay_schedule
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and s_d_date >= loan_active_date
    and should_repay_date >= '${var:ST9}'
) as repay_schedule_min
full join (
  select
    product_id,
    due_bill_no,
    max(should_repay_date) as should_repay_date
  from ods_new_s.repay_schedule
  where 1 > 0
    and product_id in ('001801','001802','001803','001804','001901','001902','001903','001904','001905','001906','001907','002001','002002','002003','002004','002005','002006','002007')
    and '${var:ST9}' between s_d_date and date_sub(e_d_date,1)
    and s_d_date >= loan_active_date
) as repay_schedule_max
on  repay_schedule_min.product_id  = repay_schedule_max.product_id
and repay_schedule_min.due_bill_no = repay_schedule_max.due_bill_no
;









set var:ST9=date_sub(to_date(current_timestamp()),2);

set var:tb_suffix=_asset;

-- set var:tb_suffix=;

select
  if('${var:tb_suffix}' = '_asset','代偿前','代偿后') as cps,
  p_type,
  d_date,
  batch_date,
  count(1) as cnt,
  count(distinct due_bill_no) as due_bill_no,
  sum(repay_amt) as repay_amount
from ods.ecas_repay_hst${var:tb_suffix}
where 1 > 0
  and d_date between date_sub(${var:ST9},2) and ${var:ST9}
  and p_type in ('lx','lxzt','lx2')
  and batch_date >= date_sub(d_date,1)
group by d_date,batch_date,p_type
order by p_type,d_date,batch_date;
