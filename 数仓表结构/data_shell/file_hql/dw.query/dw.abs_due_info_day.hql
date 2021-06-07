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
-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;



-- set hivevar:project_id='PL202105120104';

-- set hivevar:ST9=2021-05-20;

-- set hivevar:project_id=
--   select distinct project_id
--   from dim.project_info
--   where 1 > 0
-- ;


insert overwrite table dw.abs_due_info_day partition(biz_date,project_id)
select
  -- 借据级
  loan_info.due_bill_no,
  loan_info.loan_init_term,
  loan_info.loan_init_principal,
  loan_info.account_age,
  loan_info.loan_term_remain,
  loan_info.loan_term_repaid,
  loan_info.remain_principal,
  loan_info.remain_interest,
  nvl(loan_info_yesterday.remain_principal_yesterday,0) as remain_principal_yesterday,
  loan_info.loan_status,
  loan_info.paid_out_date,
  loan_info.paid_out_type,
  loan_info.overdue_due_bill_no,
  if(loan_info.overdue_days > 0,customer_info.user_hash_no,null) as overdue_user_hash_no,
  loan_info.overdue_date_start,
  loan_info.overdue_days,
  loan_info.dpd_days_max,
  loan_info.overdue_days_dpd,
  loan_once.dpd_map,
  loan_info.overdue_principal,
  loan_info.overdue_interest,
  loan_info.overdue_svc_fee,
  loan_info.overdue_term_fee,
  loan_info.overdue_penalty,
  loan_info.overdue_mult_amt,
  loan_info.overdue_remain_principal,

  -- 合同级
  loan_leading.contract_no,
  loan_leading.loan_init_interest_rate,
  loan_leading.loan_type_cn,
  loan_leading.contract_term,
  loan_leading.mortgage_rate,
  loan_leading.shoufu_amount,

  -- 客户级
  customer_info.user_hash_no,
  customer_info.age,
  customer_info.job_type,
  customer_info.income_year,
  customer_info.income_year_max,
  customer_info.income_year_min,
  customer_info.idcard_area,
  customer_info.idcard_province,

  -- 抵押物级
  guaranty.due_bill_no as due_bill_no_guaranty,
  guaranty.pawn_value,
  guaranty.guarantee_type,

  -- 分布
  array(
    -- 1 未偿本金余额分布
    case
      when loan_info.remain_principal <= 50000   then concat("1","|","5万元（含）以下",'|','1')
      when loan_info.remain_principal <= 100000  then concat("1","|","5万元（不含）- 10万元（含）",'|','2')
      when loan_info.remain_principal <= 200000  then concat("1","|","10万元（不含）- 20万元（含）",'|','3')
      when loan_info.remain_principal <= 300000  then concat("1","|","20万元（不含）- 30万元（含）",'|','4')
      when loan_info.remain_principal <= 400000  then concat("1","|","30万元（不含）- 40万元（含",'|','5')
      when loan_info.remain_principal <= 500000  then concat("1","|","40万元（不含）- 50万元（含）",'|','6')
      when loan_info.remain_principal <= 1000000 then concat("1","|","50万元（不含）- 100万元（含）",'|','7')
      else concat("1","|","100万元（不含）以上",'|','8')
    end,
    -- 2 资产利率分布
    case
      when loan_leading.loan_init_interest_rate <= 0.5  then concat("2","|","5%（含）以下",'|','1')
      when loan_leading.loan_init_interest_rate <= 0.1  then concat("2","|","5%（不含）- 10%（含）",'|','2')
      when loan_leading.loan_init_interest_rate <= 0.15 then concat("2","|","10%（不含）- 15%（含）",'|','3')
      when loan_leading.loan_init_interest_rate <= 0.2  then concat("2","|","15%（不含）- 20%（含）",'|','4')
      else concat("2","|","20%（不含）以上",'|','5')
    end,
    -- 3 资产合同期限分布
    concat("3","|",concat(cast(loan_leading.contract_term as string),"期"),'|',cast(loan_leading.contract_term as string)),
    -- 4 资产剩余期限分布
    case
      when loan_info.loan_term_remain <= 6  then concat("4","|","1期（含）~ 6期（含）",'|','1')
      when loan_info.loan_term_remain <= 12 then concat("4","|","7期（含）~ 12期（含）",'|','2')
      when loan_info.loan_term_remain <= 18 then concat("4","|","13期（含）~ 18期（含）",'|','3')
      when loan_info.loan_term_remain <= 24 then concat("4","|","19期（含）~ 24期（含）",'|','4')
      when loan_info.loan_term_remain <= 30 then concat("4","|","25期（含）~ 30期（含）",'|','5')
      when loan_info.loan_term_remain <= 36 then concat("4","|","31期（含）~ 36期（含）",'|','6')
      when loan_info.loan_term_remain <= 42 then concat("4","|","37期（含）~ 42期（含）",'|','7')
      when loan_info.loan_term_repaid <= 48 then concat("4","|","43期（含）~ 48期（含）",'|','8')
      else concat("4","|","48期（不含）以上",'|','9')
    end,
    -- 5 资产已还期数分布
    case
      when loan_info.loan_term_repaid <= 6  then concat("5","|","1期（含）~ 6期（含）",'|','1')
      when loan_info.loan_term_repaid <= 12 then concat("5","|","7期（含）~ 12期（含）",'|','2')
      when loan_info.loan_term_repaid <= 18 then concat("5","|","13期（含）~ 18期（含）",'|','3')
      when loan_info.loan_term_repaid <= 24 then concat("5","|","19期（含）~ 24期（含）",'|','4')
      when loan_info.loan_term_repaid <= 30 then concat("5","|","25期（含）~ 30期（含）",'|','5')
      when loan_info.loan_term_repaid <= 36 then concat("5","|","31期（含）~ 36期（含）",'|','6')
      when loan_info.loan_term_repaid <= 42 then concat("5","|","37期（含）~ 42期（含）",'|','7')
      when loan_info.loan_term_repaid <= 48 then concat("5","|","43期（含）~ 48期（含）",'|','8')
      else concat("5","|","48期（不含）以上",'|','9')
    end,
    -- 6 账龄分布
    case
      when loan_info.account_age = 0   then concat("6","|","0（含）",'|','1')
      when loan_info.account_age <= 6  then concat("6","|","1（含）- 6（含）",'|','2')
      when loan_info.account_age <= 12 then concat("6","|","7（含）- 12（含）",'|','3')
      when loan_info.account_age <= 18 then concat("6","|","13（含）- 18（含）",'|','4')
      when loan_info.account_age <= 24 then concat("6","|","19（含）- 24（含）",'|','5')
      when loan_info.account_age <= 30 then concat("6","|","25（含）- 30（含）",'|','6')
      when loan_info.account_age <= 36 then concat("6","|","31（含）- 36（含）",'|','7')
      when loan_info.account_age <= 42 then concat("6","|","37（含）- 42（含）",'|','8')
      when loan_info.account_age <= 48 then concat("6","|","43（含）- 48（含）",'|','9')
      else concat("6","|","48（不含）- 项目最大合同期限",'|' ,'10')
    end,
    -- 7 合同剩余期限分布（账龄相关）
    case
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) = 0   then concat("7","|","0（含）",'|','1')
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) <= 6  then concat("7","|","1（含）- 6（含）",'|','2')
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) <= 12 then concat("7","|","7（含）- 12（含）",'|','3')
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) <= 18 then concat("7","|","13（含）- 18（含）",'|','4')
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) <= 24 then concat("7","|","19（含）- 24（含）",'|','5')
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) <= 30 then concat("7","|","25（含）- 30（含）",'|','6')
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) <= 36 then concat("7","|","31（含）- 36（含）",'|','7')
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) <= 42 then concat("7","|","37（含）- 42（含）",'|','8')
      when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) <= 48 then concat("7","|","43（含）- 48（含）",'|','9')
      else concat("7","|","48（不含）- 项目最大合同期限",'|','10')
    end,
    -- 8 还款方式分布
    concat("8","|",loan_leading.loan_type_cn,'|',cast(abs(round(hash(loan_leading.loan_type_cn)%50)) as string)),
    -- 9 借款人年龄分布
    case
      when customer_info.age <= 18 then concat("9","|",'0岁（含）- 18岁（不含）','|','1')
      when customer_info.age <= 20 then concat("9","|",'18岁（含）- 20岁（含）','|','2')
      when customer_info.age <= 25 then concat("9","|",'20岁（不含）- 25岁（含）','|','3')
      when customer_info.age <= 30 then concat("9","|",'25岁（不含）- 30岁（含）','|','4')
      when customer_info.age <= 35 then concat("9","|",'30岁（不含）- 35岁（含）','|','5')
      when customer_info.age <= 40 then concat("9","|",'35岁（不含）- 40岁（含）','|','6')
      when customer_info.age <= 45 then concat("9","|",'40岁（不含）- 45岁（含）','|','7')
      when customer_info.age <= 50 then concat("9","|",'45岁（不含）- 50岁（含）','|','8')
      else concat("9","|",'50岁（不含）以上','|','9')
    end,
    -- 10 借款人行业分布
    concat("10","|",customer_info.job_type,'|',cast(abs(round(hash(customer_info.job_type) % 50)) as string)),
    -- 11 借款人年收入分布
    case
      when if(customer_info.income_year = 0,(customer_info.income_year_max + customer_info.income_year_min) / 2,customer_info.income_year) <= 50000   then concat("11","|",'5万元（含）以下','|','1')
      when if(customer_info.income_year = 0,(customer_info.income_year_max + customer_info.income_year_min) / 2,customer_info.income_year) <= 100000  then concat("11","|",'5万元（不含）- 10万元（含）','|','2')
      when if(customer_info.income_year = 0,(customer_info.income_year_max + customer_info.income_year_min) / 2,customer_info.income_year) <= 200000  then concat("11","|",'10万元（不含）- 20万元（含）','|','3')
      when if(customer_info.income_year = 0,(customer_info.income_year_max + customer_info.income_year_min) / 2,customer_info.income_year) <= 300000  then concat("11","|",'20万元（不含）- 30万元（含）','|','4')
      when if(customer_info.income_year = 0,(customer_info.income_year_max + customer_info.income_year_min) / 2,customer_info.income_year) <= 400000  then concat("11","|",'30万元（不含）- 40万元（含）','|','5')
      when if(customer_info.income_year = 0,(customer_info.income_year_max + customer_info.income_year_min) / 2,customer_info.income_year) <= 500000  then concat("11","|",'40万元（不含）- 50万元（含）','|','6')
      when if(customer_info.income_year = 0,(customer_info.income_year_max + customer_info.income_year_min) / 2,customer_info.income_year) <= 1000000 then concat("11","|",'50万元（不含）- 100万元（含）','|','7')
      else concat("11","|",'100万元（不含）以上','|','8')
    end,
    -- 12 借款人风控结果分布
    concat("12","|",nvl(risk_control.wind_control_status,-1),'|',cast(abs(round(hash(nvl(risk_control.wind_control_status,-1)) % 50)) as string)),
    -- 13 借款人信用等级分布
    concat("13","|",nvl(credit_control.credit_level,-1),'|',cast(abs(round(hash(nvl(credit_control.credit_level,-1)) % 50)) as string)),
    -- 14 借款人反欺诈等级分布
    concat("14","|",nvl(risk_control.cheat_level,-1),'|',cast(abs(round(hash(nvl(risk_control.cheat_level,-1)) % 50)) as string)),
    -- 15 借款人资产等级分布
    concat("15","|",nvl(risk_control.score_range,-1),'|',cast(abs(round(hash(nvl(risk_control.score_range,-1)) % 50) ) as string)),
    -- 16 借款人省份分布
    concat("16","|",customer_info.idcard_province,'|',cast(abs(round(hash(customer_info.idcard_province) % 50)) as string)),
    -- 17 抵押率分布
    case
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.1 then concat("17","|","10%（含）以下",'|','1')
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.2 then concat("17","|","10%（不含）- 20%（含）",'|','2')
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.3 then concat("17","|","20%（不含）- 30%（含）",'|','3')
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.4 then concat("17","|","30%（不含）- 40%（含）",'|','4')
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.5 then concat("17","|","40%（不含）- 50%（含）",'|','5')
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.6 then concat("17","|","50%（不含）- 60%（含）",'|','6')
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.7 then concat("17","|","60%（不含）- 70%（含）",'|','7')
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.8 then concat("17","|","70%（不含）- 80%（含）",'|','8')
      when nvl(if(guaranty.guarantee_type = '抵押担保',loan_info.loan_init_principal,0) / if(guaranty.guarantee_type = '抵押担保',guaranty.pawn_value,0),0) <= 0.9 then concat("17","|","80%（不含）- 90%（含）",'|','9')
      else concat("17","|","90%（不含）以上",'|','10')
    end,
    -- 18 车辆品牌分布
    concat("18","|",guaranty.car_brand,'|',cast(abs(round(hash(guaranty.car_brand) % 50)) as string)),
    -- 19 新旧车辆分布
    concat("19","|",guaranty.car_type,'|',cast(round(abs(hash(guaranty.car_type) % 50)) as string)),
    -- 20 购车融资比例分布
    case
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.1 then concat("20","|","10%（含）以下",'|','1')
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.2 then concat("20","|","10%（不含）- 20%（含）",'|','2')
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.3 then concat("20","|","20%（不含）- 30%（含）",'|','3')
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.4 then concat("20","|","30%（不含）- 40%（含）",'|','4')
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.5 then concat("20","|","40%（不含）- 50%（含）",'|','5')
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.6 then concat("20","|","50%（不含）- 60%（含）",'|','6')
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.7 then concat("20","|","60%（不含）- 70%（含）",'|','7')
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.8 then concat("20","|","70%（不含）- 80%（含）",'|','8')
      when loan_info.loan_init_principal / (loan_info.loan_init_principal + loan_leading.shoufu_amount) <= 0.9 then concat("20","|","80%（不含）- 90%（含）",'|','9')
      else concat("20","|","90%（不含）以上",'|','10')
    end,
    -- 21 监控等级
    case
      when loan_info.overdue_days > 30 or (duration_result.black_4 = 0 and duration_result.black_3 = 1 and duration_result.black_2 = 1 and duration_result.black_1 = 1) then concat("21","|",'预警','|','1')
      when duration_result.score_1 between 17 and 20 and duration_result.score_2 between 17 and 20 and duration_result.score_3 between 17 and 20                        then concat("21","|",'关注','|','2')
      else concat("21","|",'正常','|','3')
    end
  ) as distribution_array,

  -- 日期项目
  loan_info.biz_date,
  loan_info.project_id
from (
  select
    project_id                              as project_id,
    due_bill_no                             as due_bill_no,
    loan_init_term                          as loan_init_term,
    loan_init_principal                     as loan_init_principal,
    loan_status                             as loan_status,
    is_empty(paid_out_date)                 as paid_out_date,
    is_empty(paid_out_type)                 as paid_out_type,
    loan_term_remain                        as loan_term_remain,
    loan_term_repaid                        as loan_term_repaid,
    remain_principal                        as remain_principal,
    remain_interest                         as remain_interest,
    account_age                             as account_age,
    split(concat_ws(',',
      if(overdue_days >= 1, '1+',  null),
      if(overdue_days > 7,  '7+',  null),
      if(overdue_days > 14, '14+', null),
      if(overdue_days > 30, '30+', null),
      if(overdue_days > 60, '60+', null),
      if(overdue_days > 90, '90+', null),
      if(overdue_days > 120,'120+',null),
      if(overdue_days > 150,'150+',null),
      if(overdue_days > 180,'180+',null),
      case
        when overdue_days between 1   and 7   then '1_7'
        when overdue_days between 8   and 14  then '8_14'
        when overdue_days between 15  and 30  then '15_30'
        when overdue_days between 31  and 60  then '31_60'
        when overdue_days between 61  and 90  then '61_90'
        when overdue_days between 91  and 120 then '91_120'
        when overdue_days between 121 and 150 then '121_150'
        when overdue_days between 151 and 180 then '151_180'
        else null
      end
    ),',')                                  as overdue_days_dpd,
    overdue_days                            as overdue_days,
    dpd_days_max                            as dpd_days_max,
    overdue_principal                       as overdue_principal,
    overdue_interest                        as overdue_interest,
    overdue_svc_fee                         as overdue_svc_fee,
    overdue_term_fee                        as overdue_term_fee,
    overdue_penalty                         as overdue_penalty,
    overdue_mult_amt                        as overdue_mult_amt,
    overdue_date_start                      as overdue_date_start,
    if(overdue_days > 0,due_bill_no,null)   as overdue_due_bill_no,
    if(overdue_days > 0,remain_principal,0) as overdue_remain_principal,
    '${ST9}'                                as biz_date
  from ods.loan_info_abs
  where 1 > 0
    and project_id in (${project_id})
    and '${ST9}' between s_d_date and date_sub(e_d_date,1)
    -- and project_id = '001601'
    -- and due_bill_no = '1000000965'
) as loan_info
inner join (
  select
    project_id,
    due_bill_no,
    contract_no,
    loan_init_interest_rate,
    loan_type_cn,
    contract_term,
    mortgage_rate,
    shoufu_amount
  from ods.loan_lending_abs
  where 1 > 0
    -- and project_id in (${project_id})
    -- and project_id = '001601'
    -- and due_bill_no = '1000000965'
) as loan_leading
on  loan_info.project_id  = loan_leading.project_id
and loan_info.due_bill_no = loan_leading.due_bill_no
inner join (
  select
    project_id,
    due_bill_no,
    user_hash_no,
    age,
    job_type,
    income_year,
    income_year_max,
    income_year_min,
    idcard_area,
    idcard_province
  from ods.customer_info_abs
  where 1 > 0
    -- and project_id in (${project_id})
    -- and project_id = '001601'
    -- and due_bill_no = '1000000965'
) as customer_info
on  loan_info.project_id  = customer_info.project_id
and loan_info.due_bill_no = customer_info.due_bill_no
left join ( -- 取前一天的剩余本金，是为了计算提前结清的数据。提前结清当天的剩余本金会因为提前结清而变为 0
  select
    project_id,
    due_bill_no,
    remain_principal as remain_principal_yesterday
  from ods.loan_info_abs
  where 1 > 0
    and date_sub('${ST9}',1) between s_d_date and date_sub(e_d_date,1)
    -- and project_id = '001601'
    -- and due_bill_no = '1000000965'
) as loan_info_yesterday
on  loan_info.project_id  = loan_info_yesterday.project_id
and loan_info.due_bill_no = loan_info_yesterday.due_bill_no
left join (
  select
    project_id,
    due_bill_no,
    map_from_str(concat('{"',concat_ws(',"',collect_list(concat_ws('":',overdue_days_once,dpd_map))),'}')) as dpd_map
    -- str_to_map(concat_ws(',',collect_list(concat_ws(':',overdue_days_once,dpd_map)))) as dpd_map
  from (
    select
      project_id,
      due_bill_no,
      cast(overdue_days as string) as overdue_days_once,
      concat('{"',concat_ws(',"',collect_list(concat_ws('":',overdue_date_start,cast(remain_principal as string)))),'}') as dpd_map
      -- str_to_map(concat_ws(',',collect_list(concat_ws(':',overdue_date_start,cast(remain_principal as string))))) as dpd_map
    from ods.loan_info_abs
    where 1 > 0
      -- and project_id in (${project_id})
      and s_d_date <= '${ST9}'
      and overdue_days in (1,8,15,31,61,91,121,151,181)

      -- and project_id  = '001601'
      -- and due_bill_no = '1000000965'

      -- and project_id  = 'CL202011090089'
      -- and due_bill_no = '1000001858' -- overdue_days >= 61

      -- and project_id  = 'CL201911130070'
      -- and due_bill_no = '5100854335' -- 借据有问题
    group by project_id,due_bill_no,overdue_days
  ) as tmp
  group by project_id,due_bill_no
) as loan_once
on  loan_info.project_id  = loan_once.project_id
and loan_info.due_bill_no = loan_once.due_bill_no
left join (
  select
    project_id,
    due_bill_no,
    car_brand,
    car_type,
    pawn_value,
    guarantee_type
  from ods.guaranty_info_abs
  where 1 > 0
    -- and project_id in (${project_id})
    -- and project_id = '001601'
    -- and due_bill_no = '1000000965'
) as guaranty
on  loan_info.project_id  = guaranty.project_id
and loan_info.due_bill_no = guaranty.due_bill_no
left join (
  select
    project_id,
    due_bill_no,
    max(if(map_key = 'wind_control_status',map_val,null)) as wind_control_status,
    max(if(map_key = 'cheat_level',map_val,null))         as cheat_level,
    max(if(map_key = 'score_range',map_val,null))         as score_range
  from ods.risk_control_abs
  where 1 > 0
    -- and project_id in (${project_id})
    and source_table = 't_asset_wind_control_history'
    and map_key in ('wind_control_status','cheat_level','score_range')
    -- and project_id = '001601'
    -- and due_bill_no = '1000000965'
  group by project_id,due_bill_no
) as risk_control
on  loan_info.project_id  = risk_control.project_id
and loan_info.due_bill_no = risk_control.due_bill_no
left join (
  select
    project_id,
    due_bill_no,
    map_val as credit_level
  from ods.risk_control_abs
  where 1 > 0
    -- and project_id in (${project_id})
    and source_table = 't_duration_risk_control_result'
    and map_key = 'credit_level'
    -- and project_id = '001601'
    -- and due_bill_no = '1000000965'
) as credit_control
on  loan_info.project_id  = credit_control.project_id
and loan_info.due_bill_no = credit_control.due_bill_no
left join (
  select
    project_id,
    due_bill_no,

    max(if(execute_month = add_months('${ST9}',-1,'yyyy-MM'),inner_black,null)) as black_1,
    max(if(execute_month = add_months('${ST9}',-2,'yyyy-MM'),inner_black,null)) as black_2,
    max(if(execute_month = add_months('${ST9}',-3,'yyyy-MM'),inner_black,null)) as black_3,
    max(if(execute_month = add_months('${ST9}',-4,'yyyy-MM'),inner_black,null)) as black_4,

    max(if(execute_month = add_months('${ST9}',-1,'yyyy-MM'),score_range,0))    as score_1,
    max(if(execute_month = add_months('${ST9}',-2,'yyyy-MM'),score_range,0))    as score_2,
    max(if(execute_month = add_months('${ST9}',-3,'yyyy-MM'),score_range,0))    as score_3
  from (
    select
      project_id,
      due_bill_no,
      max(if(map_key = 'execute_month',map_val,null)) as execute_month,
      max(if(map_key = 'score_range',  map_val,null)) as score_range,
      max(if(map_key = 'inner_black',  map_val,null)) as inner_black,
      max(if(map_key = 'state',        map_val,null)) as state
    from ods.risk_control_abs
    where 1 > 0
      -- and project_id in (${project_id})
      and source_table = 'duration_result'
      and map_key in ('execute_month','score_range','inner_black','state')
    group by project_id,due_bill_no,date_format(create_time,'yyyy-MM-dd')
  ) as tmp
  where 1 > 0
    and state = 2
    and execute_month between add_months('${ST9}',-4,'yyyy-MM') and add_months('${ST9}',-1,'yyyy-MM')
  group by project_id,due_bill_no
) as duration_result
on  loan_info.project_id  = duration_result.project_id
and loan_info.due_bill_no = duration_result.due_bill_no
-- limit 10
;
