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
-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;




-- set hivevar:ST9=2020-10-16;
-- set hivevar:ST9=2020-10-17;

-- set hivevar:ST9=2021-05-12;

-- set hivevar:project_id=
--   select distinct project_id
--   from dim.project_info
--   where 1 > 0
-- ;





-- 封包时 只统计封包当天
with bill_info as (
  select
    loan_info.biz_date,
    loan_info.project_id,
    loan_info.due_bill_no,
    loan_info.remain_principal,
    bag_due.due_bill_no as bag_due_bill_no,
    bag_due.bag_date,
    bag_due.bag_id,
    array(
      -- 1 未偿本金余额分布
      case
        when loan_info.remain_principal <= 50000                                           then concat("1","|","5万元（含）以下",'|','1')
        when loan_info.remain_principal > 50000  and loan_info.remain_principal <= 100000  then concat("1","|","5万元（不含）- 10万元（含）",'|','2')
        when loan_info.remain_principal > 100000 and loan_info.remain_principal <= 200000  then concat("1","|","10万元（不含）- 20万元（含）",'|','3')
        when loan_info.remain_principal > 200000 and loan_info.remain_principal <= 300000  then concat("1","|","20万元（不含）- 30万元（含）",'|','4')
        when loan_info.remain_principal > 300000 and loan_info.remain_principal <= 400000  then concat("1","|","30万元（不含）- 40万元（含",'|','5')
        when loan_info.remain_principal > 400000 and loan_info.remain_principal <= 500000  then concat("1","|","40万元（不含）- 50万元（含）",'|','6')
        when loan_info.remain_principal > 500000 and loan_info.remain_principal <= 1000000 then concat("1","|","50万元（不含）- 100万元（含）",'|','7')
        else concat("1","|","100万元（不含）以上",'|','8')
      end,
      -- 2 资产利率分布
      case
        when loan_leading.loan_init_interest_rate <= 5                                                then concat("2","|","5%（含）以下",'|','1')
        when loan_leading.loan_init_interest_rate > 5  and loan_leading.loan_init_interest_rate <= 10 then concat("2","|","5%（不含）- 10%（含）",'|','2')
        when loan_leading.loan_init_interest_rate > 10 and loan_leading.loan_init_interest_rate <= 15 then concat("2","|","10%（不含）- 15%（含）",'|','3')
        when loan_leading.loan_init_interest_rate > 15 and loan_leading.loan_init_interest_rate <= 20 then concat("2","|","15%（不含）- 20%（含）",'|','4')
        else concat("2","|","20%（不含）以上",'|','5')
      end,
      -- 3 资产合同期限分布
      concat("3","|",concat(cast(loan_leading.contract_term as string),"期"),'|',cast(loan_leading.contract_term as string)),
      -- 4 资产剩余期限分布
      case
        when loan_info.loan_term_remain between 1  and 6  then concat("4","|","1期（含）~ 6期（含）",'|','1')
        when loan_info.loan_term_remain between 7  and 12 then concat("4","|","7期（含）~ 12期（含）",'|','2')
        when loan_info.loan_term_remain between 13 and 18 then concat("4","|","13期（含）~ 18期（含）",'|','3')
        when loan_info.loan_term_remain between 19 and 24 then concat("4","|","19期（含）~ 24期（含）",'|','4')
        when loan_info.loan_term_remain between 25 and 30 then concat("4","|","25期（含）~ 30期（含）",'|','5')
        when loan_info.loan_term_remain between 31 and 36 then concat("4","|","31期（含）~ 36期（含）",'|','6')
        when loan_info.loan_term_remain between 37 and 42 then concat("4","|","37期（含）~ 42期（含）",'|','7')
        when loan_info.loan_term_repaid between 43 and 48 then concat("4","|","43期（含）~ 48期（含）",'|','8')
        else concat("4","|","48期（不含）以上",'|','9')
      end,
      -- 5 资产已还期数分布
      case
        when loan_info.loan_term_repaid between 1  and 6  then concat("5","|","1期（含）~ 6期（含）",'|','1')
        when loan_info.loan_term_repaid between 7  and 12 then concat("5","|","7期（含）~ 12期（含）",'|','2')
        when loan_info.loan_term_repaid between 13 and 18 then concat("5","|","13期（含）~ 18期（含）",'|','3')
        when loan_info.loan_term_repaid between 19 and 24 then concat("5","|","19期（含）~ 24期（含）",'|','4')
        when loan_info.loan_term_repaid between 25 and 30 then concat("5","|","25期（含）~ 30期（含）",'|','5')
        when loan_info.loan_term_repaid between 31 and 36 then concat("5","|","31期（含）~ 36期（含）",'|','6')
        when loan_info.loan_term_repaid between 37 and 42 then concat("5","|","37期（含）~ 42期（含）",'|','7')
        when loan_info.loan_term_repaid between 43 and 48 then concat("5","|","43期（含）~ 48期（含）",'|','8')
        else concat("5","|","48期（不含）以上",'|','9')
      end,
      -- 6 账龄分布
      case
        when loan_info.account_age = 0               then concat("6","|","0（含）",'|','1')
        when loan_info.account_age between 1  and 6  then concat("6","|","1（含）- 6（含）",'|','2')
        when loan_info.account_age between 7  and 12 then concat("6","|","7（含）- 12（含）",'|','3')
        when loan_info.account_age between 13 and 18 then concat("6","|","13（含）- 18（含）",'|','4')
        when loan_info.account_age between 19 and 24 then concat("6","|","19（含）- 24（含）",'|','5')
        when loan_info.account_age between 25 and 30 then concat("6","|","25（含）- 30（含）",'|','6')
        when loan_info.account_age between 31 and 36 then concat("6","|","31（含）- 36（含）",'|','7')
        when loan_info.account_age between 37 and 42 then concat("6","|","37（含）- 42（含）",'|','8')
        when loan_info.account_age between 43 and 48 then concat("6","|","43（含）- 48（含）",'|','9')
        else concat("6","|","48（不含）- 项目最大合同期限",'|' ,'10')
      end,
      -- 7 合同剩余期限分布（账龄相关）
      case
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) = 0               then concat("7","|","0（含）",'|','1')
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) between 1  and 6  then concat("7","|","1（含）- 6（含）",'|','2')
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) between 7  and 12 then concat("7","|","7（含）- 12（含）",'|','3')
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) between 13 and 18 then concat("7","|","13（含）- 18（含）",'|','4')
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) between 19 and 24 then concat("7","|","19（含）- 24（含）",'|','5')
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) between 25 and 30 then concat("7","|","25（含）- 30（含）",'|','6')
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) between 31 and 36 then concat("7","|","31（含）- 36（含）",'|','7')
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) between 37 and 42 then concat("7","|","37（含）- 42（含）",'|','8')
        when nvl(loan_leading.contract_term,0) - nvl(loan_info.account_age,0) between 43 and 48 then concat("7","|","43（含）- 48（含）",'|','9')
        else concat("7","|","48（不含）- 项目最大合同期限",'|','10')
      end,
      -- 8 还款方式分布
      concat("8","|",loan_leading.loan_type_cn,'|',cast(abs(round(hash(loan_leading.loan_type_cn)%50)) as string)),
      -- 9 借款人年龄分布
      case
        when customer_info.age between 18 and 20 then concat("9","|",'18岁（含）- 20岁（含）','|','1')
        when customer_info.age between 21 and 25 then concat("9","|",'20岁（不含）- 25岁（含）','|','2')
        when customer_info.age between 26 and 30 then concat("9","|",'25岁（不含）- 30岁（含）','|','3')
        when customer_info.age between 31 and 35 then concat("9","|",'30岁（不含）- 35岁（含）','|','4')
        when customer_info.age between 36 and 40 then concat("9","|",'35岁（不含）- 40岁（含）','|','5')
        when customer_info.age between 41 and 45 then concat("9","|",'40岁（不含）- 45岁（含）','|','6')
        when customer_info.age between 46 and 50 then concat("9","|",'45岁（不含）- 50岁（含）','|','7')
        when customer_info.age > 50              then concat("9","|",'50岁（不含）以上','|','8')
        else concat("9","|",'0岁（含）- 18岁（不含）','|','9')
      end,
      -- 10 借款人行业分布
      concat("10","|",if(customer_info.job_type = '未知','Z',customer_info.job_type),'|',cast(abs(round(hash(customer_info.job_type) % 50)) as string)),
      -- 11 借款人年收入分布
      case
        when nvl(customer_info.income_year,0) <= 50000                                   then concat("11","|",'5万元（含）以下','|','1')
        when customer_info.income_year > 50000  and customer_info.income_year <= 100000  then concat("11","|",'5万元（不含）- 10万元（含）','|','2')
        when customer_info.income_year > 100000 and customer_info.income_year <= 200000  then concat("11","|",'10万元（不含）- 20万元（含）','|','3')
        when customer_info.income_year > 200000 and customer_info.income_year <= 300000  then concat("11","|",'20万元（不含）- 30万元（含）','|','4')
        when customer_info.income_year > 300000 and customer_info.income_year <= 400000  then concat("11","|",'30万元（不含）- 40万元（含）','|','5')
        when customer_info.income_year > 400000 and customer_info.income_year <= 500000  then concat("11","|",'40万元（不含）- 50万元（含）','|','6')
        when customer_info.income_year > 500000 and customer_info.income_year <= 1000000 then concat("11","|",'50万元（不含）- 100万元（含）','|','7')
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
      -- 16 借款人地区分布
      concat("16","|",customer_info.idcard_area,'|',cast(abs(round(hash(customer_info.idcard_area) % 50)) as string)),
      -- 17 抵押率分布
      case
        when loan_leading.mortgage_rate <= 10                                     then concat("17","|","10%（含）以下",'|','1')
        when loan_leading.mortgage_rate > 10 and loan_leading.mortgage_rate <= 20 then concat("17","|","10%（不含）- 20%（含）",'|','2')
        when loan_leading.mortgage_rate > 20 and loan_leading.mortgage_rate <= 30 then concat("17","|","20%（不含）- 30%（含）",'|','3')
        when loan_leading.mortgage_rate > 30 and loan_leading.mortgage_rate <= 40 then concat("17","|","30%（不含）- 40%（含）",'|','4')
        when loan_leading.mortgage_rate > 40 and loan_leading.mortgage_rate <= 50 then concat("17","|","40%（不含）- 50%（含）",'|','5')
        when loan_leading.mortgage_rate > 50 and loan_leading.mortgage_rate <= 60 then concat("17","|","50%（不含）- 60%（含）",'|','6')
        when loan_leading.mortgage_rate > 60 and loan_leading.mortgage_rate <= 70 then concat("17","|","60%（不含）- 70%（含）",'|','7')
        when loan_leading.mortgage_rate > 70 and loan_leading.mortgage_rate <= 80 then concat("17","|","70%（不含）- 80%（含）",'|','8')
        when loan_leading.mortgage_rate > 80 and loan_leading.mortgage_rate <= 90 then concat("17","|","80%（不含）- 90%（含）",'|','9')
        else concat("17","|","90%（不含）以上",'|','10')
      end,
      -- 18 车辆品牌分布
      concat("18","|",nvl(guaranty.car_brand,'NONE'),'|',cast(abs(round(hash(nvl(guaranty.car_brand,'NONE')) % 50)) as string)),
      -- 19 新旧车辆分布
      concat("19","|",nvl(guaranty.car_type,'NONE'),'|',cast(round(abs(hash(nvl(guaranty.car_type,'NONE')) % 50)) as string))
    ) as distribution_array
  from (
    select
      '${ST9}' as biz_date,
      project_id,
      due_bill_no,
      remain_principal,
      loan_term_remain,
      loan_term_repaid,
      account_age
    from ods.loan_info_abs
    where 1 > 0
      and project_id in (${project_id})
      and '${ST9}' between s_d_date and date_sub(e_d_date,1)
      and loan_status <> 'F'
  ) as loan_info
  inner join (
    select
      project_id,
      due_bill_no,
      loan_init_interest_rate,
      nvl(loan_type_cn,'Z') as loan_type_cn,
      contract_term,
      nvl(mortgage_rate,0)  as mortgage_rate
    from ods.loan_lending_abs
  ) as loan_leading
  on  loan_info.project_id  = loan_leading.project_id
  and loan_info.due_bill_no = loan_leading.due_bill_no
  inner join (
    select
      project_id,
      due_bill_no,
      age,
      job_type,
      income_year,
      idcard_area
    from ods.customer_info_abs
  ) as customer_info
  on  loan_info.project_id  = customer_info.project_id
  and loan_info.due_bill_no = customer_info.due_bill_no
  left join (
    select
      project_id,
      due_bill_no,
      max(if(map_key = 'wind_control_status',map_val,null)) as wind_control_status,
      max(if(map_key = 'cheat_level',map_val,null))         as cheat_level,
      max(if(map_key = 'score_range',map_val,null))         as score_range
    from ods.risk_control_abs
    where source_table in ('t_asset_wind_control_history')
      and map_key in ('wind_control_status','cheat_level','score_range')
    group by project_id,due_bill_no
  ) as risk_control
  on  loan_info.project_id  = risk_control.project_id
  and loan_info.due_bill_no = risk_control.due_bill_no
  left join (
    select
      due_bill_no,
      project_id,
      max(if(map_key = 'credit_level',map_val,null)) as credit_level
    from ods.risk_control_abs
    where source_table in ('t_duration_risk_control_result')
      and map_key in ('credit_level')
    group by project_id,due_bill_no
  ) as credit_control
  on  loan_info.project_id  = credit_control.project_id
  and loan_info.due_bill_no = credit_control.due_bill_no
  left join (
    select
      due_bill_no,
      project_id,
      car_brand,
      car_type
    from ods.guaranty_info_abs
  ) as guaranty
  on  loan_info.project_id  = guaranty.project_id
  and loan_info.due_bill_no = guaranty.due_bill_no
  left join (
    select
      bag_info.project_id,
      bag_info.bag_id,
      bag_info.bag_date,
      bag_due.due_bill_no
    from (
      select
        project_id,
        bag_id,
        bag_date
      from dim.bag_info
    ) as bag_info
    inner join (
      select
        project_id,
        bag_id,
        due_bill_no
      from dim.bag_due_bill_no
    ) as bag_due
    on  bag_info.project_id  = bag_due.project_id
    and bag_info.bag_id      = bag_due.bag_id
  ) as bag_due
  on  loan_info.project_id  = bag_due.project_id
  and loan_info.due_bill_no = bag_due.due_bill_no
  and loan_info.biz_date    >= bag_due.bag_date
)

-- 插入数据
insert overwrite table dm_eagle.abs_asset_distribution_day partition(biz_date,project_id)
select
  'y'                                                    as is_allbag,
  asset_tab_name                                         as asset_tab_name,
  asset_name                                             as asset_name,
  asset_name_order                                       as asset_name_order,
  nvl(asset_remain_principal,0)                          as remain_principal,
  nvl(asset_remain_principal / total_remain_principal,0) as remain_principal_ratio,
  nvl(asset_loan_num,0)                                  as loan_num,
  nvl(asset_loan_num / total_loan_num,0)                 as loan_numratio,
  nvl(asset_remain_principal / asset_loan_num,0)         as remain_principal_loan_num_avg,
  asset_biz_date                                         as biz_date,
  asset_project_id                                       as project_id
from (
  select
    project_id                   as asset_project_id,
    biz_date                     as asset_biz_date,
    split(part_info,'\\|')[0]    as asset_tab_name,
    split(part_info,'\\|')[2]    as asset_name_order,
    split(part_info,'\\|')[1]    as asset_name,
    count(distinct due_bill_no)  as asset_loan_num,
    sum(nvl(remain_principal,0)) as asset_remain_principal
  from bill_info
  lateral view explode(distribution_array) mode_info as part_info
  where 1 > 0
    and bag_due_bill_no is not null -- 过滤出封包资产，即所有包的数据
    and split(part_info,'\\|')[1] != 'NONE'
  group by project_id,biz_date,split(part_info,'\\|')[0],split(part_info,'\\|')[1],split(part_info,'\\|')[2]
) as asset_total
inner join (
  select
    project_id                   as total_project_id,
    count(distinct due_bill_no)  as total_loan_num,
    sum(nvl(remain_principal,0)) as total_remain_principal
  from bill_info
  where 1 > 0
    and bag_due_bill_no is not null -- 过滤出封包资产，即所有包的数据
  group by project_id
) as project_total
on asset_total.asset_project_id = project_total.total_project_id
union all
select
  'n'                                                    as is_allbag,
  asset_tab_name                                         as asset_tab_name,
  asset_name                                             as asset_name,
  asset_name_order                                       as asset_name_order,
  nvl(asset_remain_principal,0)                          as remain_principal,
  nvl(asset_remain_principal / total_remain_principal,0) as remain_principal_ratio,
  nvl(asset_loan_num,0)                                  as loan_num,
  nvl(asset_loan_num / total_loan_num,0)                 as loan_numratio,
  nvl(asset_remain_principal / asset_loan_num,0)         as remain_principal_loan_num_avg,
  asset_biz_date                                         as biz_date,
  asset_project_id                                       as project_id
from (
  select
    project_id                   as asset_project_id,
    biz_date                     as asset_biz_date,
    split(part_info,'\\|')[0]    as asset_tab_name,
    split(part_info,'\\|')[2]    as asset_name_order,
    split(part_info,'\\|')[1]    as asset_name,
    count(distinct due_bill_no)  as asset_loan_num,
    sum(nvl(remain_principal,0)) as asset_remain_principal
  from bill_info
  lateral view explode(distribution_array) mode_info as part_info
  where 1 > 0
    -- and bag_due_bill_no is not null -- 取项目数据，不需要过滤
    and split(part_info,'\\|')[1] != 'NONE'
  group by project_id,biz_date,split(part_info,'\\|')[0],split(part_info,'\\|')[1],split(part_info,'\\|')[2]
) as asset_total
inner join (
  select
    project_id                   as total_project_id,
    count(distinct due_bill_no)  as total_loan_num,
    sum(nvl(remain_principal,0)) as total_remain_principal
  from bill_info
  where 1 > 0
    -- and bag_due_bill_no is not null -- 取项目数据，不需要过滤
  group by project_id
) as project_total
on asset_total.asset_project_id = project_total.total_project_id
-- limit 10
;
