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

--set hivevar:ST9 = 2020-10-01;


-- 封包时
-- 只统计封包当天
with
bill_info as (
  select
    loan_info.due_bill_no,
    loan_info.project_id,
    loan_info.remain_principal,

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
    end                                                                                                                                             as un_principal_distribution,
    -- 2 资产利率分布
    case
      when loan_leading.loan_init_interest_rate <= 5                                                then concat("2","|","5%（含）以下",'|','1')
      when loan_leading.loan_init_interest_rate > 5  and loan_leading.loan_init_interest_rate <= 10 then concat("2","|","5%（不含）- 10%（含）",'|','2')
      when loan_leading.loan_init_interest_rate > 10 and loan_leading.loan_init_interest_rate <= 15 then concat("2","|","10%（不含）- 15%（含）",'|','3')
      when loan_leading.loan_init_interest_rate > 15 and loan_leading.loan_init_interest_rate <= 20 then concat("2","|","15%（不含）- 20%（含）",'|','4')
      else concat("2","|","20%（不含）以上",'|','5')
    end                                                                                                                                             as interest_rate_distribution,
    -- 3 资产合同期限分布
    concat("3","|",concat(cast(loan_leading.contract_term as string),"期"),'|',cast(loan_leading.contract_term as string))                          as residual_maturity_distribution,
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
    end                                                                                                                                             as loan_term_remain_distribution,
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
    end                                                                                                                                             as loan_term_repaid_distribution,
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
    end                                                                                                                                             as account_age_distribution,
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
    end                                                                                                                                             as residual_maturity_remain_distribution,
    -- 8 还款方式分布
    concat("8","|",loan_leading.loan_type_cn,'|',cast(abs(round(hash(loan_leading.loan_type_cn) % 50)) as string))                                  as loan_type_distribution,
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
    end                                                                                                                                             as age_distribution,
    -- 10 借款人行业分布
    concat("10","|",if(customer_info.job_type='未知','Z',customer_info.job_type),'|',cast(abs(round(hash(customer_info.job_type) % 50)) as string)) as job_type_distribution,
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
    end                                                                                                                                             as income_year_distribution,
    -- 12 借款人风控结果分布
    concat("12","|",nvl(risk_control.wind_control_status,-1),'|',cast(abs(round(hash(nvl(risk_control.wind_control_status,-1)) % 50)) as string))   as wind_control_status_distribution,
    -- 13 借款人信用等级分布
    concat("13","|",nvl(credit_control.credit_level,-1),'|',cast(abs(round(hash(nvl(credit_control.credit_level,-1)) % 50)) as string))             as credit_level_distribution,
    -- 14 借款人反欺诈等级分布
    concat("14","|",nvl(risk_control.cheat_level,-1),'|',cast(abs(round(hash(nvl(risk_control.cheat_level,-1)) % 50)) as string))                   as cheat_level_distribution,
    -- 15 借款人资产等级分布
    concat("15","|",nvl(risk_control.score_range,-1),'|',cast(abs(round(hash(nvl(risk_control.score_range,-1)) % 50) ) as string))                  as score_range_distribution,
    -- 16 借款人地区分布
    concat("16","|",customer_info.idcard_area,'|',cast(abs(round(hash(customer_info.idcard_area) % 50)) as string))                                 as idcard_area_distribution,
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
    end                                                                                                                                             as mortgage_rate_distribution,
    -- 18 车辆品牌分布
    concat("18","|",nvl(guaranty_info.car_brand,'NONE'),'|',cast(abs(round(hash(nvl(guaranty_info.car_brand,'NONE')) % 50)) as string))             as car_brand_distribution,
    -- 19 新旧车辆分布
    concat("19","|",nvl(guaranty_info.car_type,'NONE'),'|',cast(round(abs(hash(nvl(guaranty_info.car_type,'NONE')) % 50)) as string))               as car_type_distribution
  from (
    select
      due_bill_no,
      project_id,
      remain_principal,
      loan_term_remain,
      loan_term_repaid,
      account_age
    from ods.loan_info_abs
    where '${ST9}' between s_d_date and date_sub(e_d_date,1)
  ) as loan_info
  left join (
    select
      due_bill_no,
      project_id,
      loan_init_interest_rate,
      nvl(loan_type_cn,'Z') as loan_type_cn,
      contract_term,
      nvl(mortgage_rate,0)  as mortgage_rate
    from ods.loan_lending_abs
    where biz_date <= '${ST9}'
  ) as loan_leading
  on  loan_info.due_bill_no = loan_leading.due_bill_no
  and loan_info.project_id  = loan_leading.project_id
  left join (
    select
      due_bill_no,
      project_id,
      age,
      job_type,
      income_year,
      idcard_area
    from ods.customer_info_abs
    --where
  ) as customer_info
  on  loan_info.due_bill_no = customer_info.due_bill_no
  and loan_info.project_id  = customer_info.project_id
  left join (
    select
      due_bill_no,
      product_id,
      max(if(map_key = 'wind_control_status',map_val,null)) as wind_control_status,
      max(if(map_key = 'cheat_level',map_val,null))         as cheat_level,
      max(if(map_key = 'score_range',map_val,null))         as score_range
    from ods.risk_control
    where source_table in ('t_asset_wind_control_history')
      and map_key in ('wind_control_status','cheat_level','score_range')
    group by product_id,due_bill_no
  ) as risk_control
  on  loan_info.due_bill_no = risk_control.due_bill_no
  and loan_info.project_id  = risk_control.product_id
  left join (
    select
      due_bill_no,
      product_id,
      max(if(map_key = 'credit_level',map_val,null)) as credit_level
    from ods.risk_control
    where source_table in ('t_duration_risk_control_result')
      and map_key in ('credit_level')
    group by product_id,due_bill_no
  ) as credit_control
  on  loan_info.due_bill_no = credit_control.due_bill_no
  and loan_info.project_id  = credit_control.product_id
  left join (
    select
      due_bill_no,
      project_id,
      car_brand,
      car_type
    from ods.guaranty_info_abs
  ) as guaranty_info
  on  loan_info.due_bill_no = guaranty_info.due_bill_no
  and loan_info.project_id  = guaranty_info.project_id
),

--汇总数据--项目下所有有包的借据
project_total_bag_bill as (
  select
    bill_info.project_id,
    bag_info.bag_id,
    sum(remain_principal) as total_remain_principal,
    count(bill_info.due_bill_no)    as total_bill
  from bill_info
  join (
    select
      bag.bag_id,
      bag.project_id,
      bag_bill.due_bill_no
    from (
      select
        bag_id,
        project_id
      from dim.bag_info
      where bag_date = '${ST9}'
    ) as bag
    join dim.bag_due_bill_no as bag_bill
    on bag.bag_id = bag_bill.bag_id
  ) as bag_info
  on bill_info.due_bill_no=bag_info.due_bill_no
  and bill_info.project_id=bag_info.project_id
  group by bill_info.project_id,bag_id
)


-- 插入数据
insert overwrite table dm_eagle.abs_asset_distribution_bag_snapshot_day partition(biz_date,bag_id)
select
  tmp.project_id                                                    as project_id,
  asset_tab_name                                                    as asset_tab_name,
  asset_name                                                        as asset_name,
  asset_name_order                                                  as asset_name_order,
  sum(remain_principal)                                             as remain_principal,
  sum(remain_principal) / max(project_total.total_remain_principal) as remain_principal_ratio,
  count(tmp.due_bill_no)                                            as loan_num,
  count(tmp.due_bill_no) / max(project_total.total_bill)            as loan_numratio,
  sum(remain_principal) / count(tmp.due_bill_no)                    as remain_principal_loan_num_avg,
  '${ST9}'                                                          as biz_date,
 bag_info.bag_id                                                    as bag_id
from (
  select
    due_bill_no,
    project_id,
    remain_principal,
    split(part_info,'\\|')[0] as asset_tab_name,
    split(part_info,'\\|')[1] as asset_name,
    split(part_info,'\\|')[2] as asset_name_order
  from bill_info
  lateral view explode(array(
    -- 未尝本金分布
    un_principal_distribution,
    -- 利息分布
    interest_rate_distribution,
    -- 资产合同期限分布
    residual_maturity_distribution,
    -- 剩余期数分布
    loan_term_remain_distribution,
    -- 已还期数分布
    loan_term_repaid_distribution,
    -- 账龄分布
    account_age_distribution,
    -- 合同剩余期限分布
    residual_maturity_remain_distribution,
    -- 放款类型分布
    loan_type_distribution,
    -- 年龄分布
    age_distribution,
    -- 借款人行业类型分布
    job_type_distribution,
    -- 风控结果分布
    wind_control_status_distribution,
    -- 信用分布
    credit_level_distribution,
    -- 反欺诈分布
    cheat_level_distribution,
    -- 资产等级分布
    score_range_distribution,
    -- 省份证地区分布
    idcard_area_distribution,
    -- 抵押率
    mortgage_rate_distribution,
    -- 车库品牌
    car_brand_distribution,
    -- 车辆类型
    car_type_distribution
  ))mode_info as part_info
) as tmp
join (
  select
    bag.bag_id,
    bag.project_id,
    bag_bill.due_bill_no
  from (
    select
      bag_id,
      project_id
    from dim.bag_info
    where bag_date = '${ST9}'
  ) as bag
  join dim.bag_due_bill_no as bag_bill
  on bag.bag_id = bag_bill.bag_id
) as bag_info
on tmp.due_bill_no=bag_info.due_bill_no and tmp.project_id=bag_info.project_id
inner join project_total_bag_bill as project_total
on  tmp.project_id = project_total.project_id
and bag_info.bag_id     = project_total.bag_id
where asset_name != 'NONE'
group by asset_tab_name,asset_name,asset_name_order,tmp.project_id,bag_info.bag_id
-- limit 10
;
