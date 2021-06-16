-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;


select
  'ods stage loan_info_inter count'            as title,
  nvl(ods.project_id,stage.project_id)         as project_id,
  nvl(ods.loan_status_cn,stage.loan_status_cn) as status,
  nvl(ods.loan_status_cn,null)                 as status_ods,
  nvl(stage.loan_status_cn,null)               as status_stage,
  nvl(ods.cnt,0)                               as cnt_ods,
  nvl(stage.cnt,0)                             as cnt_stage,
  nvl(ods.cnt,0) - nvl(stage.cnt,0)            as cnt_diff
from (
  select
    project_id,
    loan_status_cn,
    count(distinct due_bill_no) as cnt
  from (
    select
      t3.project_id,
      t1.due_bill_no,
      case t1.loan_status_cn when '已还清' then '已结清' else t1.loan_status_cn end loan_status_cn,
      t1.biz_date as s_d_date,
      lead(t1.biz_date,1,'3000-12-31') over(partition by t3.project_id,t1.due_bill_no,t1.loan_term,t1.should_repay_date order by t1.biz_date) as e_d_date
    from ods.loan_info_inter as t1
    join dim.project_due_bill_no as t3
    on  t1.product_id  = t3.partition_id
    and t1.due_bill_no = t3.due_bill_no
  ) as tmp
  where 1 > 0
    and if('${ST9}' = '\$\{ST9\}',if(current_timestamp > concat(current_date,' ','18:00:00'),date_sub(current_date,1),date_sub(current_date,2)),'${ST9}') between s_d_date and date_sub(e_d_date,1)
    -- and project_id = 'DIDI201908161538'
    and project_id not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银新增
      'DIDI201908161538', -- 滴滴
      ''
    )
  group by project_id,loan_status_cn
) as ods
full join (
  select
    t3.project_id,
    t1.loan_status_cn,
    count(distinct t1.due_bill_no) as cnt
  from (
    select
      case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
      asset_id as due_bill_no,
      assets_status as loan_status_cn,
      account_date
    from stage.asset_10_t_asset_check
  ) as t1
  join (
    select
      case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
      asset_id as due_bill_no,
      max(account_date) as account_date
    from stage.asset_10_t_asset_check
    where account_date <= if('${ST9}' = '\$\{ST9\}',if(current_timestamp > concat(current_date,' ','18:00:00'),date_sub(current_date,1),date_sub(current_date,2)),'${ST9}')
    group by case project_id when 'Cl00333' then 'cl00333' else project_id end,asset_id
  ) as t2
  on  t1.project_id   = t2.project_id
  and t1.due_bill_no  = t2.due_bill_no
  and t1.account_date = t2.account_date
  join dim.project_due_bill_no as t3
  on  t1.project_id  = nvl(t3.related_project_id,t3.project_id)
  and t1.due_bill_no = t3.due_bill_no
  where t3.project_id not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
  group by t3.project_id,t1.loan_status_cn
) as stage
on  ods.project_id     = stage.project_id
and ods.loan_status_cn = stage.loan_status_cn
where 1 > 0
  and nvl(ods.project_id,stage.project_id) in (
    -- 汇通国银车分期
    'cl00297',
    'cl00306',
    'cl00309',
    'CL201911130070',
    'CL202002240081',

    -- 汇通国银新增
    'CL202012280092',
    'CL202106110115',

    -- 汇通应收ABN
    'CL202011090089',
    ''
  )
  and (
    nvl(ods.cnt,0) - nvl(stage