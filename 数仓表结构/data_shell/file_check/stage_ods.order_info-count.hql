-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;


select
  'ods stage order_info count'         as title,
  nvl(ods.biz_date,stage.biz_date)     as biz_date,
  nvl(ods.biz_date,null)               as biz_date_ods,
  nvl(stage.biz_date,null)             as biz_date_stage,
  nvl(ods.project_id,stage.project_id) as project_id,
  nvl(ods.cnt,0)                       as cnt_ods,
  nvl(stage.cnt,0)                     as cnt_stage,
  nvl(ods.cnt,0) - nvl(stage.cnt,0)    as cnt_diff
from (
  select
    nvl(txn_date,to_date(txn_time)) as biz_date,
    project_id,
    count(distinct due_bill_no) as cnt
  from ods.order_info_abs
  where 1 > 0
    -- and project_id = 'DIDI201908161538'
    and project_id not in (
      -- '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银新增
      'DIDI201908161538', -- 滴滴
      ''
    )
  group by project_id,nvl(txn_date,to_date(txn_time))
) as ods
full join (
  select
    t1.biz_date,
    t3.project_id,
    count(distinct t1.due_bill_no) as cnt
  from (
    select
      is_empty(datefmt(map_from_str(extra_info)['交易时间'],'','yyyy-MM-dd'),datefmt(trade_time,'','yyyy-MM-dd')) as biz_date,
      case is_empty(map_from_str(extra_info)['项目编号'],project_id)
        when 'Cl00333' then 'cl00333'
        else is_empty(map_from_str(extra_info)['项目编号'],project_id)
      end as project_id,
      is_empty(map_from_str(extra_info)['借据号'],asset_id) as due_bill_no
    from stage.asset_06_t_asset_pay_flow
  ) as t1
  join dim.project_due_bill_no as t3
  on  t1.project_id  = nvl(t3.related_project_id,t3.project_id)
  and t1.due_bill_no = t3.due_bill_no
  where 1 > 0
    -- and t3.project_id = 'DIDI201908161538'
    and t3.project_id not in (
      -- '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
  group by t3.project_id,t1.biz_date
) as stage
on  ods.project_id = stage.project_id
and ods.biz_date   = stage.biz_date
where 1 > 0
  and nvl(ods.project_id,stage.project_id) not in (
    '001601',
    'CL202011090089',
    'CL202007020086',
    'CL202003230083',
    'CL202011090088',
    'CL202012160091',
    'CL202103160101',
    'CL202106070111',
    'CL202011090090',
    'CL202101220094',
    ''
  )
  -- and nvl(ods.project_id,stage.project_id) in (
  --   -- 汇通国银车分期
  --   'cl00297',
  --   'cl00306',
  --   'cl00309',
  --   'CL201911130070',
  --   'CL202002240081',

  --   -- 汇通国银新增
  --   'CL202012280092',
  --   'CL202106110115',

  --   -- 汇通应收ABN
  --   'CL202011090089',
  --   ''
  -- )
  and (
    nvl(ods.cnt,0) - nvl(stage.cnt,0) != 0    or
    nvl(ods.biz_date,null)            is null or
    nvl(stage.biz_date,null)          is null or
    false
  )
order by project_id,biz_date,cnt_diff
limit 100
;
