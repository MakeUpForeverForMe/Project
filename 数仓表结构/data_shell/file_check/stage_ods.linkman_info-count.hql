-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;


select
  'ods stage linkman_info count'       as title,
  nvl(ods.project_id,stage.project_id) as project_id,
  nvl(ods.cnt,0)                       as cnt_ods,
  nvl(stage.cnt,0)                     as cnt_stage,
  nvl(ods.cnt,0) - nvl(stage.cnt,0)    as cnt_diff
from (
  select
    project_id,
    count(distinct due_bill_no) as cnt
  from ods.linkman_info_abs
  where 1 > 0
    -- and project_id = 'DIDI201908161538'
    and project_id not in (
      -- '001601',           -- 汇通
      -- 'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银新增
      'DIDI201908161538', -- 滴滴
      ''
    )
  group by project_id
) as ods
full join (
  select
    t3.project_id,
    count(distinct t1.due_bill_no) as cnt
  from (
    select
      case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
      asset_id as due_bill_no
    from stage.asset_03_t_contact_person_info
  ) as t1
  join dim.project_due_bill_no as t3
  on  t1.project_id  = nvl(t3.related_project_id,t3.project_id)
  and t1.due_bill_no = t3.due_bill_no
  where 1 > 0
    -- and t3.project_id = 'DIDI201908161538'
    and t3.project_id not in (
      -- '001601',           -- 汇通
      -- 'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
  group by t3.project_id
) as stage
on ods.project_id = stage.project_id
where 1 > 0
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
    nvl(ods.cnt,0) - nvl(stage.cnt,0) != 0
  )
order by project_id,cnt_diff
-- limit 10
;
