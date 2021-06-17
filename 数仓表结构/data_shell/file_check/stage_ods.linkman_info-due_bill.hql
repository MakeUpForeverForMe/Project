-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;


-- +-------------------------------+-----------------+----------+------------+-----------+
-- |             title             |   project_id    | cnt_ods  | cnt_stage  | cnt_diff  |
-- +-------------------------------+-----------------+----------+------------+-----------+
-- | ods stage linkman_info count  | 001601          | 7619     | 7615       | 4         |
-- | ods stage linkman_info count  | CL202007020086  | 1156     | 1152       | 4         |
-- | ods stage linkman_info count  | CL202011090089  | 3970     | 3968       | 2         |
-- +-------------------------------+-----------------+----------+------------+-----------+



select
  'ods stage linkman_info count'         as title,
  nvl(ods.project_id,stage.project_id)   as project_id,
  nvl(ods.due_bill_no,stage.due_bill_no) as due,
  nvl(ods.due_bill_no,null)              as due_ods,
  nvl(stage.due_bill_no,null)            as due_stage
from (
  select distinct
    project_id,
    due_bill_no
  from ods.linkman_info_abs
  where 1 > 0
    and project_id not in (
      -- '001601',           -- 汇通
      -- 'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银新增
      'DIDI201908161538', -- 滴滴
      ''
    )
) as ods
full join (
  select distinct
    t3.project_id,
    t1.due_bill_no
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
    and t3.project_id not in (
      -- '001601',           -- 汇通
      -- 'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
) as stage
on  ods.project_id  = stage.project_id
and ods.due_bill_no = stage.due_bill_no
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
    nvl(ods.due_bill_no,null)   is null or
    nvl(stage.due_bill_no,null) is null or
    false
  )
order by project_id,due
-- limit 10
;
