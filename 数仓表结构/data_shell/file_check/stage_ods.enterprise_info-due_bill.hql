-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;


select
  'ods stage enterprise_info count'                        as title,
  nvl(ods.project_id,stage.project_id)                     as project_id,
  nvl(ods.due_bill_no,stage.due_bill_no)                   as due,
  nvl(ods.due_bill_no,null)                                as due_ods,
  nvl(stage.due_bill_no,null)                              as due_stage,
  nvl(ods.due_bill_no,null) != nvl(stage.due_bill_no,null) as due_diff
from (
  select distinct
    project_id,
    due_bill_no
  from ods.enterprise_info_abs
  where 1 > 0
    and project_id not in (
      -- '001601',           -- 汇通
      -- 'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银新增
      -- 'DIDI201908161538', -- 滴滴
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
      asset_id                                                          as due_bill_no
    from stage.asset_12_t_enterprise_info
  ) as t1
  join dim.project_due_bill_no as t3
  on  t1.project_id  = nvl(t3.related_project_id,t3.project_id)
  and t1.due_bill_no = t3.due_bill_no
  where 1 > 0
    and t3.project_id not in (
      -- '001601',           -- 汇通
      -- 'WS0005200001',     -- 瓜子
      -- 'CL202012280092',   -- 汇通国银
      -- 'DIDI201908161538', -- 滴滴
      ''
    )
) as stage
on  ods.project_id  = stage.project_id
and ods.due_bill_no = stage.due_bill_no
where 1 > 0
  -- and nvl(ods.project_id,stage.project_id) = 'DIDI201908161538'
  and (
    nvl(ods.due_bill_no,null)   is null or
    nvl(stage.due_bill_no,null) is null
  )
order by project_id,due
-- limit 10
;
