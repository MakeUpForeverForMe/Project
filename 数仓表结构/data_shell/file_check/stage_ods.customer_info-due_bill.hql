-- 关闭自动 MapJoin （ Hive3 的 bug，引发 No work found for tablescan ）
-- set hive.auto.convert.join=false;
-- set hive.auto.convert.join.noconditionaltask=false;



-- +--------------------------------+-----------------+----------+------------+-----------+
-- |             title              |   project_id    | cnt_ods  | cnt_stage  | cnt_diff  |
-- +--------------------------------+-----------------+----------+------------+-----------+
-- | ods stage customer_info count  | 001601          | 7450     | 7595       | -145      |
-- | ods stage customer_info count  | CL202106070111  | 543      | 641        | -98       |
-- +--------------------------------+-----------------+----------+------------+-----------+



select
  'ods stage customer_info count'                          as title,
  nvl(ods.project_id,stage.project_id)                     as project_id,
  nvl(ods.due_bill_no,stage.due_bill_no)                   as due,
  nvl(ods.due_bill_no,null)                                as due_ods,
  nvl(stage.due_bill_no,null)                              as due_stage,
  nvl(ods.due_bill_no,null) != nvl(stage.due_bill_no,null) as due_diff
from (
  select distinct
    project_id,
    due_bill_no
  from ods.customer_info_abs
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
      case is_empty(map_from_str(extra_info)['项目编号'],project_id)
        when 'Cl00333' then 'cl00333'
        else is_empty(map_from_str(extra_info)['项目编号'],project_id)
      end                                                   as project_id,
      is_empty(map_from_str(extra_info)['借据号'],asset_id) as due_bill_no
    from stage.asset_02_t_principal_borrower_info
  ) as t1
  join stage.asset_01_t_loan_contract_info as t2
  on  t1.project_id  = case is_empty(map_from_str(t2.extra_info)['项目编号'],t2.project_id) when 'Cl00333' then 'cl00333' else is_empty(map_from_str(t2.extra_info)['项目编号'],t2.project_id) end
  and t1.due_bill_no = is_empty(map_from_str(t2.extra_info)['借据号'],t2.asset_id)
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
  and nvl(ods.project_id,stage.project_id) = 'DIDI201908161538'
  and (
    nvl(ods.due_bill_no,null)   is null or
    nvl(stage.due_bill_no,null) is null
  )
order by project_id,due
-- limit 10
;
