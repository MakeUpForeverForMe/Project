-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


insert overwrite table dim.project_due_bill_no partition(project_id)
select distinct
  asset_01.due_bill_no                          as due_bill_no,
  t_project.related_project_id                  as related_project_id,
  null                                          as related_date,
  nvl(loan_info.product_id,asset_01.project_id) as partition_id,
  asset_01.project_id                           as project_id
from (
  select distinct
    t_project.project_id,
    t_related_assets.project_id as related_project_id
  from (
    select project_id
    from stage.abs_t_project
    where 1 > 0
      and project_id not in (
        'PL202102010096'
      )
  ) as t_project
  left join (
    select
      project_id,
      related_project_id
    from stage.abs_t_related_assets
  ) as t_related_assets
  on t_project.project_id = t_related_assets.related_project_id
) as t_project
join (
  select
    is_empty(map_from_str(extra_info)['借据号'],asset_id) as due_bill_no,
    case is_empty(map_from_str(extra_info)['项目编号'],project_id)
    when 'Cl00333' then 'cl00333'
    else is_empty(map_from_str(extra_info)['项目编号'],project_id)
    end                                                   as project_id
  from stage.asset_01_t_loan_contract_info
  where 1 > 0
    and is_empty(map_from_str(extra_info)['借据号'],asset_id) not in (
      '5100835880',
      '5100836522',
      '5100839019',
      '5100842477',
      '5100844953',
      '5100847081',
      '5100848663',
      '5100851402',
      '5100851697',
      '5100854650',
      '5100855935',
      '5100856230',
      '5100857239',
      '5100871716',
      '5100872146',
      '5100874704',
      ''
    )
) as asset_01
on t_project.project_id = asset_01.project_id
join (
  select
    is_empty(map_from_str(extra_info)['项目编号'],project_id) as project_id,
    is_empty(map_from_str(extra_info)['借据号'],asset_id)     as due_bill_no
  from stage.asset_02_t_principal_borrower_info
) as asset_02
on  asset_01.project_id  = asset_02.project_id
and asset_01.due_bill_no = asset_02.due_bill_no
left join (
  select distinct
    product_code as product_id,
    due_bill_no
  from stage.ecas_loan
  where d_date = date_sub(current_date(),2)
    and p_type in ('ddht','htgy')
) as loan_info
on asset_01.due_bill_no = loan_info.due_bill_no
-- limit 100
;
