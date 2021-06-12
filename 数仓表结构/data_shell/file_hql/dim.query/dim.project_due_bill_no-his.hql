-- 设置动态分区
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=50000;
-- 执行sql前，加上如下参数，禁用hive矢量执行：
set hive.vectorized.execution.enabled=false;
set hive.vectorized.execution.reduce.enabled=false;
set hive.vectorized.execution.reduce.groupby.enabled=false;


insert overwrite table dim.project_due_bill_no partition(project_id,import_id)
select distinct
  t_basic.due_bill_no         as due_bill_no,
  t_related_assets.project_id as related_project_id,
  null                        as related_date,
  is_empty(
    loan_info.product_id,
    t_related_assets.project_id,
    t_basic.project_id
  )                           as partition_id,
  t_basic.project_id          as project_id,
  t_basic.import_id           as import_id
from (
  select
    case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
    if(size(split(serial_number,'_')) = 2,split(serial_number,'_')[1],split(serial_number,'_')[0]) as due_bill_no,
    import_id
  from stage.abs_t_basic_asset
  where 1 > 0
    and project_id not in (
      'PL202102010096', -- 1-1-1-1年第1期
      'PL201907050063', -- WY-中航-消费分期-2019年第1期
      'PL201908220067', -- 东亚中国-银登-车位分期-2019年第1期-te
      ''
    )
) as t_basic
left join (
  select
    case project_id when 'Cl00333' then 'cl00333' else project_id end as project_id,
    serial_number as related_due_bill_no,
    related_project_id
  from stage.abs_t_related_assets
) as t_related_assets
on  t_basic.project_id  = t_related_assets.related_project_id
and t_basic.due_bill_no = t_related_assets.related_due_bill_no
left join (
  select
    product_code as product_id,
    due_bill_no
  from stage.ecas_loan
  where d_date between date_sub(current_date(),3) and current_date()
    and p_type in (
      'ddht',
      -- 'htgy',
      ''
    )
) as loan_info
on t_basic.due_bill_no = loan_info.due_bill_no
-- order by t_basic.project_id
-- limit 100
;
