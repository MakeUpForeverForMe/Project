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
select
  project_due.due_bill_no,
  project_due.related_project_id,
  project_due.related_date,
  is_empty(
    loan_info.product_id,
    project_due.related_project_id,
    project_due.project_id
  ) as partition_id,
  project_due.project_id,
  project_due.import_id
from (
  select
    dues.serialNumber     as due_bill_no,
    dues.relatedProjectId as related_project_id,
    dues.relatedDate      as related_date,
    project_id            as project_id,
    import_id             as import_id
  from dim.project_due_bill_no_json
  lateral view explode(due_bill_no) due_bill as dues
  where 1 > 0
    and project_id = '${exe_id}'
    and row_type != 'delete'
) as project_due
left join (
  select
    product_code as product_id,
    due_bill_no
  from stage.ecas_loan
  where 1 > 0
    and d_date between date_sub(current_date(),3) and current_date()
    and p_type in (
      'ddht',
      -- 'htgy',
      ''
    )
) as loan_info
on project_due.due_bill_no = loan_info.due_bill_no
-- limit 10
;
