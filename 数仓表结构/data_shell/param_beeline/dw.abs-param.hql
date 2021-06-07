set hivevar:project_id=
  select distinct project_id
  from dim.project_info
  join (
    select distinct project_id as project_id_unrelated
    from dim.project_due_bill_no
    where related_project_id is null
  ) as related_project
  on project_info.project_id = related_project.project_id_unrelated
;
