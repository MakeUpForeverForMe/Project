set hivevar:project_id=
  select distinct project_id
  from dim.project_info
  where 1 > 0
;

set hivevar:bag_id=
  select distinct bag_id
  from dim.bag_info
  where 1 > 0
;
