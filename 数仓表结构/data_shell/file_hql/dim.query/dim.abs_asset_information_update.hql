-- 封包成功后将包状态由封包中更新为已封包
insert overwrite table dim_new.bag_info partition(bag_id = '${bag_id}')
select
  project_id,
  bag_name,
  '已封包' as bag_status,
  bag_remain_principal,
  bag_date,
  insert_date
from dim_new.bag_info where bag_id = '${bag_id}';
