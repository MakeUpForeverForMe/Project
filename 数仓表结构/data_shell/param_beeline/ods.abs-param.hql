set hivevar:db_suffix=;
set hivevar:product_id=
  select distinct project_id
  from dim.project_info
  where 1 > 0
    and project_id not in (
      '001601',           -- 汇通
      'WS0005200001',     -- 瓜子
      'CL202012280092',   -- 汇通国银
      'DIDI201908161538', -- 滴滴
      ''
    )
;
