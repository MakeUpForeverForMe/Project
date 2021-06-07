set hivevar:product_id=
  select distinct product_id
  from (
    select
      max(if(col_name = 'channel_id',col_val,null)) as channel_id,
      max(if(col_name = 'product_id',col_val,null)) as product_id
    from dim.data_conf
    where col_type = 'ac'
    group by col_id
  ) as tmp
  where channel_id = '0012'
;
set hivevar:db_suffix=_cps;
set hivevar:tb_prefix=;
set hivevar:type=CAPITAL;

