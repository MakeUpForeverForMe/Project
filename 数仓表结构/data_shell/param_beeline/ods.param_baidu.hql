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
set hivevar:db_suffix=;
set hivevar:tb_prefix=asset_;
set hivevar:type=ASSET;
