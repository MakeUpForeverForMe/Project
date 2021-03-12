insert overwrite table dim_new.dim_idno
select
  cast(idprefix as string)                                                    as idno_addr,
  substring(cast(idprefix as string),1,1)                                     as idno_area,
  case substring(cast(idprefix as string),1,1)
  when '1' then '华北地区'
  when '2' then '东北地区'
  when '3' then '华东地区'
  when '4' then '中南地区'
  when '5' then '西南地区'
  when '6' then '西北地区'
  when '7' then '台湾地区'
  when '8' then '港澳台地区'
  else substring(cast(idprefix as string),1,1)
  end                                                                         as idno_area_cn,
  substring(cast(idprefix as string),1,2)                                     as idno_province,
  prv                                                                         as idno_province_cn,
  substring(cast(idprefix as string),1,4)                                     as idno_city,
  if(city is null or city='',prv,city)                                        as idno_city_cn,
  substring(cast(idprefix as string),1,6)                                     as idno_county,
  if(county is null or county='',if(city is null or city='',prv,city),county) as idno_county_cn
from default.t_idcard_prv_city_county
-- limit 10
;
