#bin/bash -e
now=$(date "+%Y-%m-%d %H:%M:%S")
# 风控舆情抽数到星象
echo 执行日期为:$now
mysql -P3306 -h'10.80.16.48' -uroot -p'Xfx2018@)!*' -Dreptile -s -N -e "
select
  id             as id,
  case
  when keyword = '乐信' then '0006'
  when keyword = '分期乐' then '0006'
  when keyword = '乐信（北京）网络科技有限公司' then '0006'
  when keyword like '%汇通信诚%' then '0003'
  when keyword like '%广汽%'     then '0007'
  when keyword like '%易鑫%'     then '0010'
  when keyword like '%滴滴%'     then '10000'
  when keyword like '%天津恒通%' then '天津恒通'
  when keyword like '%先锋太盟%' then '先锋太盟'
  else '' end as channel_id,
  IFNULL(keyword,'')        as keyword,
  IFNULL(is_effective,'')   as is_effective,
  IFNULL(url,'')            as url,
  IFNULL(title,'')          as title,
  IFNULL(release_time,'')   as release_time,
  IFNULL(emotion_type,'')   as emotion_type,
  IFNULL(emotion_score,'')  as emotion_score,
  IFNULL(create_time,'')    as create_time,
  IFNULL(update_time,'')    as update_time,
  IFNULL(tag,'')            as tag
from ca_urls_email
where create_time >= '2020-01-01 00:00:00.0'
and (keyword like '%乐%'
or keyword like '%汇通信诚%'
or keyword like '%广汽%'
or keyword like '%易鑫%'
or keyword like '%滴滴%'
or keyword like '%天津恒通%'
or keyword like '%先锋太盟%'
)
;" > eagle_risk_news.tsv

echo '抽数完成！'

mysql -P3306 -h'10.80.16.73' -u'hadoop' -p'XFfRjfWFWOki^kU0' -D'dm_eagle' -e "LOAD DATA LOCAL INFILE 'eagle_risk_news.tsv' ignore INTO TABLE eagle_risk_news FIELDS TERMINATED BY '\t' (id,channel_id,keyword,is_effective,url,title,release_time,emotion_type,emotion_score,create_time,update_time,tag)"

rm -f eagle_risk_news.tsv

echo 结束时间为:$(date "+%Y-%m-%d %H:%M:%S")
