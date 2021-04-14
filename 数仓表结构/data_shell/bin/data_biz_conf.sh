#!/bin/bash

. ${data_manage_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

biz_conf_file=$file_import/dim_new.biz_conf.tsv
biz_conf_hdfs=/user/hive/warehouse/dim_new.db/biz_conf

case $(ifconfig | grep -Po 'inet[ ]\K[^ ]+' | grep -v '127') in
  (10.80.* ) mysql_host='10.80.16.87' mysql_port='3306' mysql_user='root' mysql_pass='wH7Emvsrg&V5'     mysql_db='ueagle'     ;;
  (10.83.* ) mysql_host='10.83.16.43' mysql_port='3306' mysql_user='root' mysql_pass='zU!ykpx3EG)$$1e6' mysql_db='ueagle-dev' ;;
esac

temp_file=$(mktemp -t biz_conf.XXXXXX) || exit 1

echo '拉取 biz_conf 数据 开始'
$beeline --showHeader=false --outputformat=tsv2 -e '
select * from (
  select
    max(if(col_name = "biz_name",          col_val,null)) as biz_name,
    max(if(col_name = "biz_name_en",       col_val,null)) as biz_name_en,
    max(if(col_name = "capital_id",        col_val,null)) as capital_id,
    max(if(col_name = "capital_name",      col_val,null)) as capital_name,
    max(if(col_name = "capital_name_en",   col_val,null)) as capital_name_en,
    max(if(col_name = "channel_id",        col_val,null)) as channel_id,
    max(if(col_name = "channel_name",      col_val,null)) as channel_name,
    max(if(col_name = "channel_name_en",   col_val,null)) as channel_name_en,
    max(if(col_name = "trust_id",          col_val,null)) as trust_id,
    max(if(col_name = "trust_name",        col_val,null)) as trust_name,
    max(if(col_name = "trust_name_en",     col_val,null)) as trust_name_en,
    max(if(col_name = "project_id",        col_val,null)) as project_id,
    max(if(col_name = "project_name",      col_val,null)) as project_name,
    max(if(col_name = "project_name_en",   col_val,null)) as project_name_en,
    max(if(col_name = "project_amount",    col_val,null)) as project_amount,
    max(if(col_name = "product_id",        col_val,null)) as product_id,
    max(if(col_name = "product_name",      col_val,null)) as product_name,
    max(if(col_name = "product_name_en",   col_val,null)) as product_name_en,
    max(if(col_name = "product_id_vt",     col_val,null)) as product_id_vt,
    max(if(col_name = "product_name_vt",   col_val,null)) as product_name_vt,
    max(if(col_name = "product_name_en_vt",col_val,null)) as product_name_en_vt
  from dim.data_conf
  where col_type = "ac"
  group by col_id
) as tmp
where 1 > 0
  and is_empty(product_id_vt) is not null
  and is_empty(project_id)    is not null
  and is_empty(channel_id)    is not null
;' 2> /dev/null > $temp_file
echo '拉取 biz_conf 数据 结束'


echo '加载 biz_conf 数据 到 MySQL 开始'
mysql -h${mysql_host} -P${mysql_port} -u${mysql_user} -p${mysql_pass} -D${mysql_db} -e "
truncate table t_biz_conf;
LOAD DATA LOCAL INFILE '$temp_file' INTO TABLE t_biz_conf
FIELDS TERMINATED BY '\t'
;"
echo '加载 biz_conf 数据 到 MySQL 结束'

trap $(rm -f $temp_file) 1 2 9 15 19 20
