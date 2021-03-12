#! /bin/bash

. ${data_sqoop_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/conf_env/env-dev.sh
. $lib/function.sh

sqoop_import(){
  sudo -u hive sqoop import \
  -D "mapreduce.map.memory.mb=4096" \
  -D "mapreduce.reduce.memory.mb=4096" \
  -D "org.apache.sqoop.splitter.allow_text_splitter=true" \
  --mapreduce-job-name "mapred.job.name='${hive_db_tb} <—— ${mysql_db}.${mysql_tb} : ${ST9}'" \
  --connect "jdbc:mysql://${mysql_host}:3306/${mysql_db}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=convertToNull" \
  --username "${mysql_user}" --password "${mysql_pass}" \
  --query "${import_sql}" \
  --split-by "${split_str}" \
  --null-string '\\N' --null-non-string '\\N' \
  --hcatalog-database "${hive_db}" --hcatalog-table "${hive_tb}" \
  --hcatalog-storage-stanza 'stored as parquet' \
  --num-mappers 1
}



ST9='2021-02-02'

mysql_abs=$(data_conf "mysql" "abs" "root")

mysql_host=$(echo "$mysql_abs" | jq -r '.host')
mysql_user=$(echo "$mysql_abs" | jq -r '.user')
mysql_pass=$(echo "$mysql_abs" | jq -r '.pass')

import_abs=$(data_conf "import" "abs" "abs-core~default")

mysql_db=$(echo "$import_abs" | jq -r ".mysql_db")
hive_db=$(echo "$import_abs" | jq -r ".hive_db")


for (( j = 0; j < $(echo "$import_abs" | jq -r ".tables | length"); j++ )); do
  mysql_tb="$(echo "$import_abs" | jq -r ".tables[$j].mysql_tb")"
  hive_tb="$(echo "$import_abs" | jq -r ".tables[$j].hive_tb")"
  hive_db_tb=${hive_db}.${hive_tb}

  offset=$(data_conf 'offset' "' or 1 = '1" "${mysql_tb}~${hive_tb}" | jq -r ".offset" | sort | tail -n 1)
  offset_id="$(echo "$import_abs" | jq -r ".tables[$j].offset_id" | sed "s/null//g;")"
  split_str="$(echo "$import_abs" | jq -r ".tables[$j].split_str")"

  partition_create="$(echo "$import_abs" | jq -r ".tables[$j].partition_create" | sed "s/null//g;")"

  truncate_hql="$(echo "$import_abs" | jq -r ".tables[$j].truncate_hql" | sed "s/null//g; s/\${ST9}/${ST9}/g;")"

  import_sql="$(echo "$import_abs" | jq -r ".tables[$j].import_sql" | sed "s/null//g; s/\${ST9}/${ST9}/g; s/\${offset}/${offset}/g;")"

  [[ -n "${offset_id}" ]] && truncate_hql=''


  table_exists=$($beeline --outputformat=tsv2 --showHeader=false -e "show tables from ${hive_db} like '${hive_tb}'")

  if [[ -z $table_exists ]]; then
    $python $bin/create_table.py "${mysql_host}" "${mysql_user}" "${mysql_pass}" "${mysql_db}" "${mysql_tb}" "${hive_db}" "${hive_tb}" "${partition_create}"
    $beeline -f "$file_hql/create.query/create.${hive_db_tb}.hql"
  fi

  $beeline -e "$truncate_hql"

  sqoop_import

  [[ -n "${offset_id}" ]] && {
    offset=$($beeline --outputformat=tsv2 --showHeader=false --nullemptystring=true  -e "select max(${offset_id}) from ${hive_db_tb}")
    date_conf_update_offset 'offset' "${ST9}" "${mysql_tb}~${hive_tb}" '{"offset": '"${offset:-0}"' }'
  }

  exit
done

exit
