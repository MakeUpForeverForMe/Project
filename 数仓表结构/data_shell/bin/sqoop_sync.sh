#! /bin/bash

. ${data_sqoop_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


########### Sqoop 同步数据到 Hive 中 ############
sqoop_import(){
  sudo -u hive sqoop import \
  -D "mapreduce.map.memory.mb=4096" \
  -D "mapreduce.reduce.memory.mb=4096" \
  -D "org.apache.sqoop.splitter.allow_text_splitter=true" \
  --mapreduce-job-name "mapred.job.name='${hive_database}.${hive_tablname} <—— ${mysql_database}.${mysql_tablname} : ${ST9}'" \
  --connect "jdbc:mysql://${mysql_hostname}:3306/${mysql_database}?useUnicode=true&characterEncoding=utf8" \
  --username "${mysql_username}" --password "${mysql_password}" \
  --query "${sql}" \
  --split-by "${split_str}" \
  --null-string '\\N' --null-non-string '\\N' \
  --hcatalog-database "${hive_database}" --hcatalog-table "${hive_tablname}" \
  --hcatalog-storage-stanza 'stored as parquet' \
  --num-mappers 1
}

########### 抽数 #############
#         增量全表           #
#         增量分区           #
#         全量全表           #
#         全量分区           #
##############################


[[ -z $1 ]] && {
  echo -e '  请输入参数。参数为要使用的环境配置脚本，即：
  \033[33m sqoop_sync_conf_pro.sh \033[0m 或 \033[33m sqoop_sync_conf_sit.sh \033[0m'
  exit 1
}

echo -e "${date_s_aa:=$(date +'%F %T')} Sqoop 抽数  开始 当前脚本进程ID为：$(pid)" #&>> $log

. $1
if [[ $1 =~ pro.sh ]]; then
  sqoop_param='10.80.16.9:ccsdb'
elif [[ $1 =~ sit.sh ]]; then
  sqoop_param='10.83.16.43:ccs_t_30'
fi

# 日期格式：yyyy-MM-dd HH:mm:ss
d_date=$(date -d "${2:-$(date +'%F %T')}" +%F)
last_time=$3
# 传入重跑表的格式："hostname:库名:表名" ，多个以","（英文逗号）分隔
tables_param=$4

beeline="beeline -u jdbc:hive2://$hiveserver2:10000 -n hive"

for connect_keys in ${tables_param:-${!connects[@]}}; do
  connect_key=(${connect_keys//:/ })
  connect_val=(${connects["${connect_keys}"]//:/ })

  hostname=${connect_key[0]}
  database=${connect_key[1]}

  username=${connect_val[0]}
  password=${connect_val[1]}

  tables=${connect_val[2]}

  # printf '%-16s%-10s%-20s%-15s%s\n' $hostname $username $password $database $tables

  ###########获取当前mysql表记录数############
  for table in ${tables//,/ }; do
    tbl=${table//-/}

    # printf '%-25s%-10s\n' $table $tbl

    mysql_count=$(mysql -h${hostname} -P3306 -u${username} -p${password} -D${database} -s -N -e "select count(1) as cnt from ${tbl};")

    ###########sqoop同步数据到hive中############
    if [[ ${mysql_count:=-1} > 0 ]]; then
      sqoop_import "$([[ $table =~ -$ ]] && echo 'N' || echo 'Y')" "${connect_keys}" "${sqoop_param}"

      [[ $? -eq 0 ]] && {
        ###########获取同步到hive中的记录数###########
        [[ $table =~ -$ ]] && \
        sql="select count(1) as cnt from ${hive_db}.${tbl};" || \
        sql="select count(1) as cnt from ${hive_db}.${tbl} where d_date = ${d_date};"

        # hive_count=$($beeline \
        # --showHeader=false --outputformat=csv2 \
        # -e "${sql}")
      } || hive_count=-1
    fi

    ss=$(printf "Sqoop_sync.sh --> %-50s MySQL中数据量为：${mysql_count}，Hive分区 ${d_date}（如表中无分区，则为全量数据）中数据量为：${hive_count}" ${hostname}:${database}:${table})

    ###########比较记录数大小，判断数据同步是否成功############
    if [[ ${mysql_count} -eq -1 ]];then
      echo -e "$ss\t因为 MySQL 数据量为 -1，所以 MySQL 端存在问题，请检查错误并重新执行！"
    elif [[ ${mysql_count} -eq 0 ]];then
      echo -e "$ss\t因为 MySQL 数据量为：0，所以跳过 Sqoop 抽数执行！"
    elif [[ ${hive_count} -eq -1 ]]; then
      echo -e "$ss\t因为 Hive 数据量为：-1，所以 Hive 端存在问题，请检查错误并重新执行！"
    elif [[ ${hive_count} -eq 0 ]];then
      echo -e "$ss\t因为 Hive 数据量为：0，所以 Sqoop 数据抽取失败，请检查错误并重新执行！"
    elif [[ ${hive_count} -eq ${mysql_count} ]];then
      echo -e "$ss\t因为 MySQL 与 Hive 的数据量相等，所以同步成功！"
    else
      echo -e "$ss\t因为 MySQL 与 Hive 的数据量不一致，请查明原因后重新执行！"
    fi
  done
    # exit
  done

  echo -e "${end:=$(date +'%F %T')} 执行Sqoop数据抽取任务  结束      用时：$(during "$end" "$start")\n\n"

