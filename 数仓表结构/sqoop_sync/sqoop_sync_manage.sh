#! /bin/bash

. /etc/profile
. ~/.bash_profile
export LANG=zh_CN.UTF-8

###############盒子数据过滤###############
hz_data='("000015644920921admin000186000005","000015645393481admin000106000005","000015645707031admin000186000008","000015647199671admin000186000008","000015647355571admin000006000008",
"000015651383251admin000186000008","000015651753381admin000186000008","000015651767611admin000186000008","000015651878811admin000186000008","000015652277751admin000006000008",
"000015652307771admin000106000008","000015652336571admin000106000008","000015652378551admin000106000008","000015652396011admin000006000008","000015652830991admin000006000008",
"000015653089641admin000186000008","000015653156791admin000186000008","000015653292571admin000186000008","000015653424571admin000006000008","000015654268701admin000006000008",
"000015654698111admin000106000008","000015655096171admin000006000008","000015655718851admin000006000008","000015655769541admin000006000008","000015657057971admin000186000008",
"000015657525991admin000006000008","000015657531091admin000186000008","000015657599921admin000186000008","000015657604121admin000186000008","000015657989941admin000186000008",
"000015659266791admin000106000008","000015659300441admin000186000008","000015661270421admin000006000008","000015661818021admin000006000008","000015662098061admin000106000008",
"000015662389301admin000106000008","000015663637631admin000106000008","000015664003531admin000006000008","000015664468001admin000006000008","000015664759271admin000106000008",
"000015665589091admin000106000008","000015665776251admin000106000008","000015666629441admin000006000008","000015668101841admin000186000008","000015668149541admin000006000008",
"000015668785841admin000106000008","000015678401481admin000006000008","000015679378831admin000006000008","000015679537231admin000106000008","000015679962111admin000186000008",
"000015679965661admin000106000008","000015680014271admin000186000008","000015680142191admin000006000008","000015680757631admin000106000008","000015680800881admin000186000008",
"000015680838631admin000186000008","000015680916491admin000006000008","000015681040271admin000186000008","000015681627031admin000106000008","000015681915071admin000186000008",
"000015682879941admin000186000008","000015683601641admin000106000008","000015684287461admin000106000008","000015684399031admin000186000008","000015685142431admin000186000008",
"000015685466431admin000006000008","000015686197261admin000106000008","000015686908261admin000186000008","000015687192661admin000106000008","000015687288051admin000006000008",
"000015687610231admin000186000008","000015687770481admin000106000008","000015689208771admin000106000008","000015689432701admin000106000008","000015689643471admin000006000008",
"000015690503391admin000006000008","000015691436481admin000106000008","000015692119381admin000106000000","000015693894961admin000186000008","000015694079601admin000006000008",
"000015694157551admin000106000008","000015694706231admin000006000008","000015695076901admin000106000011","000015695811591admin000106000011","000015697276121admin000186000001",
"000015697426901admin000006000008","000015697479091admin000106000011")'



###########sqoop同步数据到hive中############
sqoop_import(){
  echo $@
  [[ -e $base_dir/QueryResult.java ]] && rm -rf $base_dir/QueryResult.java
  # if [[ ${1} = 'Y' && ${2} = ${3} ]];then
  #   $beeline -e "alter table ${hive_db}.${tbl} drop partition(d_date = '${d_date}')"
  #   sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
  #   --connect "jdbc:mysql://${hostname}:3306/${database}?useUnicode=true&characterEncoding=utf8" \
  #   --username ${username} \
  #   --password ${password} \
  #   --query "select *,'${d_date}' as d_date from ${tbl} where acct_nbr not in ${hz_data} and \$CONDITIONS" \
  #   --hcatalog-database ${hive_db} --hcatalog-table ${tbl} \
  #   --hcatalog-storage-stanza "STORED AS parquet" \
  #   --split-by org \
  #   --null-string '\\N' --null-non-string '\\N' \
  #   -m 1
  # elif [[ ${1} = 'Y' && ${2} != ${3} ]];then
  #   $beeline -e "alter table ${hive_db}.${tbl} drop partition(d_date = '${d_date}')"
  #   sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
  #   --connect "jdbc:mysql://${hostname}:3306/${database}?useUnicode=true&characterEncoding=utf8" \
  #   --username ${username} \
  #   --password ${password} \
  #   --query "select *,'${d_date}' as d_date from ${tbl} where \$CONDITIONS" \
  #   --hcatalog-database ${hive_db} --hcatalog-table ${tbl} \
  #   --hcatalog-storage-stanza "STORED AS parquet" \
  #   --split-by org \
  #   --null-string '\\N' --null-non-string '\\N' \
  #   -m 1
  # else
  #   $beeline -e "truncate table ${hive_db}.${tbl}"
  #   sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
  #   --connect "jdbc:mysql://${hostname}:3306/${database}?useUnicode=true&characterEncoding=utf8" \
  #   --username ${username} \
  #   --password ${password} \
  #   --query "select * from ${tbl} where \$CONDITIONS" \
  #   --hcatalog-database ${hive_db} --hcatalog-table ${tbl} \
  #   --hcatalog-storage-stanza "STORED AS parquet" \
  #   --split-by org \
  #   --hive-overwrite \
  #   --null-string '\\N' --null-non-string '\\N' \
  #   -m 1
  # fi
}


during(){
  s_time=$(date -d "$1" +%s)
  e_time=$(date -d "$2" +%s)
  [[ $s_time > $e_time ]] && u=$(( $s_time - $e_time )) || u=$(( $e_time - $s_time ))
  s=$(( $u % 60 )) u=$(( $u / 60 ))
  m=$(( $u % 60 )) u=$(( $u / 60 ))
  h=$(( $u % 24 ))
  d=$(( $u / 24 ))
  printf '%d天%02d时%02d分%02d秒' $d $h $m $s
}


#################################################
#        连接 MySQL 的节点、库、表              #
#                                               #
#        时间格式：yyyy-MM-dd HH:mm:ss          #
#        第三参数：以","（英文逗号）分隔        #
#################################################

base_dir=$(dirname "${BASH_SOURCE[0]}")

echo -e "${start:=$(date +'%F %T')} 执行Sqoop数据抽取任务  开始"

[[ -z $1 ]] && {
  echo -e '  请输入参数。参数为要使用的环境配置脚本，即：
  \033[33msqoop_sync_conf_pro.sh\033[0m 或 \033[33msqoop_sync_conf_sit.sh\033[0m'
  exit 1
}

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

