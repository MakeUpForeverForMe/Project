#!/usr/bin/env bash

. ${sqoop_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


########### Sqoop 同步数据到 Hive 中 ############
sqoop_import(){
  debug "${hive_db}.${hive_tb} sqoop 导数 开始！"
  sqoop_fun_time=$curr_time

  if [[ ${is_partition} = y ]]; then
    sqoop_query="select *,'${ST9}' as d_date from ${mysql_tb} where \$CONDITIONS"
  elif [[ ${is_test} = y && ${sys_type} = xinglian && ${is_partition} = y ]]; then
    sqoop_query="
      select *,'${ST9}' as d_date from asset_status_test.${mysql_tb}   where \$CONDITIONS union all
      select *,'${ST9}' as d_date from asset_status_test10.${mysql_tb} where \$CONDITIONS union all
      select *,'${ST9}' as d_date from asset_status_test20.${mysql_tb} where \$CONDITIONS union all
      select *,'${ST9}' as d_date from asset_status_test30.${mysql_tb} where \$CONDITIONS union all
      select *,'${ST9}' as d_date from asset_status_test40.${mysql_tb} where \$CONDITIONS
    "
  elif [[ ${is_test} = y && ${sys_type} = xinglian && ${is_partition} = n ]]; then
    sqoop_query="
      select * from asset_status_test.${mysql_tb}   where \$CONDITIONS union all
      select * from asset_status_test10.${mysql_tb} where \$CONDITIONS union all
      select * from asset_status_test20.${mysql_tb} where \$CONDITIONS union all
      select * from asset_status_test30.${mysql_tb} where \$CONDITIONS union all
      select * from asset_status_test40.${mysql_tb} where \$CONDITIONS
    "
  else
    sqoop_query="select * from ${mysql_tb} where \$CONDITIONS"
  fi

  debug "${hive_db}.${hive_tb} 导数 SQL 为：${sqoop_query}！"

  sqoop_command="
    $sqoop import \
    -D 'mapreduce.map.memory.mb=4096' \
    -D 'mapreduce.reduce.memory.mb=4096' \
    -D 'org.apache.sqoop.splitter.allow_text_splitter=true' \
    --mapreduce-job-name '${hive_db}.${hive_tb} <—— ${mysql_db}.${mysql_tb} : ${ST9}' \
    --connect 'jdbc:mysql://${mysql_host}:3306/${mysql_db}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=convertToNull' \
    --username '${mysql_user}' --password '${mysql_pass}' \
    --query '${sqoop_query}' \
    --split-by '${incre_field}' \
    --null-string '\\\N' --null-non-string '\\\N' \
    --hcatalog-database '${hive_db}' --hcatalog-table '${hive_tb}' \
    --hcatalog-storage-stanza 'stored as parquet' \
    --num-mappers 1
  "
  debug "${hive_db}.${hive_tb} 导数 sqoop 命令 为：${sqoop_command}！"
  eval "${sqoop_command}" 2>&1 | tee -a $log_file

  [[ ${PIPESTATUS[0]} = 0 ]] && info "${hive_db}.${hive_tb} sqoop 导数 结束！用时：$(during "${sqoop_fun_time}")" || error "${hive_db}.${hive_tb} sqoop 导数 失败！用时：$(during "${sqoop_fun_time}")"
}

########### 创建表 ###########
create_table(){
  debug "执行 ${hive_db}.${hive_tb} 自动建表语句 开始！"

  create_command="python3 ${bin}/create_table.py '${hive_db}' '${hive_tb}' '${mysql_host}' '${mysql_user}' '${mysql_pass}' '${mysql_db}' '${mysql_tb}' '${is_partition}'"
  debug "${hive_db}.${hive_tb} 自动建表语句 执行命令为 ${create_command}！"

  eval ${create_command} 2>&1 | tee -a $log_file
  [[ ${PIPESTATUS[0]} = 0 ]] && debug "${hive_db}.${hive_tb} 建表语句创建 完成！" || error "${hive_db}.${hive_tb} 建表语句创建 失败！"
}

########### 在 Hive 中的表重命名 ###########
tabla_alias(){
  debug "对表 ${mysql_db}.${mysql_tb} 重命名！"
  case "${mysql_db}.${mysql_tb}" in
    # 测试相关表


    # 生产相关表
    ( asset_status.t_loan_contract_info      ) hive_tb=asset_01_t_loan_contract_info      incre_field='id' ;;
    ( asset_status.t_principal_borrower_info ) hive_tb=asset_02_t_principal_borrower_info incre_field='id' ;;
    ( asset_status.t_contact_person_info     ) hive_tb=asset_03_t_contact_person_info     incre_field='id' ;;
    ( asset_status.t_guaranty_info           ) hive_tb=asset_04_t_guaranty_info           incre_field='id' ;;
    ( asset_status.t_repayment_schedule      ) hive_tb=asset_05_t_repayment_schedule      incre_field='id' ;;
    ( asset_status.t_asset_pay_flow          ) hive_tb=asset_06_t_asset_pay_flow          incre_field='id' ;;
    ( asset_status.t_repayment_info          ) hive_tb=asset_07_t_repayment_info          incre_field='id' ;;
    ( asset_status.t_asset_disposal          ) hive_tb=asset_08_t_asset_disposal          incre_field='id' ;;
    ( asset_status.t_asset_supplement        ) hive_tb=asset_09_t_asset_supplement        incre_field='id' ;;
    ( asset_status.t_asset_check             ) hive_tb=asset_10_t_asset_check             incre_field='id' ;;
    ( asset_status.t_project_check           ) hive_tb=asset_11_t_project_check           incre_field='id' ;;
    ( asset_status.t_enterprise_info         ) hive_tb=asset_12_t_enterprise_info         incre_field='id' ;;
    ( asset_status.*                         ) hive_tb=asset_$mysql_tb                    incre_field='id' ;;

    ( abs-core.t_loancontractinfo            ) hive_tb=abs_01_t_loancontractinfo          incre_field='id' ;;
    ( abs-core.t_borrowerinfo                ) hive_tb=abs_02_t_borrowerinfo              incre_field='id' ;;
    ( abs-core.t_associatesinfo              ) hive_tb=abs_03_t_associatesinfo            incre_field='id' ;;
    ( abs-core.t_guarantycarinfo             ) hive_tb=abs_04_t_guarantycarinfo           incre_field='id' ;;
    ( abs-core.t_repaymentplan               ) hive_tb=abs_05_t_repaymentplan             incre_field='id' ;;
    ( abs-core.t_repaymentplan_history       ) hive_tb=abs_05_t_repaymentplan_history     incre_field='id' ;;
    ( abs-core.t_assettradeflow              ) hive_tb=abs_06_t_assettradeflow            incre_field='id' ;;
    ( abs-core.t_actualrepayinfo             ) hive_tb=abs_07_t_actualrepayinfo           incre_field='id' ;;
    ( abs-core.t_assetdealprocessinfo        ) hive_tb=abs_08_t_assetdealprocessinfo      incre_field='id' ;;
    ( abs-core.t_assetaddtradeinfo           ) hive_tb=abs_09_t_assetaddtradeinfo         incre_field='id' ;;
    ( abs-core.t_assetaccountcheck           ) hive_tb=abs_10_t_assetaccountcheck         incre_field='id' ;;
    ( abs-core.t_projectaccountcheck         ) hive_tb=abs_11_t_projectaccountcheck       incre_field='id' ;;
    ( abs-core.t_enterpriseinfo              ) hive_tb=abs_12_t_enterpriseinfo            incre_field='id' ;;
    ( abs-core.t_guarantyhouseinfo           ) hive_tb=abs_13_t_guarantyhouseinfo         incre_field='id' ;;
    ( abs-core.*                             ) hive_tb=abs_$mysql_tb                      incre_field='id' ;;


    # 其他相关表
    ( * )                                      hive_tb=$mysql_tb                          incre_field=''   ;;
  esac
  debug "对表 ${mysql_db}.${mysql_tb} 重命名为 Hive 中的表名 ：${hive_db}.${hive_tb}！"
}

mysql_item(){
  debug "根据 是否是测试环境（${is_test}） 和 系统类型（${sys_type}） 获取 MySQL 相关属性！"
  if [[ ${is_test} = n ]]; then
    case ${sys_type} in
      ( xinglian ) mysql_host=10.80.16.5 mysql_user=root mysql_pass='Xfx2018@)!*' mysql_db=asset_status ;;
      ( abs      ) mysql_host=10.80.16.21 mysql_user=sqoop_user mysql_pass='xy@Eh93AmnCkMbiU' mysql_db=abs-core ;;
      ( *        ) error "'${sys_type}' 是未定义的系统类型！" ;;
    esac
  elif [[ ${is_test} = y ]]; then
    case ${sys_type} in
      ( xinglian ) mysql_host=10.83.16.43 mysql_user=root mysql_pass='zU!ykpx3EG)$$1e6' mysql_db=asset_status ;;
      ( abs      ) mysql_host=10.83.16.15 mysql_user=root mysql_pass='Ws2018!07@' mysql_db=abs-core ;;
      ( *        ) error "'${sys_type}' 是未定义的系统类型！" ;;
    esac
  fi
}

execute_fun(){
  hive_db=stage
  data_warehouse=${cos_uri}${warehouse}

  [[ $# != 2 && $# != 3 ]] && error "参数个数不正确！参数一：系统类型，参数二：MySQL 表名，参数三（可选，默认n）：是否是分区表（y或n）！"

  sys_type=${1}
  mysql_tb=${2}
  [[ -z ${sys_type} || -z ${mysql_tb} ]] && error "系统类型（参数一）或 MySQL 表名（参数二）为空！"

  is_partition=$(echo ${3:-n} | tr 'A-Z' 'a-z')
  [[ ${is_partition} != y && ${is_partition} != n ]] && error "输入的 是否是分区表 的参数错误！"

  mysql_item

  log_file=$log/sqoop/$(basename "${BASH_SOURCE[0]}")-${mysql_tb}-${ST9:=$(date -d '-1 day' +%F)}.log

  info "${hive_db}.${hive_tb} Sqoop 抽数 开始！"
  sqoop_s_time=$curr_time

  debug "$(printf '抽数系统(%-10s)  抽数表(%-27s)  在 Hive 中是否是分区表(%s)' "'${sys_type}'" "'${mysql_tb}'" "'${is_partition}'")"

  debug "将 MySQL 的表转为 Hive 的表！"
  tabla_alias

  debug "判断是否需要执行自动创建表 ${hive_db}.${hive_tb}！"
  table_exists_command="$beeline --showHeader=false --outputformat=csv2 -e 'show tables in ${hive_db} like \"${hive_tb}\";'"
  debug "判断命令为：${table_exists_command}！"
  [[ -z $(eval ${table_exists_command}) ]] && {
    debug "${hive_db}.${hive_tb} 表不存在，执行创建表 开始！"
    create_table
  } || debug "${hive_db}.${hive_tb} 表已存在！"


  hdfs_rm_command="$hdfs -rm -r -skipTrash ${data_warehouse}/${hive_db}.db/${hive_tb}$([[ ${is_partition} = y ]] && echo "/d_date=${ST9}" || echo '')/*"
  debug "${hive_db}.${hive_tb} 删除 HDFS 中的文件命令为：${hdfs_rm_command}！"
  eval ${hdfs_rm_command} 2>&1 | tee -a $log_file
  [[ ${PIPESTATUS[0]} = 0 ]] && debug "${hive_db}.${hive_tb} 删除 HDFS 中的文件 完成！" || warn "${hive_db}.${hive_tb} 删除 HDFS 中的文件 失败！"

  info "${hive_db}.${hive_tb} 正式执行 sqoop 导数任务 开始！"
  sqoop_import

  info "${hive_db}.${hive_tb} Sqoop 抽数 结束！用时：$(during "${sqoop_s_time}")"
}



arg_list=(
  'xinglian-t_loan_contract_info'
  'xinglian-t_principal_borrower_info'
  'xinglian-t_contact_person_info'
  'xinglian-t_guaranty_info'
  'xinglian-t_repayment_schedule-y'
  'xinglian-t_asset_pay_flow'
  'xinglian-t_repayment_info'
  'xinglian-t_asset_disposal'
  'xinglian-t_asset_supplement'
  'xinglian-t_asset_check'
  'xinglian-t_project_check'
  'xinglian-t_enterprise_info'

  # 'abs-t_loancontractinfo'
  # 'abs-t_borrowerinfo'
  # 'abs-t_associatesinfo'
  # 'abs-t_guarantycarinfo'
  # 'abs-t_repaymentplan'
  # 'abs-t_repaymentplan_history'
  # 'abs-t_assettradeflow'
  # 'abs-t_actualrepayinfo'
  # 'abs-t_assetdealprocessinfo'
  # 'abs-t_assetaddtradeinfo'
  # 'abs-t_assetaccountcheck'
  # 'abs-t_projectaccountcheck'
  # 'abs-t_enterpriseinfo'
  # 'abs-t_guarantyhouseinfo'
)

p_num=2

if [[ $# != 0 ]]; then
  execute_fun $@
else
  for arg in ${arg_list[@]}; do
    execute_fun $(b_a ${arg}) &
    p_opera ${p_num}
  done
fi
