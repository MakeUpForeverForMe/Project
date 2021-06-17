#!/usr/bin/env bash

. ${sqoop_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


send_mail(){
  $mail $pm_rd "${1} $(pid)" "
    执行抽数日期： $ST9
    执行开始时间： $sqoop_time
    执行结束时间： $curr_time
    执行执行时长： $during_time
  " 2>&1 | tee -a $log_file
}

########### 创建表 ###########
create_table(){
  debug "执行 ${hive_db}.${hive_tb} 自动建表语句 开始！"

  create_command="python3 ${bin}/create_table.py '${hive_db}' '${hive_tb}' '${mysql_host}' '${mysql_user}' '${mysql_pass}' '${mysql_db}' '${mysql_tb}' '${is_partition}'"
  debug "${hive_db}.${hive_tb} 自动建表语句 执行命令为 ${create_command}！"

  eval ${create_command} 2>&1 | tee -a $log_file
  [[ ${PIPESTATUS[0]} = 0 ]] && debug "${hive_db}.${hive_tb} 建表语句创建 完成！" || error "${hive_db}.${hive_tb} 建表语句创建 失败！"
}

########### Sqoop 同步数据到 Hive 中 ############
sqoop_import(){
  debug "${hive_db}.${hive_tb} sqoop 导数 开始！"
  sqoop_fun_time=$curr_time

  if [[ ${is_test} = y && ${sys_type} = xinglian && ${is_partition} = y ]]; then
      # select *,\"${ST9}\" as d_date from ${mysql_tb}   where \$CONDITIONS union all
    sqoop_query="
      select *,\"${ST9}\" as d_date from ${mysql_tb} where \$CONDITIONS
    "
  elif [[ ${is_test} = y && ${sys_type} = xinglian && ${is_partition} = n ]]; then
      # select * from ${mysql_tb}   where \$CONDITIONS union all
    sqoop_query="
      select * from ${mysql_tb} where \$CONDITIONS
    "
  elif [[ ${is_partition} = y ]]; then
    sqoop_query="select *,\"${ST9}\" as d_date from ${mysql_tb} where \$CONDITIONS"
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
    --num-mappers ${sqoop_num_mappers:-1}
  "
  debug "${hive_db}.${hive_tb} 导数 sqoop 命令 为：${sqoop_command}！"
  eval "${sqoop_command}" 2>&1 | tee -a $log_file

  [[ ${PIPESTATUS[0]} = 0 ]] && {
    info "${hive_db}.${hive_tb} sqoop 导数 结束！用时：$(during "${sqoop_fun_time}")"
    return 0
  } || {
    warn "${hive_db}.${hive_tb} sqoop 导数 失败！用时：$(during "${sqoop_fun_time}")"
    return 1
  }
}

########### 在 Hive 中的表重命名 ###########
tabla_alias(){
  debug "对表 ${mysql_db}.${mysql_tb} 重命名！"
  case "${sys_type}.${mysql_tb}" in
    ( ${sys_01}.t_loan_contract_info      ) hive_tb=asset_01_t_loan_contract_info      incre_field='id' ;;
    ( ${sys_01}.t_principal_borrower_info ) hive_tb=asset_02_t_principal_borrower_info incre_field='id' ;;
    ( ${sys_01}.t_contact_person_info     ) hive_tb=asset_03_t_contact_person_info     incre_field='id' ;;
    ( ${sys_01}.t_guaranty_info           ) hive_tb=asset_04_t_guaranty_info           incre_field='id' ;;
    ( ${sys_01}.t_repayment_schedule      ) hive_tb=asset_05_t_repayment_schedule      incre_field='id' ;;
    ( ${sys_01}.t_asset_pay_flow          ) hive_tb=asset_06_t_asset_pay_flow          incre_field='id' ;;
    ( ${sys_01}.t_repayment_info          ) hive_tb=asset_07_t_repayment_info          incre_field='id' ;;
    ( ${sys_01}.t_asset_disposal          ) hive_tb=asset_08_t_asset_disposal          incre_field='id' ;;
    ( ${sys_01}.t_asset_supplement        ) hive_tb=asset_09_t_asset_supplement        incre_field='id' ;;
    ( ${sys_01}.t_asset_check             ) hive_tb=asset_10_t_asset_check             incre_field='id' sqoop_num_mappers=5 ;;
    ( ${sys_01}.t_project_check           ) hive_tb=asset_11_t_project_check           incre_field='id' ;;
    ( ${sys_01}.t_enterprise_info         ) hive_tb=asset_12_t_enterprise_info         incre_field='id' ;;
    ( ${sys_01}.*                         ) hive_tb=asset_$mysql_tb                    incre_field='id' ;;

    ( ${sys_02}.t_loancontractinfo      ) hive_tb=abs_01_t_loancontractinfo      incre_field='id' ;;
    ( ${sys_02}.t_borrowerinfo          ) hive_tb=abs_02_t_borrowerinfo          incre_field='id' ;;
    ( ${sys_02}.t_associatesinfo        ) hive_tb=abs_03_t_associatesinfo        incre_field='id' ;;
    ( ${sys_02}.t_guarantycarinfo       ) hive_tb=abs_04_t_guarantycarinfo       incre_field='id' ;;
    ( ${sys_02}.t_repaymentplan         ) hive_tb=abs_05_t_repaymentplan         incre_field='id' ;;
    ( ${sys_02}.t_repaymentplan_history ) hive_tb=abs_05_t_repaymentplan_history incre_field='id' ;;
    ( ${sys_02}.t_assettradeflow        ) hive_tb=abs_06_t_assettradeflow        incre_field='id' ;;
    ( ${sys_02}.t_actualrepayinfo       ) hive_tb=abs_07_t_actualrepayinfo       incre_field='id' ;;
    ( ${sys_02}.t_assetdealprocessinfo  ) hive_tb=abs_08_t_assetdealprocessinfo  incre_field='id' ;;
    ( ${sys_02}.t_assetaddtradeinfo     ) hive_tb=abs_09_t_assetaddtradeinfo     incre_field='id' ;;
    ( ${sys_02}.t_assetaccountcheck     ) hive_tb=abs_10_t_assetaccountcheck     incre_field='id' sqoop_num_mappers=5 ;;
    ( ${sys_02}.t_projectaccountcheck   ) hive_tb=abs_11_t_projectaccountcheck   incre_field='id' ;;
    ( ${sys_02}.t_enterpriseinfo        ) hive_tb=abs_12_t_enterpriseinfo        incre_field='id' ;;
    ( ${sys_02}.t_guarantyhouseinfo     ) hive_tb=abs_13_t_guarantyhouseinfo     incre_field='id' ;;
    ( ${sys_02}.*                       ) hive_tb=abs_$mysql_tb                  incre_field='id' ;;

    ( * ) hive_tb=$mysql_tb incre_field='' ;;
  esac
  debug "对表 ${mysql_db}.${mysql_tb} 重命名为 Hive 中的表名 ：${hive_db}.${hive_tb}！"
}

mysql_item(){
  debug "根据 是否是测试环境（${is_test}） 和 系统类型（${sys_type}） 获取 MySQL 相关属性！"
  if [[ ${is_test} = n ]]; then
    case ${sys_type} in
      ( ${sys_01} ) mysql_host=10.80.16.5 mysql_user=root mysql_pass='Xfx2018@)!*' mysql_db=asset_status ;;
      ( ${sys_02} ) mysql_host=10.80.16.21 mysql_user=sqoop_user mysql_pass='xy@Eh93AmnCkMbiU' mysql_db=abs-core ;;
      ( *         ) error "'${sys_type}' 是未定义的系统类型！" ;;
    esac
  elif [[ ${is_test} = y ]]; then
    case ${sys_type} in
      ( ${sys_01} ) mysql_host=10.83.16.43 mysql_user=root mysql_pass='zU!ykpx3EG)$$1e6' mysql_db=asset_status ;;
      ( ${sys_02} ) mysql_host=10.83.16.15 mysql_user=root mysql_pass='Ws2018!07@' mysql_db=abs-core ;;
      ( *         ) error "'${sys_type}' 是未定义的系统类型！" ;;
    esac
  fi
}

execute_fun(){
  hive_db=stage
  data_warehouse=${cos_uri}${warehouse}
  sys_01=xinglian
  sys_02=abs

  [[ $# != 2 && $# != 3 ]] && error "参数个数不正确！参数一：系统类型，参数二：MySQL 表名，参数三（可选，默认n）：是否是分区表（y或n）！"

  sys_type=${1}
  mysql_tb=${2}
  [[ -z ${sys_type} || -z ${mysql_tb} ]] && error "系统类型（参数一）或 MySQL 表名（参数二）为空！"

  is_partition=$(echo ${3:-n} | tr 'A-Z' 'a-z')
  [[ ${is_partition} != y && ${is_partition} != n ]] && error "输入的 是否是分区表 的参数错误！"

  mysql_item
  tabla_alias

  log_file=$log/sqoop/$(basename "${BASH_SOURCE[0]}")-${hive_db}.${hive_tb}-${ST9:=$(date -d '-1 day' +%F)}.log

  debug "所有参数为：$@！"

  info "${hive_db}.${hive_tb} Sqoop 抽数 开始！"
  sqoop_s_time=$curr_time

  debug "$(printf '抽数系统(%-10s)  抽数表(%-27s)  在 Hive 中是否是分区表(%s)' "'${sys_type}'" "'${mysql_tb}'" "'${is_partition}'")"

  debug "判断是否需要执行自动创建表 ${hive_db}.${hive_tb}！"
  table_exists_command="$beeline --showHeader=false --outputformat=csv2 -e 'show tables in ${hive_db} like \"${hive_tb}\";'"
  debug "判断命令为：${table_exists_command}！"
  [[ -z $(eval ${table_exists_command}) ]] && {
    debug "${hive_db}.${hive_tb} 表不存在，执行创建表 开始！"
    create_table
  } || debug "${hive_db}.${hive_tb} 表已存在！"



  while [[ (${mysql_data_count:-0} != ${hive_data_count:-1}) && (${retry_i:=0} -le ${retry_num:=5}) ]]; do
    if [[ ${retry_i} -gt 0 ]]; then
      info "MySQL 表 ${mysql_db}.${mysql_tb} 的数据量为：${mysql_data_count} 与 Hive ${hive_db}.${hive_tb} 的数据量为：${hive_data_count}，两者不相等，重试第 ${retry_i} 次（默认 ${retry_num} 次）！"
    fi

    [[ ${is_partition} = y ]] && {
      drop_hive_partition_command="$beeline -e 'alter table ${hive_db}.${hive_tb} drop if exists partition (d_date = \"${ST9}\")';"
      debug "${hive_db}.${hive_tb} 是分区表，需要删除分区 ${ST9}，执行命令为：${drop_hive_partition_command}！"
      eval "${drop_hive_partition_command}" 2>&1 | tee -a $log_file
      [[ ${PIPESTATUS[0]} = 0 ]] && debug "${hive_db}.${hive_tb} 删除 分区 ${ST9} 完成！" || warn "${hive_db}.${hive_tb} 删除 分区 ${ST9} 失败！"
    }

    hdfs_rm_command="$hdfs -rm -r ${data_warehouse}/${hive_db}.db/${hive_tb}$([[ ${is_partition} = y ]] && echo "/d_date=${ST9}" || echo '')/*"
    debug "${hive_db}.${hive_tb} 删除 HDFS 中的文件命令为：${hdfs_rm_command}！"
    eval "${hdfs_rm_command}" 2>&1 | tee -a $log_file
    [[ ${PIPESTATUS[0]} = 0 ]] && debug "${hive_db}.${hive_tb} 删除 HDFS 中的文件 完成！" || warn "${hive_db}.${hive_tb} 删除 HDFS 中的文件 失败！"


    mysql_count="$($mysql_cmd) -N -s -e 'select count(1) as cnt from ${mysql_tb};'"
    debug "计算 MySQL 中表 ${mysql_db}.${mysql_tb} 的数据量命令为：${mysql_count}！"
    mysql_data_count=$(eval "${mysql_count}")
    debug "MySQL 表 ${mysql_db}.${mysql_tb} 的数据量为：==========|${mysql_data_count}|==========！"


    info "${hive_db}.${hive_tb} 正式执行 sqoop 导数任务 开始！"
    sqoop_import
    [[ $? != 0 && ${retry_i} -gt 0 ]] && {
      sqoop_import_flag=false
      send_mail "sqoop 抽数 失败 ${hive_db}.${hive_tb} 重试 ${retry_i} 次！"
    } || {
      sqoop_import_flag=true
    }

    debug "${hive_db}.${hive_tb} 修复表！"
    $beeline -e "MSCK REPAIR TABLE ${hive_db}.${hive_tb};" 2>&1 | tee -a $log_file
    [[ ${PIPESTATUS[0]} = 0 ]] && debug "${hive_db}.${hive_tb} 修复表 完成！" || warn "${hive_db}.${hive_tb} 修复表 失败！"

    hive_count="$beeline --showHeader=false --outputformat=csv2 -e 'select count(1) as cnt from ${hive_db}.${hive_tb} $([[ ${is_partition} = y ]] && echo "where d_date = \"${ST9}\"" || echo '');'"
    debug "计算 Hive 中表 ${hive_db}.${hive_tb} 的数据量命令为：${hive_count}！"
    hive_data_count=$(eval "${hive_count}")
    debug "Hive 表 ${hive_db}.${hive_tb} 的数据量为：==========|${hive_data_count}|==========！"

    ((retry_i++))
  done

  [[ (${mysql_data_count:-0} != ${hive_data_count:-1}) && $((${retry_i} - 1)) = ${retry_num} ]] && {
    [[ ${sqoop_import_flag} = true ]] && sqoop_import_msg='数量不等' || sqoop_import_msg='sqoop报错'
    warn "${hive_db}.${hive_tb} 抽数 ${sqoop_import_msg} 失败！用时：${during_time:=$(during "${sqoop_s_time}")}"
    send_mail "sqoop 抽数任务 ${sqoop_import_msg} 失败 ${hive_db}.${hive_tb}！"
  } || {
    info "Sqoop 抽数 ${hive_db}.${hive_tb} 数据量 ==========|${hive_data_count}|========== 结束！用时：$(during "${sqoop_s_time}")"
  }
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
  'xinglian-t_ods_credit'
  'xinglian-t_credit_loan'

  'abs-t_loancontractinfo'
  'abs-t_borrowerinfo'
  'abs-t_associatesinfo'
  'abs-t_guarantycarinfo'
  'abs-t_repaymentplan_history'
  'abs-t_assettradeflow'
  'abs-t_actualrepayinfo'
  'abs-t_assetdealprocessinfo'
  'abs-t_assetaddtradeinfo'
  'abs-t_assetaccountcheck'
  'abs-t_projectaccountcheck'
  'abs-t_enterpriseinfo'
  'abs-t_guarantyhouseinfo'

  'abs-t_basic_asset'
  'abs-t_asset_wind_control_history'
  'abs-t_wind_control_resp_log'
  'abs-t_project'
  'abs-t_related_assets'
  'abs-t_asset_bag'
)

ST9=${1:-$(date -d '-1 day' +%F)}
shift 1

p_num=5
retry_num=1

info "Sqoop 导数总体 开始！"
sqoop_time=$curr_time
if [[ $# = 2 || $# = 3 ]]; then
  execute_fun $@
else
  arg_list=(${1:-${arg_list[@]}})
  for arg in ${arg_list[@]}; do
    execute_fun $(b_a ${arg}) &
    p_opera ${p_num}
  done
  wait_jobs
fi

info "Sqoop 导数总体 结束！用时：$(during "${sqoop_time}")"
