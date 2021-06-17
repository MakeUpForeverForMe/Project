#!/usr/bin/env bash

. ${abs_manage_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


log_file=$log/abs_bag_project/${abs_manage_name:=$(basename "${BASH_SOURCE[0]}")}-$(date +'%F').log

debug "${abs_manage_name} 脚本运行 执行命令为：$0 $@！"

info "${abs_manage_name} 脚本运行 开始！"
abs_time=${curr_time}


debug "初始化函数 开始！"

# first arg : packet / unpacket    second arg : job name
get_result(){
  param_len_get_result=$#
  [[ $param_len_get_result != 2 ]] && error "get_result 输入参数个数错误（应为两个），输入个数为：$param_len_get_result！"
  [[ $? == 0 ]] && info "$1 job $2 finished ! " || error "$1 job $2 run failed !"
}

upload_file(){
  debug "此时 row_type 为 ${row_type}，需要上传 ${abs_file} 文件 开始！"
  hdfs_command="${hdfs} -put -f ${abs_file} ${hdfs_uri}/${warehouse}/dim.db/${input_file_prefix}_json"
  info "执行命令 ${hdfs_command}"
  eval ${hdfs_command} 2>&1 | tee -a $log_file
  [[ ${PIPESTATUS[0]} = 0 ]] && debug "此时 row_type 为 ${row_type}，需要上传 ${abs_file} 文件 结束！" || error "此时 row_type 为 ${row_type}，需要上传 ${abs_file} 文件 失败！"
}


update_func(){
  [[ $# != 2 ]] && error "增加或者更新操作的函数参数个数不为 2 ，退出！"
  file_list=(project_info project_due_bill_no bag_info bag_due_bill_no)
  table_name=${1}
  [[ ! ${file_list[@]} =~ ${table_name} ]] && error "传入的第一个参数 ${table_name} 不为表名列表中（${file_list[@]}），退出！"
  exe_id=${2}

  debug "执行 从 dim.${table_name}_json 文件表，插入到正式表 dim.${table_name} 的操作 开始！"
  update_command="sh $data_manage -a $rd -s $(date +%F) -e $(date +%F) -f $dim_new_hql/dim.${table_name}-abs.hql -k exe_id='${exe_id}'"
  debug "执行 插入 命令为：${update_command}"
  eval ${update_command} 2>&1 | tee -a $log_file
  debug "执行 从 dim.${table_name}_json 文件表，插入到正式表 dim.${table_name} 的操作 结束！"
}

# 执行任务前判断任务是否已存在
# status_before_execute_job(){}

debug "初始化函数 结束！"

debug "初始化属性 开始！"

call_back=$abs_call_back_addr
abs_import=$file_import/abs_cloud

debug "初始化属性 结束！"


info "参数判断！"

param_len=$#
debug "参数数量（1或2）判断，结果：（$param_len） 开始！"
[[ $param_len != 1 && $param_len != 2 ]] && error "参数数量（1或2），结果：$param_len！"
debug "参数数量（1或2）判断，结果：（$param_len） 结束！"


info "执行操作 开始！"
[[ $param_len = 1 ]] && {
  info "当参数个数为 1 时，开始以下操作！"
  abs_s_time_1=${curr_time}

  input_file_list=(project_info project_due_bill_no bag_info)

  debug "当参数个数为 1 时，非封包操作初始化属性 开始！"
  input_file=$(s_r_r $1)
  debug "文件名称为：${input_file}"

  debug "判断输入参数是否以 '@' 符号为分隔符！"
  [[ ${input_file} =~ '@' ]] || error "输入参数 ${input_file} 不是以 '@' 为分隔符的格式参数（表名@项目编号）！"
  debug "输入参数是以 '@' 符号为分隔符的参数！"

  input_file_name_arr=($(a_a $input_file))

  input_file_name_arr_len=${#input_file_name_arr[@]}
  debug "判断输入参数经 '@' 分隔后的长度（$input_file_name_arr_len）是否符合要求长度 2！"
  [[ ${input_file_name_arr_len} != 2 ]] && error "输入参数 ${input_file} 经 '@' 分隔后的长度（${input_file_name_arr_len}）不符合要求 2 ！"
  debug "判断输入参数经 '@' 分隔后的长度（$input_file_name_arr_len）符合要求长度 2！"

  input_file_prefix=${input_file_name_arr[0]}
  debug "判断输入参数前缀（${input_file_prefix}）是否在 ${input_file_list[@]} 中！"
  [[ -n $(echo "${input_file_list[@]}" | grep -ow "${input_file_prefix}") ]] || error "输入参数 ${input_file} 的前缀（${input_file_prefix}）不符合要求 ${input_file_list[@]}！"
  debug "判断输入参数前缀符合要求，当前参数前缀为 ${input_file_prefix}！"

  input_file_suffix=$(p_l_l ${input_file_name_arr[1]})
  abs_file=${abs_import}/${input_file_prefix}/$input_file
  row_type=$(jq -r '.row_type' ${abs_file})
  debug "文件的 row_type 为：${row_type}！"

  debug "当参数个数为 1 时，非封包操作初始化属性 结束！"


  info "星云 单个参数 具体执行操作 开始！"
  if [[ ${row_type} = 'insert' ]]; then
    info "添加操作 开始！"

    upload_file

    update_func "${input_file_prefix}" "${input_file_suffix}"

    debug "判断要执行的表 ${input_file_prefix} 是否是 project_due_bill_no！"
    [[ ${input_file_prefix} = 'project_due_bill_no' ]] && {
      debug "要执行的表 ${input_file_prefix} 是 project_due_bill_no ，回调操作执行 开始！"

      debug "回调之前先获取 导入ID（即：import_id） 开始！"
      import_id=$(jq -r '.import_id' ${abs_file})
      debug "获取 导入ID（即：import_id：${import_id}） 结束！"

      curl_command="curl -v --connect-timeout 30 -m 60 -w '\n' ${call_back}/uabs-core/callback/assetTransferSuccessConfirm -d 'importId=${import_id}'"
      debug "执行 债转 回调命令为： ${curl_command}"
      eval ${curl_command} 2>&1 | tee -a $log_file
      [[ ${PIPESTATUS[0]} = 0 ]] && debug "project_due_bill_no 回调操作执行 结束！" || warn "project_due_bill_no 回调执行 失败！"
    } || debug "要执行的表 ${input_file_prefix} 不是 project_due_bill_no ，跳过回调操作！"

    info "添加操作 结束！"
  elif [[ ${row_type} = 'update' ]]; then
    info "更新操作 开始！"

    upload_file

    update_func "${input_file_prefix}" "${input_file_suffix}"

    info "更新操作 结束！"
  elif [[ ${row_type} = 'delete' ]]; then
    info "删除操作 开始！"

    if [[ "${input_file_prefix}" = 'project_due_bill_no' ]]; then
      exe_id=$(jq -r '.import_id' ${abs_file})
      debug "删除 dim.${input_file_prefix} 表的分区 import_id 编号为：${exe_id}！"
    elif [[ "${input_file_prefix}" = 'bag_info' ]]; then
      exe_id=$(jq -r '.bag_id' ${abs_file})
      debug "删除 dim.${input_file_prefix} 表的分区 bag_id 编号为：${exe_id}！"

      debug "获取 dim.${input_file_prefix} 表的分区 ${exe_id} 的封包日期 开始！"
      bag_date=$(jq -r '.bag_date' ${abs_file})
      debug "获取 dim.${input_file_prefix} 表的分区 ${exe_id} 的封包日期 为：${bag_date}！"
    else
      error "${input_file_prefix} 不应该存在 删除 操作！"
    fi

    debug "执行 从 dim.${input_file_prefix}_json 文件表，删除 操作 开始！"
    delete_command="sh $data_manage -a $rd -s $(date +%F) -e $(date +%F) -f $dim_new_hql/dim.${input_file_prefix}-delete_patition.hql -k exe_id='${exe_id}'"
    debug "执行 操作 命令为：${delete_command}"
    eval ${delete_command} 2>&1 | tee -a $log_file
    debug "执行 从 dim.${input_file_prefix}_json 文件表，删除 操作 结束！"

    if [[ "${input_file_prefix}" = 'bag_info' ]]; then
      debug "${exe_id} 解包成功，回调操作执行 开始！"
      curl_command="curl -v --connect-timeout 30 -m 60 -w '\n' ${call_back}/uabs-core/callback/unPackageSuccessConfirm -d 'assetBagId=${exe_id}'"
      debug "${exe_id} 解包成功，回调命令为： ${curl_command}"
      eval ${curl_command} 2>&1 | tee -a $log_file
      [[ ${PIPESTATUS[0]} = 0 ]] && debug "${exe_id} 解包回调执行 结束！" || warn "${exe_id} 解包回调执行 失败！"

      if [[ -n ${bag_date} && "${input_file_prefix}" = 'bag_info' ]]; then
        debug "删除 项目 ${input_file_suffix} 下的包后，所有包的统计结果是错的，需要重新计算 开始！"

        if [[ ${is_test} = y ]]; then
          delete_end_date=${bag_date}
        elif [[ $(date -d "$(date +'%F 19:00:00')" +%s) -ge $(date +$s) ]]; then
          delete_end_date=$(date -d '-2 day' +%F)
        else
          delete_end_date=$(date -d '-1 day' +%F)
        fi

        rerun_command="sh $bin_abs/data_cloud-dm-all_bag.sh ${bag_date} ${delete_end_date} $(u_l_l ${input_file_suffix})"
        debug "执行 命令为：${rerun_command}"
        eval ${rerun_command} 2>&1 | tee -a $log_file
        debug "删除 项目 ${input_file_suffix} 下的包后，所有包的统计结果是错的，需要重新计算 结束！"
      fi
    else
      debug "删除 dim.${input_file_prefix} 表的分区 import_id 编号为：${exe_id} 结束！"
    fi

    info "删除操作 结束！"
  else
    error "row_type（${row_type}）参数传入错误！"
  fi

  info "星云 单个参数 具体执行操作 结束！用时：$(during "${abs_s_time_1}")"
} || {
  info "当参数个数为 2 时，开始以下操作！"
  abs_s_time_2=${curr_time}

  input_file_list=(bag_info bag_due_bill_no)

  debug "当参数个数为 2 时，封包操作初始化属性 开始！"

  input_file1=$(s_r_r $1)
  input_file2=$(s_r_r $2)
  debug "文件名称为：${input_file1} ${input_file1}"

  debug "判断输入参数是否以 '@' 符号为分隔符！"
  [[ ${input_file1} =~ '@' ]] || error "输入参数 ${input_file1} 不是以 '@' 为分隔符的格式参数（表名@项目编号）！"
  [[ ${input_file2} =~ '@' ]] || error "输入参数 ${input_file2} 不是以 '@' 为分隔符的格式参数（表名@项目编号）！"
  debug "输入参数是以 '@' 符号为分隔符的参数！"

  input_file_name_arr1=($(a_a ${input_file1}))
  input_file_name_arr2=($(a_a ${input_file2}))

  input_file_name_arr_len1=${#input_file_name_arr1[@]}
  debug "判断输入参数 ${input_file1} 经 '@' 分隔后的长度（$input_file_name_arr_len1）是否符合要求长度 2！"
  [[ ${input_file_name_arr_len1} != 2 ]] && error "输入参数 ${input_file1} 经 '@' 分隔后的长度（${input_file_name_arr_len1}）不符合要求 2 ！"
  debug "判断输入参数 ${input_file1} 经 '@' 分隔后的长度（$input_file_name_arr_len1）符合要求长度 2！"

  input_file_name_arr_len2=${#input_file_name_arr2[@]}
  debug "判断输入参数 ${input_file2} 经 '@' 分隔后的长度（$input_file_name_arr_len2）是否符合要求长度 2！"
  [[ ${input_file_name_arr_len2} != 2 ]] && error "输入参数 ${input_file2} 经 '@' 分隔后的长度（${input_file_name_arr_len2}）不符合要求 2 ！"
  debug "判断输入参数 ${input_file2} 经 '@' 分隔后的长度（$input_file_name_arr_len2）符合要求长度 2！"


  input_file_prefix1=${input_file_name_arr1[0]}
  debug "判断输入参数前缀（${input_file_prefix1}）是否在 ${input_file_list[@]} 中！"
  [[ -n $(echo "${input_file_list[@]}" | grep -ow "${input_file_prefix1}") ]] || error "输入参数 ${input_file1} 的前缀（${input_file_prefix1}）不符合要求 ${input_file_list[@]}！"
  debug "判断输入参数前缀符合要求，当前参数前缀为 ${input_file_prefix1}！"

  input_file_prefix2=${input_file_name_arr2[0]}
  debug "判断输入参数前缀（${input_file_prefix2}）是否在 ${input_file_list[@]} 中！"
  [[ -n $(echo "${input_file_list[@]}" | grep -ow "${input_file_prefix2}") ]] || error "输入参数 ${input_file2} 的前缀（${input_file_prefix2}）不符合要求 ${input_file_list[@]}！"
  debug "判断输入参数前缀符合要求，当前参数前缀为 ${input_file_prefix2}！"


  input_file_suffix1=$(p_l_l ${input_file_name_arr1[1]})
  input_file_suffix2=$(p_l_l ${input_file_name_arr2[1]})

  debug "判断两个参数的 包编号 是否相等！"
  [[ ${input_file_suffix1} != ${input_file_suffix2} ]] && error "两个参数的 ${input_file_suffix1} 包编号 ${input_file_suffix2} 不相等，退出封包操作！"
  info "两参数的 包编号 相等，继续进行封包操作！"


  abs_file1=${abs_import}/${input_file_prefix1}/${input_file1}
  abs_file2=${abs_import}/${input_file_prefix2}/${input_file2}

  row_type1=$(jq -r '.row_type' ${abs_file1})
  row_type2=$(jq -r '.row_type' ${abs_file2})

  debug "判断 两参数的 row_type 是否都为 insert！"
  [[ ${row_type1} != 'insert' ]] && error "参数 1 的文件 ${input_file1} 的 row_type 为：${row_type1} 退出！"
  [[ ${row_type1} != 'insert' ]] && error "参数 2 的文件 ${input_file2} 的 row_type 为：${row_type2} 退出！"
  info "两参数的 row_type 都为 insert，继续进行封包操作！"

  debug "当参数个数为 2 时，封包操作初始化属性 结束！"


  info "封包 操作 开始！"
  [[ ${input_file_prefix1} = 'bag_info' ]] && {
    debug "第一个参数为 bag_info，获取封包 日期！"
    bag_date=$(jq -r '.bag_date' ${abs_file1})
  } || {
    debug "第二个参数为 bag_info，获取封包 日期！"
    bag_date=$(jq -r '.bag_date' ${abs_file2})
  }
  debug "封包 起始日期 为：${bag_date}！"

  info "将文件表数据插入到正式表，开始"

  update_func "${input_file_prefix1}" "${input_file_suffix1}" &
  update_func "${input_file_prefix2}" "${input_file_suffix2}" &

  wait_jobs

  info "数据已插入正式表，开始封包报表任务！"

  debug "执行 封包操作 开始！"
  bag_snapshot="sh $bin_abs/data_cloud-dm-bag_snapshot.sh ${bag_date} ${bag_date} '${input_file_suffix1}'"
  debug "执行 封包时报表 任务 命令为：${bag_snapshot}"

  [[ ${is_test} = y ]] && {
    end_date=${bag_date}
  } || {
    end_date=$([[ $(date -d "$(date +'%F 19:00:00') +%s") -gt $(date +%s) ]] && date -d '-2 day' +%F || date -d '-1 day' +%F)
  }
  debug "封包结束日期为：${end_date}！"

  bag_execute="sh $bin_abs/data_cloud-dm-bag.sh ${bag_date} ${end_date} "$(u_l_l ${input_file_suffix1})" '${input_file_suffix1}'"
  debug "执行 封包历史数据报表 任务 命令为：${bag_execute}"

  debug "封包时任务前台执行，封包历史数据任务后台执行！"
  [[ ${is_test} = 'n' ]] && {
    eval ${bag_execute} 2>&1 | tee -a $log_file &
  }
  eval ${bag_snapshot} 2>&1 | tee -a $log_file
  info "封包 数据中台 任务结束！"

  debug "${input_file_suffix1} 封包成功，更新包状态 开始！"
  update_status="sh $data_manage -a $rd -s $(date +%F) -e $(date +%F) -f $dim_new_hql/dim.bag_info-update_status.hql -k exe_id='${input_file_suffix1}'"
  debug "执行 命令为：${update_status}"
  eval ${update_status} 2>&1 | tee -a $log_file
  debug "${input_file_suffix1} 封包成功，更新包状态 结束！"

  debug "${input_file_suffix1} 封包成功，回调通知星云平台 执行 开始！"
  curl_command="curl -v --connect-timeout 30 -m 60 -w '\n' ${call_back}/uabs-core/callback/packageSuccessConfirm -d 'assetBagId=${input_file_suffix1}'"
  debug "${input_file_suffix1} 封包成功，回调命令为： ${curl_command}"
  eval ${curl_command} 2>&1 | tee -a $log_file
  [[ ${PIPESTATUS[0]} = 0 ]] && debug "${input_file_suffix1} 封包回调执行 结束！" || warn "${input_file_suffix1} 封包回调执行 失败！"

  debug "执行 封包操作 结束！"

  info "封包 操作 结束！"
  info "星云 两个参数 具体执行操作 结束！用时：$(during "${abs_s_time_2}")"
}

info "${abs_manage_name} 脚本运行 结束！用时：$(during "${abs_time}")"
