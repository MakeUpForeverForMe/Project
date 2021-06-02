#!/usr/bin/env bash

. ${data_manage_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

trap '
  [[ -f ${manage_kv_tmp_file} ]] && {
    rm -f ${manage_kv_tmp_file}
    info "执行 ${manage_hql} 退出时，删除 ${manage_kv_tmp_file}"
  }
' EXIT

title(){
  echo '****************************************'
  echo '*        本脚本的参数如下              *'
  echo '*        -h  无参  脚本使用方法        *'
  echo '*        -f  必填  执行文件            *'
  echo '*        -s  必填  开始日期            *'
  echo '*        -e  必填  结束日期            *'
  echo '*        -t  无参  设置执行引擎        *'
  echo '*        -l  无参  日志日期为当前日期  *'
  echo '*        -b  可无  设置任务名称        *'
  echo '*        -i  可无  执行参数文件        *'
  echo '*        -k  可无  输入kv型数据        *'
  echo '*        -r  可无  失败重试次数        *'
  echo '*        -n  可无  任务并行数量        *'
  echo '*        -a  可无  邮件配置路径        *'
  echo '****************************************'
}

execute_hql(){
  local flag=false n=0
  while [[ $flag == false && $n -le ${manage_retry_num:=5} ]]; do
    beeline_log_tmp_file=$(mktemp -t ${manage_job_name}$app_name-$(date -d ${manage_s_date} +%Y_%m_%d).XXXXXX) || error "创建临时文件失败！"
    debug "创建临时文件 ${beeline_log_tmp_file}"

    query_cmd="$beeline_cmd -f $manage_hql"
    debug "Beeline 执行命令为：${query_cmd}"

    info "执行日期为：$manage_s_date 的数据任务 $file $manage_param 执行开始！"
    execute_time=$curr_time
    eval $query_cmd 2>&1 | tee $beeline_log_tmp_file 2>&1 | tee -a $log_file
    flag=$([[ ${PIPESTATUS[0]} == 0 ]] && echo true || echo false)
    yarn_application_id=$(grep -Po 'App id \Kapplication[_\d]+[^)]' $beeline_log_tmp_file)
    [[ -n $yarn_application_id ]] && debug "执行日期为：$manage_s_date 的数据任务 $file $manage_param 的 yarn application_id 为：$yarn_application_id"
    [[ -n $beeline_log_tmp_file ]] && {
      rm -f $beeline_log_tmp_file
      debug "执行日期为：$manage_s_date 的数据任务 $file $manage_param 删除临时文件 $beeline_log_tmp_file"
    }

    [[ $flag == false ]] && {
      warn "执行日期为：$manage_s_date 的数据任务 $file $manage_param 执行失败！用时：$(during "$execute_time") 重试第 $(($n + 1)) 次（默认 $manage_retry_num 次） $([[ $n == $manage_retry_num ]] && echo '退出')"
    } || {
      info "执行日期为：$manage_s_date 的数据任务 $file $manage_param 执行结束！用时：$(during "$execute_time")"
    }

    [[ $flag == false && -n $manage_mail_file && $n == $manage_retry_num ]] && {
      debug "执行日期为：$manage_s_date 的数据任务 $file $manage_param 发送邮件！"
      $mail $manage_mail_file 'data_manage 执行任务失败' "$(echo -e "
        \n发送时间为： $(date +'%F %T')
        \n执行日期为： $manage_s_date
        \n执行文件为： $manage_hql
        \n执行yarnId： $yarn_application_id
        \n执行语句为： $query_cmd
        \n配置文件为： $manage_param
        \n配置文件为： $manage_kv_tmp_file
        \n    内容为：
              \n$(cat ${manage_param:-/dev/null})
              \n$(cat ${manage_kv_tmp_file:-/dev/null})
        \n报错内容为：
              \n$(tail -n 10000 | grep -niwPo '.*[10.]*> \K[^No][\w/].*|.*err.*|.*erro.*|.*error.*|.*Caused by.*|.*No such file or directory.*' $log_file | grep -Eiw 'err|erro|error|.*/.*|.*No such file or directory.*' | tail -n 10)
      " | sed '/^\s*$/d')"
    }
    ((++n))
  done
}

impala(){
  debug "Impala 命令执行 开始！"
  impala_time=$curr_time
  unset impala_args
  for (( i = 1; i <= $#; i++ )); do
    impala_args=(${impala_args[*]} "${!i}")
  done
  impala_cmd="$impala ${impala_args[*]:0:$((${#impala_args[*]} - 1))} '${impala_args[-1]}'"
  debug "Impala 执行命令为：${impala_cmd}"
  eval "${impala_cmd}" 2>&1 | tee -a $log_file
  debug "Impala 命令执行 结束！用时：$(during "${impala_time}")"
}




OPTIND=1
while getopts :f:b:i:s:e:r:k:a:n:tlh opt; do
  case $opt in
    (h)
      title
      exit 0
    ;;
    (l) # 日志的日期
      manage_log_date=$(date +%F)
    ;;
    (t) # 执行引擎
      manage_type=impala
    ;;
    (f) # 执行 hql 文件名称，可以带路径
      manage_hql=$OPTARG
      file=$(s_r_r $manage_hql) # 解析文件名（如：***/dim.dim_encrypt_info.hql ——> dim.dim_encrypt_info.hql）
      file_db_tb_pa=($(p_a $file)) # 根据文件名解析库名、表名（如：dim.dim_encrypt_info.hql ——> 0：dim、1：dim_encrypt_info[-abs]、2：hql）
      file_db_tb=(${file_db_tb_pa[0]} $(b_a ${file_db_tb_pa[1]})) # 解析为：0：dim、1：dim_encrypt_info、2：abs...

      manage_log_dir=$log/${file_db_tb[0]}
      [[ ! -d $manage_log_dir ]] && mkdir -p $manage_log_dir
    ;;
    (b) # 设置任务名称
      manage_job_name=$OPTARG
    ;;
    (s) # 开始时间 格式：2020-07-01
      manage_s_date=$OPTARG
    ;;
    (e) # 结束时间 格式：2020-07-01
      manage_e_date=$OPTARG
    ;;
    (i) # 执行 hql 文件 相应的参数脚本名称，可以带路径
      manage_param=$OPTARG
    ;;
    (a) # 指定邮件的配置文件路径
      manage_mail_file=$OPTARG
    ;;
    (n) # 设置并行数量
      manage_parallel_num=$OPTARG
      [[ $(echo "$manage_parallel_num > 0" | bc) != 1 || $(echo "$manage_parallel_num % 1" | bc) != 0 ]] && error "并行数量 请输入非零正整数（${manage_parallel_num}）！"
    ;;
    (r) # 任务出错时，重试次数
      manage_retry_num=$OPTARG
      [[ $(echo "$manage_retry_num > 0" | bc) != 1 || $(echo "$manage_retry_num % 1" | bc) != 0 ]] && error "重试次数 请输入非零正整数（${manage_retry_num}）！"
    ;;
    (k) # 输入kv型数据
      manage_kv=$OPTARG

      [[ ! ${manage_kv} =~ '=' ]] && error "请输入kv型数据（如：aa=bb或aa=）。错误输入：${manage_kv}"

      manage_kv_key=$(e_l_l "$manage_kv")
      manage_kv_val=$(e_l_r "$manage_kv")
      manage_kvs=("$manage_kv_key" "$manage_kv_val")

      # echo "manage_kv     -----|"$manage_kv"|-----" "!!!!!|$manage_kv|!!!!!"
      # echo "manage_kv_key -----|"$manage_kv_key"|-----" "!!!!!|$manage_kv_key|!!!!!"
      # echo "manage_kv_val -----|"$manage_kv_val"|-----" "!!!!!|$manage_kv_val|!!!!!"
      # echo "manage_kvs    -----|0:"${manage_kvs[0]}",1:${manage_kvs[1]}|-----" "!!!!!|0:${manage_kvs[0]},1:${manage_kvs[1]}|!!!!!"

      [[ -z ${manage_kv_tmp_file} ]] && {
        manage_kv_tmp_file=$(mktemp -t kv.XXXXXX) || exit 1
      }
      echo "set hivevar:$manage_kv_key=$manage_kv_val;" >> $manage_kv_tmp_file
    ;;
    (:)
      error "请添加参数: -$OPTARG"
    ;;
    (?)
      title
      error "选项未设置: -$OPTARG"
    ;;
    (*)
      error "未知情况"
    ;;
  esac
done
shift $(($OPTIND - 1))
[[ -n $@ || $OPTIND = 1 ]] && {
  title
  error "不允许有非指定性参数！"
}
[[ -z $manage_hql ]] && error "请输入要执行的 Hql 文件！"
[[ -z $manage_s_date || -z $manage_e_date ]] && error "请输入日期区间！（s_date=$manage_s_date，e_date=$manage_e_date）"
[[ $(date -d $manage_s_date +%s) -gt $(date -d $manage_e_date +%s) ]] && error "开始时间（$manage_s_date）不应大于结束时间（$manage_e_date）"

log_file=$manage_log_dir/${file_db_tb[1]}.${manage_log_date:-${manage_e_date}}.log

param_db_tb=($(p_a $(s_r_r $manage_param)))
dt_name=${param_db_tb[0]:-${file_db_tb[0]}}.${file_db_tb[1]}
app_name=${param_db_tb[0]:-${file_db_tb[0]}}.${file_db_tb_pa[1]}
manage_type=${manage_type:-hive}
manage_job_name=$([[ -n "${manage_job_name}" ]] && echo "${manage_job_name}-" || echo "${manage_job_name}")


# echo $file ${file_db_tb[@]} ${param_db_tb[@]} $manage_type


[[ $manage_type == 'hive' ]] && {
  [[ -n $manage_param ]] && hive_param="-i $manage_param" || hive_param=''
  # [[ -n $manage_kv ]] && hive_kv="--hivevar ${manage_kv}" || hive_kv=''
  [[ -n $manage_kv ]] && hive_kv="-i ${manage_kv_tmp_file}" || hive_kv=''
  cmd_beeline="$beeline $hive_param $hive_kv"
} || {
  [[ -n $manage_param ]] && impala_param="--config_file=$manage_param" || impala_param=''
  [[ -n $manage_kv ]] && impala_kv="--var=${manage_kvs[0]}='${manage_kvs[1]}'" || impala_kv=''
  cmd_beeline="$impala $impala_param $impala_kv"
}



info "Beeline 执行任务 $file $manage_param 执行开始！"
manage_s_time=$curr_time
while [[ "$(date -d "$manage_s_date" +%s)" -le "$(date -d "${manage_e_date:=$2}" +%s)" ]]; do
  unset date_in_s date_in_e
  {
    [[ $manage_type == 'hive' ]] && {
      # --hiveconf hive.session.id='${manage_job_name}$app_name-${manage_s_date}-$(date +%N)' \ # 不能添加，会导致日志不全，没有yarn的applicationID
      beeline_cmd="$cmd_beeline \
      --hiveconf mapred.job.name='${manage_job_name}$app_name-$(date -d ${manage_s_date} +%Y_%m_%d)-$(date +%N)' \
      --hiveconf spark.app.name='${manage_job_name}$app_name-$(date -d ${manage_s_date} +%Y_%m_%d)-$(date +%N)' \
      --hivevar ST9=${manage_s_date}"
    } || {
      beeline_cmd="$cmd_beeline \
      --var=ST9=${manage_s_date}"
    }

    execute_hql
  } &
  p_opera ${manage_parallel_num:-1}
  [[ $manage_s_date == $manage_e_date ]] && wait_jobs
  manage_s_date=$(date -d "$manage_s_date +1 day" +%F)
done
info "Beeline 执行任务 $file $manage_param 执行结束！用时：$(during "$manage_s_time")"

info "Impala 刷新任务 $file $manage_param 执行开始！"
impala_s_time=$curr_time

impala -q "refresh $dt_name;"
# impala -q "invalidate metadata $dt_name;"
# impala -q "COMPUTE STATS $dt_name;"
impala -q "select * from $dt_name where false;"

info "Impala 刷新任务 $file $manage_param 执行结束！用时：$(during "$impala_s_time")"

info "任务 $file $manage_param 执行结束！用时：$(during "$manage_s_time")"
