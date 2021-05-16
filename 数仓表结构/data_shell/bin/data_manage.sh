#!/usr/bin/env bash

. ${data_manage_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


title(){
  echo '****************************************'
  echo '*        本脚本的参数如下              *'
  echo '*        -f  必填  执行文件            *'
  echo '*        -s  必填  开始日期            *'
  echo '*        -e  必填  结束日期            *'
  echo '*        -t  无参  设置执行引擎        *'
  echo '*        -b  可无  设置任务名称        *'
  echo '*        -i  可无  执行参数文件        *'
  echo '*        -k  可无  输入kv型数据        *'
  echo '*        -r  可无  失败重试次数        *'
  echo '*        -n  可无  任务并行数量        *'
  echo '*        -a  可无  邮件配置路径        *'
  echo '*        -h  无参  脚本使用方法        *'
  echo '****************************************'
}

execute_hql(){
  local flag=false n=0
  while [[ $flag == false && $n -le ${manage_retry_num:=5} ]]; do
    query_cmd="$beeline_cmd -f $manage_hql"
    beeline_log_tmp_file=$(mktemp -t ${manage_job_name}$app_name-${manage_s_date}.XXXXXX) || exit 1

    echo 'yarn_application_id in beeline_log_tmp_file is'$beeline_log_tmp_file

    echo $query_cmd
    eval $query_cmd 2>&1 | tee $beeline_log_tmp_file

    flag=$([[ ${PIPESTATUS[0]} == 0 ]] && echo true || echo false)

    yarn_application_id=$(grep -Po 'App id \Kapplication[_\d]+[^)]' $beeline_log_tmp_file)
    echo 'yarn_application_id is '$yarn_application_id

    [[ -n $beeline_log_tmp_file ]] && trap $(rm -f $beeline_log_tmp_file) 1 2 9 15 19 20

    [[ $flag == false ]] && echo -e "$(date +'%F %T') 执行日期为：$manage_s_date 的数据任务 $app_name ${param_db_tb[1]} $yarn_application_id 执行失败，重试第 $(($n + 1)) 次（默认重试次数为： $manage_retry_num） $([[ $n == $manage_retry_num ]] && echo '跳过')"
    [[ $flag == false && -n $manage_mail_file && $n == $manage_retry_num ]] && {
      # echo \
      $mail $manage_mail_file 'data_manage 执行任务失败' "$(echo -e "
        \n发送时间为： $(date +'%F %T')
        \n执行日期为： $manage_s_date
        \n执行文件为： $manage_hql
        \n执行yarnId： $yarn_application_id
        \n执行语句为： $query_cmd
        \n配置文件为： $manage_param
        \n    内容为：
              \n$(cat ${manage_param:-/dev/null})
        \n报错内容为：
              \n$(tail -n 10000 | grep -niwPo '.*[10.]*> \K[^No][\w/].*|.*err.*|.*erro.*|.*error.*|.*Caused by.*|.*No such file or directory.*' $manage_log | grep -Eiw 'err|erro|error|.*/.*|.*No such file or directory.*' | tail -n 10)
      " | sed '/^\s*$/d')"
    }
    ((++n))
  done
}


manage_kv_tmp_file=$(mktemp -t kv.XXXXXX) || exit 1

OPTIND=1
while getopts :f:b:i:s:e:r:k:a:n:th opt; do
  case $opt in
    (h)
      title
      exit 0
    ;;
    (f) # 执行 hql 文件名称，可以带路径
      manage_hql=$OPTARG
    ;;
    (b) # 设置任务名称
      manage_job_name=$OPTARG
    ;;
    (t) # 执行引擎
      manage_type=impala
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
      [[ $(echo "$manage_parallel_num > 0" | bc) != 1 || $(echo "$manage_parallel_num % 1" | bc) != 0 ]] && {
        echo "$(date +'%F %T') 请输入非零正整数。错误的输入参数为：$manage_parallel_num"
        exit 1
      }
    ;;
    (r) # 任务出错时，重试次数
      manage_retry_num=$OPTARG
      [[ $(echo "$manage_retry_num > 0" | bc) != 1 || $(echo "$manage_retry_num % 1" | bc) != 0 ]] && {
        echo "$(date +'%F %T') 请输入非零正整数。错误的输入参数为：$manage_retry_num"
        exit 1
      }
    ;;
    (k) # 输入kv型数据
      manage_kv=$OPTARG
      manage_kv_key=$(e_l_l "$manage_kv")
      manage_kv_val=$(e_l_r "$manage_kv")
      manage_kvs=("$manage_kv_key" "$manage_kv_val")

      # echo "manage_kv     -----|"$manage_kv"|-----" "!!!!!|$manage_kv|!!!!!"
      # echo "manage_kv_key -----|"$manage_kv_key"|-----" "!!!!!|$manage_kv_key|!!!!!"
      # echo "manage_kv_val -----|"$manage_kv_val"|-----" "!!!!!|$manage_kv_val|!!!!!"
      # echo "manage_kvs    -----|0:"${manage_kvs[0]}",1:${manage_kvs[1]}|-----" "!!!!!|0:${manage_kvs[0]},1:${manage_kvs[1]}|!!!!!"
      # echo "manage_kv     $manage_kv"

      [[ ${#manage_kvs[@]} != 1 && ${#manage_kvs[@]} != 2 ]] && {
        echo "$(date +'%F %T') 请输入kv型数据（如：aa=bb或aa=）。错误的输入参数为：$manage_kv"
        exit 1
      }

      echo "set hivevar:$manage_kv_key=$manage_kv_val;" >> $manage_kv_tmp_file
    ;;
    (:)
      echo "请添加参数: -$OPTARG"
      exit 1
    ;;
    (?)
      echo -e "选项未设置: -$OPTARG\n$(title)"
      exit 1
    ;;
    (*)
      echo "未知情况"
      exit 1
    ;;
  esac
done
shift $(($OPTIND - 1))
[[ -n $@ || $OPTIND = 1 ]] && {
  echo -e "$(date +'%F %T') 不允许有非指定性参数，请使用\n$(title)"
  exit 1
}
[[ -z $manage_hql ]] && {
  echo "$(date +'%F %T') 请输入要执行的 Hql 文件！"
  exit 1
}
[[ -z $manage_s_date || -z $manage_e_date ]] && {
  echo "$(date +'%F %T') 请输入日期区间！（s_date=$manage_s_date，e_date=$manage_e_date）"
  exit 1
}
[[ $(date -d $manage_s_date +%s) -gt $(date -d $manage_e_date +%s) ]] && {
  echo "$(date +'%F %T') 开始时间（ $manage_s_date ）不应大于结束时间（ $manage_e_date ）"
  exit 1
}



file=$(s_r_r $manage_hql) # 解析文件名（如：***/dim_new.dim_encrypt_info.hql ——> dim_new.dim_encrypt_info.hql）
file_db_tb=($(p_a $file)) # 根据文件名解析库名、表名（如：dim_new.dim_encrypt_info.hql ——> 0：dim_new、1：dim_encrypt_info、2：hql）
param_db_tb=($(p_a $(s_r_r $manage_param)))
app_name=$(
  case ${file_db_tb[1]} in
    ( 'eagle_asset_change_comp_t1' ) echo dm_eagle ;;
    ( * ) echo ${param_db_tb[0]:-${file_db_tb[0]}} ;;
  esac
).$(
  if [[ ${file_db_tb[1]} =~ ^repay_detail_* ]]; then
    echo repay_detail
  else
    echo ${file_db_tb[1]}
  fi
)
manage_type=${manage_type:-hive}
manage_job_name=$([[ -n "${manage_job_name}" ]] && echo "${manage_job_name}-")


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

# echo $cmd_beeline

# exit

manage_log_dir=$log/${file_db_tb[0]}
[[ ! -d $manage_log_dir ]] && mkdir -p $manage_log_dir
manage_log=$manage_log_dir/${file_db_tb[1]}.${manage_e_date}.log
# manage_log=$manage_log_dir/${file_db_tb[1]}.$(date +%F).log

# &>> $manage_log

echo -e "${date_s:=$(date +'%F %T')} 任务 $app_name 执行  开始 当前脚本进程ID为：$(pid)\n"                                                                                                    &>> $manage_log

while [[ "$(date -d "$manage_s_date" +%s)" -le "$(date -d "${manage_e_date:=$2}" +%s)" ]]; do
  unset date_in_s date_in_e
  {
    [[ $manage_type == 'hive' ]] && {
      # --hiveconf hive.session.id='${manage_job_name}$app_name-${manage_s_date}-$(date +%N)' \ # 不能添加，会导致日志不全，没有yarn的applicationID
      beeline_cmd="$cmd_beeline \
      --hiveconf mapred.job.name='${manage_job_name}$app_name-${manage_s_date}-$(date +%N)' \
      --hiveconf spark.app.name='${manage_job_name}$app_name-${manage_s_date}-$(date +%N)' \
      --hivevar ST9=${manage_s_date}"
    } || {
      beeline_cmd="$cmd_beeline \
      --var=ST9=${manage_s_date}"
    }

    echo -e "${date_in_s:=$(date +'%F %T')} 开始日期为：$manage_s_date 的数据任务 $app_name ${param_db_tb[1]} 执行开始 当前脚本进程ID为：$(pid)"                                              &>> $manage_log
    execute_hql                                                                                                                                                                               &>> $manage_log
    echo -e "${date_in_e:=$(date +'%F %T')} 结束日期为：$manage_s_date 的数据任务 $app_name ${param_db_tb[1]} $yarn_application_id 执行结束 当前脚本进程ID为：$(pid)  用时：$(during "$date_in_e" "$date_in_s")\n" &>> $manage_log
  } &
  p_opera ${manage_parallel_num:-1}                                                                                                                                                           &>> $manage_log
  [[ $manage_s_date == $manage_e_date ]] && wait_jobs
  manage_s_date=$(date -d "$manage_s_date +1 day" +%F)
done

[[ -n $manage_kv_tmp_file ]] && trap $(rm -f $manage_kv_tmp_file) 1 2 9 15 19 20

echo -e "${date_impala_s:=$(date +'%F %T')} Impala 刷新任务 执行开始  表名为：$app_name 当前脚本进程ID为：$(pid)"                                                                             &>> $manage_log
# $impala -q "refresh $app_name;"                                                                                                                                                               &>> $manage_log
# $impala -q "invalidate metadata $app_name;"                                                                                                                                                   &>> $manage_log
# $impala -q "COMPUTE STATS $app_name;"                                                                                                                                                         &>> $manage_log
# $impala -q "select * from $app_name where false;"                                                                                                                                             &>> $manage_log
echo -e "${date_impala_e:=$(date +'%F %T')} Impala 刷新任务 执行结束  表名为：$app_name 当前脚本进程ID为：$(pid)  用时：$(during "$date_impala_e" "$date_impala_s")"                          &>> $manage_log

echo -e "${date_e:=$(date +'%F %T')} 任务 $app_name 执行  结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_e" "$date_s")\n\n"                                                           &>> $manage_log
