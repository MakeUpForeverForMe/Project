#!/bin/bash -e

. ${abs_manage_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../lib/function.sh
. ${data_manage_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh

import_file_dir=/root/data_shell/file_import/abs_cloud
log_file=${abs_manage_dir}/../log/abs_bag_project/logs/spark-driver.log
input_file_list=(project_info project_due_bill_no bag_info bag_due_bill_no)
call_back_address=$([[ ${is_test} == 'y' ]] && echo '10.83.0.69:8210' || echo 'https://uabs-server.weshareholdings.com')

# 解析json
parse_json(){
    echo "${1//\"/}" | sed "s/.*$2:\([^,}]*\).*/\1/"
}

print_log(){
    echo "$1" >> $log_file
}

# first arg : packet / unpacket    second arg : job name
get_result(){
    if [[ $? != 0 ]]
    then
    echo "$1 job $2 run failed !" >> $log_file
    exit 1
    else
    echo "$1 job $2 finished ! " >> $log_file
    fi
}

# 执行任务前判断yarn上是否存在当前文件id的任务
status_before_execute_job(){
  while :
  do
      yarn application --list > /tmp/yarn_app_list.log
      assetjobstr=`cat /tmp/yarn_app_list.log | grep "AssetFileToHive" | awk -F ' ' '{print $4}'`
      echo $assetjobstr
      assetjobarr=($assetjobstr)
      flag='false'
      for ((i=0;i<${#assetjobarr[@]};i++))
      do
          [[ ${assetjobarr[$i]} =~ "@" ]] && fileid=`echo ${assetjobarr[$i]} | awk -F '@' '{print $2}'`
          [[ ${assetjobarr[$i]} =~ "@" ]] || fileid=`echo ${assetjobarr[$i]} | awk -F '-' '{print $2}'`
          echo "fileid:$fileid" >> $log_file
          # if [[ $fileid == ${input_file_suffix_arr[0]} ]];then
          if [[ $fileid == "$1" ]];then
              flag='true'
              echo "sleep 10s .........." >> $log_file
              sleep 10
              break 1
          fi
      done
      if [[ $flag == 'false' ]];then
          break
      fi
  done
}

[[ -z $@ ]] && {
  echo "$(date +'%F %T') 请输入至少一个参数！"
  exit 1
}

# 声明文件类型数组
declare -a input_file_prefix_arr
# 声明文件id数组
declare -a input_file_suffix_arr
# 声明文件row_type类型数组
declare -a input_file_type_arr
index=0

if [ $# -eq 2 ]
then
    for input_file in $@; do
      # 解析传入参数filename@id.json
      input_file_name_arr=($(a_a $(s_r_r $input_file)))
      input_file_name_arr_len=${#input_file_name_arr[@]}
      input_file_prefix=${input_file_name_arr[0]}
      input_file_suffix=$(p_l_l ${input_file_name_arr[1]})
      # 文件校验
      [[ $input_file_name_arr_len != 2 ]] && {
        echo "$(date +'%F %T') 传入的参数【${input_file}，解析后为：${input_file_name_arr[@]}】长度【默认：2；实际：${input_file_name_arr_len}】不正确！" >> ${abs_manage_dir}/../log/abs_bag_project/$(date +'%F').log
        exit 1
      }
      #echo ${input_file_name_arr[@]} $input_file $input_file_prefix $input_file_suffix >> ${abs_manage_dir}/../log/abs_bag_project/$(date +'%F').log
      # 将文件前缀和后缀放入数组
      input_file_prefix_arr[index]=${input_file_prefix}
      input_file_suffix_arr[index]=${input_file_suffix}
      row_type=$(parse_json "`cat ${import_file_dir}/${input_file_prefix}/$input_file`" "row_type")
      print_log "row_type $index : ${row_type}"
      input_file_type_arr[index]=${row_type}
      let index++
    done
    # 满足此条件时，则为封包 : 文件名为bag_info或bag_due_bill_no且两个文件的包id相同且都为insert类型
    if [[ ((${input_file_prefix_arr[0]} = 'bag_info' && ${input_file_prefix_arr[1]} = 'bag_due_bill_no')
            || (${input_file_prefix_arr[0]} = 'bag_due_bill_no' && ${input_file_prefix_arr[1]} = 'bag_info'))
          && ${input_file_type_arr[0]} = 'insert' && ${input_file_type_arr[1]} = 'insert'
          && ${input_file_suffix_arr[0]} = ${input_file_suffix_arr[1]} ]]
    then
        # 先校验是否有当前fileid的任务正在执行，若有，则等待
        status_before_execute_job ${input_file_suffix_arr[0]}
        print_log "start to packet ! "
        bag_date=$(parse_json "`cat ${import_file_dir}/bag_info/bag_info@${input_file_suffix_arr[0]}.json`" "bag_date")
        end_date=$(parse_json "`cat ${import_file_dir}/bag_info/bag_info@${input_file_suffix_arr[0]}.json`" "insert_date")
        sh /root/data_shell/bin_abs/abs-submit.sh ${input_file_prefix_arr[0]} ${input_file_suffix_arr[0]}
        get_result "packet" "first file : ${input_file_prefix_arr[0]}"
        impala-shell -q "refresh dim_new.${input_file_prefix_arr[0]};"
        sh /root/data_shell/bin_abs/abs-submit.sh ${input_file_prefix_arr[1]} ${input_file_suffix_arr[1]}
        get_result "packet" "second file : ${input_file_prefix_arr[1]}"
        impala-shell -q "refresh dim_new.${input_file_prefix_arr[1]};"
        # 封包任务
        sh $bin_abs/data_abs_dm_eagle_snapshot.sh $bag_date $bag_date ${input_file_suffix_arr[0]} >> $log_file
        # sh $bin_abs/data_abs_dm_eagle.sh $bag_date $end_date ${input_file_suffix_arr[0]} >> $log_file
        sh $bin_abs/data_abs_dm_eagle.sh $bag_date $bag_date ${input_file_suffix_arr[0]} >> $log_file &
    else
        print_log "illegal arguments ! "
        print_log "first arg : $1 , second arg : $2"
        exit 1
    fi
    # 封包成功回调
    if [[ $? != 0 ]]
    then
    print_log "packet job run failed !"
    exit 1
    else
    print_log "packet finished,bag id is ${input_file_suffix} ! "
    sh $bin_abs/data_abs_dm_eagle_update.sh $bag_date $bag_date ${input_file_suffix_arr[0]} >> $log_file
    impala-shell -q "refresh dim_new.bag_info;"
    curl -d "assetBagId=${input_file_suffix}" --connect-timeout 30 -m 60 ${call_back_address}/uabs-core/callback/packageSuccessConfirm >> $log_file
    fi
elif [ $# -eq 1 ]
then
    input_file=$1
    input_file_name_arr=($(a_a $(s_r_r $input_file)))
    input_file_name_arr_len=${#input_file_name_arr[@]}
    input_file_prefix=${input_file_name_arr[0]}
    input_file_suffix=$(p_l_l ${input_file_name_arr[1]})
    row_type=$(parse_json "`cat ${import_file_dir}/${input_file_prefix}/$input_file`" "row_type")
    print_log "row_type : ${row_type}"
    # 满足此条件时，为解包 : bag_info
    if [[ ${input_file_prefix} = 'bag_info' && ${row_type} = 'delete' ]]
    then
        # 先校验是否有当前fileid的任务正在执行，若有，则等待
        status_before_execute_job ${input_file_suffix}
        print_log "start to unpacket ! "
        sh /root/data_shell/bin_abs/abs-submit.sh ${input_file_prefix} ${input_file_suffix}
        bag_date=$(parse_json "`cat ${import_file_dir}/bag_info/bag_info@${input_file_suffix}.json`" "bag_date")
        # 解包任务 : 删除对应bag_id分区
        sh $bin_abs/data_abs_dm_eagle_unpacket.sh $bag_date $bag_date ${input_file_suffix} >> $log_file
       # 解包成功回调
        print_log "unpacket finished,bag id is ${input_file_suffix} ! "
        curl -d "assetBagId=${input_file_suffix}" --connect-timeout 30 -m 60 ${call_back_address}/uabs-core/callback/unPackageSuccessConfirm >> $log_file
    else
      # 先校验是否有当前fileid的任务正在执行，若有，则等待
      status_before_execute_job ${input_file_suffix}
      print_log "normal job start !"
      sh /root/data_shell/bin_abs/abs-submit.sh ${input_file_prefix} ${input_file_suffix}
      impala-shell -q "refresh dim_new.${input_file_prefix};"
    fi
else
  print_log "illegal arguments : the size of arguments is not one or two ! "
fi
