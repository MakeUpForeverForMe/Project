#!/bin/bash
. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

status_before_execute_job(){
  while :
  do
      # yarn application --list > /tmp/yarn_app_list.log
      assetjobstr=`cat /tmp/yarn_app_list.log | grep "AssetFileToHive" | awk -F ' ' '{print $4}'`
      echo $assetjobstr
      assetjobarr=($assetjobstr)
      flag='false'
      for ((i=0;i<${#assetjobarr[@]};i++))
      do
          echo "curr : ${assetjobarr[$i]}"
          [[ ${assetjobarr[$i]} =~ "@" ]] && fileid=`echo ${assetjobarr[$i]} | awk -F '@' '{print $2}'`
          [[ ${assetjobarr[$i]} =~ "@" ]] || fileid=${assetjobarr[$i]}
          echo "fileid:$fileid"
          # if [[ $fileid == ${input_file_suffix_arr[0]} ]];then
          if [[ $fileid == "$1" ]];then
              flag='true'
              echo "sleep 10s .........."
              sleep 10
              break 1
          fi
      done
      if [[ $flag == 'false' ]];then
          break
      fi
  done
}

status_before_execute_job test_bag


