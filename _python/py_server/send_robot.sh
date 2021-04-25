#!/bin/bash

. ${send_robot_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


# yum list installed jq | grep jq

args_length=$#
if [[ "${args_length}" != 2 && "${args_length}" != 3 ]]; then
  echo "输入的参数数量（2或3个）不正确 ${args_length}！
    1、机器人 Hook 的 Key
    2、要发送的数据
    3、要提醒人的手机号（(逗号),分隔）【可无】
  "
  exit 1
fi

url="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=${1} -H 'Content-Type: application/json'"


old_IFS=$IFS IFS=$'\n'
read -ra lines -d $'\0' <<< "${2}"
for line in ${lines[@]}; do
  if [[ ${i:=1} = 1 ]]; then
    data="<font color='warning'>${line}</font>"
  else
    tmp_IFS=$IFS IFS='|'
    kv=(${line//:/|})
    k="${kv[0]}"
    v="${kv[1]}"
    data+="\n>${k}:<font color='comment'>${v}</font>"
    IFS=$tmp_IFS
  fi
  ((i++))
done
IFS=$old_IFS

data="$(date +'%F %T') ${data}"


curl "${url}" -d "$(echo '{
  "msgtype": "markdown",
  "markdown": {
    "content": "'${data}'"
  }
}')"


[[ $args_length = 3 ]] && {
  phone_nums="${3//,/","}"
  curl "${url}" -d "$(echo '{
    "msgtype": "text",
    "text": {
      "content": "",
      "mentioned_mobile_list": ["'${phone_nums}'"]
    }
  }')"
}
