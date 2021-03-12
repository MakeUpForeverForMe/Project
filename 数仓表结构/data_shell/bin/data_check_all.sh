#!/bin/bash -e

. ${data_check_all_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

aa=$1 # 传入校验的开始日期
bb=$2 # 传入校验的结束日期
cc=$3 # 传入校验的文件类别

[[ -z $cc ]] && {
  echo "$(date +'%F %T') 文件类别参数为空"
  exit 1
}

s_r_r(){ echo ${1##*/}; }

send_mail(){
  $mail $pm_rd "数据4.0校验 $1 代偿$2 出错！" "$(echo -e "执行区间：$aa ~ $bb\n$3")"
}

for check_file in $file_check/$cc.*.hql; do
  [[ ! -f $check_file ]] && {
    echo "$(date +'%F %T') 文件不存在 $check_file"
    exit 1
  }
  log1=$(mktemp -t log.check_all_log1.XXXXXX) || exit 1
  log2=$(mktemp -t log.check_all_log2.XXXXXX) || exit 1

  sh $data_check $check_file $aa $bb   1> $log1 &
  sh $data_check $check_file $aa $bb 1 1> $log2 &

  for pid in $(jobs -p); do
    wait $pid
  done

  [[ -s $log1 ]] && send_mail $(s_r_r $check_file) '前' "$(cat $log1)"
  [[ -s $log2 ]] && send_mail $(s_r_r $check_file) '后' "$(cat $log2)"

  trap $(rm -f $log1 $log2) 1 2 9 15 19 20
done
