#!/bin/bash -e

. /etc/profile
. ~/.bash_profile


OPTIND=1
while getopts :f:n: opt; do
  case $opt in
    (f) file=$OPTARG ;;
    (n) line=$OPTARG ;;
    (:) echo "请添加参数: -$OPTARG" ;;
    (?) echo "选项未设置: -$OPTARG" ;;
    (*) echo "未知情况" ;;
  esac
done
shift $(($OPTIND - 1))
[[ -n $@ || $OPTIND = 1 ]] && {
  echo "$(date +'%F %T') 不允许有非指定性参数，请使用 -f（指定日志文件）、-n（指定要读取的行数，默认从第一行开始。可无）"
  exit
}


tail -n ${line:-+0} -F $file | grep -winPo '10000[\.> ]+\K[\d,No]{2,4}.*|^[\d,No ]+row[s]? affected.*|.*err.*|.*erro.*|.*error.*|.*整体任务.*|.*任务执行.*|.*执行开始.*|.*数据任务 执行结束  用时.*|.*执行失败.*|Query: invalidate metadata.*|{"code[":"].*|.*Impala 刷新任务.*|.*执行  结束.*|.*执行  开始.*|.*重试第.*|.*请输入.*|.*参数.*'
