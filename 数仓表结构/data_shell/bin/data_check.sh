#!/bin/bash

. ${data_check_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

q_file=$1
s_date=${2:-'2020-06-01'}
e_date=${3:-$(date -d '-1 day' +%F)}
suffix=${4}

[[ (-z $q_file) || (-z $s_date) || (-z $e_date) ]] && {
  echo -e "请输入 q_file：${q_file} s_date：${s_date} e_date：${e_date} 的值"
  exit 1
}

[[ -n $suffix && $suffix != 1 ]] && {
  echo -e "输入错误：$suffix，需要为（空 或 1）"
  exit 1
}

[[ -z $suffix ]] && db_suffix= tb_suffix=_asset || db_suffix=_cps tb_suffix=

temp_file=$(mktemp -t log.${suffix:-0}.${1##*/}.XXXXXX) || exit 1

for (( i = $(date -d "$s_date" +%s); $i <= $(date -d "$e_date" +%s); i = $(date -d "1 day $(date -d @$i +%F)" +%s) )); do
  date_st9=$(date -d@$i +%F)
  echo $date_st9 $([[ $db_suffix == _cps ]] && echo '代偿后' || echo '代偿前') ${q_file##*/} 1>&2
  # echo \
  $impala --var=ST9=$date_st9 --var=db_suffix=$db_suffix --var=tb_suffix=$tb_suffix -f $q_file 2> $temp_file
  [[ $? != 0 ]] && { cat $temp_file 1>&2; exit 1; }
done

trap $(rm -f $temp_file) 1 2 9 15 19 20
