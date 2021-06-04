#!/usr/bin/env bash

. ${ftp_udf_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


udf_file=$file_import/HiveUDF-1.0-shaded.jar

[[ -z ${ftp_user} ]] && read -p "请输入用户名：" ftp_user
[[ -z ${ftp_pass} ]] && read -p "请输入密码：" -s ftp_pass
[[ -z ${ftp_dir} ]] && ftp_dir=${ftp_user}

ftp -n << eof
open ${ftp_host}
user ${ftp_user} ${ftp_pass}
binary
cd ${ftp_dir}
lcd ${file_import}
prompt off
get HiveUDF-1.0-shaded.jar HiveUDF-1.0-shaded.jar
close
bye
eof

[[ -f ${udf_file} ]] && $hdfs -put -f ${udf_file} $cos_uri/user/auxlib || error "文件 ${udf_file} 不存在！"
