#!/bin/bash

. ${ftp_udf_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh


ftp -n << eof
open 10.90.0.5
user ximing.wei wxm.123..
binary
cd /ximing.wei
lcd /data/data_shell/file_import
prompt
get HiveUDF-1.0-shaded.jar HiveUDF-1.0-shaded.jar
close
bye
eof

$hdfs -put -f $file_import/HiveUDF-1.0-shaded.jar $hdfs_root_dir/user/auxlib
