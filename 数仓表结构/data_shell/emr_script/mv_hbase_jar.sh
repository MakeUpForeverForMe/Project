#!bin/bash

jarFile=/usr/local/service/hbase/lib/._htrace-core-3.1.0-incubating.jar
# -f 参数判断 $file 是否存在
if [ -f "$jarfile" ]; then
  echo `mv /usr/local/service/hbase/lib/._htrace-core-3.1.0-incubating.jar ~/`
else
  echo 'file not existed'
fi


