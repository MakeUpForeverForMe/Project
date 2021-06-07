#!/bin/bash -e
jarFile=/usr/local/service/hadoop/share/hadoop/yarn/lib/spark-2.4.3-yarn-shuffle.jar
sourceFile=/usr/local/service/hadoop/share/hadoop/yarn/spark-2.4.3-yarn-shuffle.jar

# -f 参数判断 $file 是否存在
if [ ! -f "$jarfile" ]; then
  echo `sudo -u hadoop cp -v $sourceFile /usr/local/service/hadoop/share/hadoop/yarn/lib/`
  #chown hadoop $jarFile
  #chown hadoop $jarFile
else
  echo 'file existed'
fi
