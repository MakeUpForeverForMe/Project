#!/usr/bin/env bash

import_file_dir=/data/data_shell/file_import
hdfs_file_dir=cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db

for year in {2001..2050}; do
  s_date=$(date -d "${year}-01-01" +%F)
  {
    while [[ "$(date -d "$s_date" +%s)" -le "$(date -d "${year}-12-31" +%s)" ]]; do
      echo "${s_date}" >> ${import_file_dir}/dim.dim_date.csv.${year}
      s_date=$(date -d "$s_date +1 day" +%F)
    done
  } &
  sleep 0.1
done

for pid in $(jobs -p); do
  wait $pid
done


cat ${import_file_dir}/dim.dim_date.csv.20* > ${import_file_dir}/dim.dim_date.csv

rm -f ${import_file_dir}/dim.dim_date.csv.20*


sudo -u hadoop hdfs dfs -put -f ${import_file_dir}/dim.dim_date.csv ${hdfs_file_dir}/dim_date
