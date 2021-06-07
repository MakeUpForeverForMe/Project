#!/bin/bash
mysql_user=wscxq
mysql_password="weshare20210524"
mysql_host="10.83.16.31"
mysql_table_name="loan_monitor.duration_request"
export_file_path=/data/data_shell/file_export
disk_file_name=wind_data_export_table__htgy_2021-03-01.tsv
sqoop_table_list=(loan_monitor.duration_result loan_monitor.duration_request)
beeline_commad='beeline -u jdbc:hive2://10.83.1.157:7001 -n hadoop --color=true --hiveconf hive.resultset.use.unique.column.names=false'
#beeline_commad='beeline -n hadoop -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --color=true --hiveconf hive.resultset.use.unique.column.names=false'
during_query_status(){
  response_data=$(mysql -u ${mysql_user} -p'${mysql_password}' -h "${mysql_host}" -e "select request_id,file_path,state,error_msg from ${mysql_table_name} where file_path='${export_file_path}' and file_name='${disk_file_name}'" 2>>/dev/null)
  status=$(echo "${response_data}" | grep -v grep | grep state)
  file_path_result=$(echo "${response_data}" | grep -v grep | grep file_path)
   request_id=$(echo "${response_data}" | grep -v grep | grep request_id)
   ##wind_data_export_table_WS10043190001_2020-03-01.tsv
  # shellcheck disable=SC2209
  project_id=echo "${file_path_result}" | awk -F "_" '{print $5}'
  # shellcheck disable=SC2209
  biz_date=echo "${file_path_result}" | awk -F "_" '{print $6}' | awk -F "." '{print $1}'
  ##执行sqoop抽数任务
  execue_sqoop biz_date project_id  request_id
}


execue_sqoop(){
  for table_name in "${sqoop_table_list[@]}"

  do
    query_sql="select * '$1' as d_date from $table_name  and \$CONDITIONS"
    [[ $table_name == "loan_monitor.duration_result" ]] && {
      query_sql="select * '$1' as d_date,'$2' as project_id from $table_name where request_id='$3' and \$CONDITIONS"
    }
    ##判断表是否存在
    sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    -D mapreduce.map.memory.mb=4096 \
    --connect "jdbc:mysql://${mysql_host}:3306/loan_monitor?useUnicode=true&characterEncoding=utf8" \
    --username ${mysql_user} \
    --password "${mysql_password}" \
    --query "${query_sql}" \
    --hcatalog-database stage --hcatalog-table ${table_name} \
    --hcatalog-storage-stanza "STORED AS parquet" \
    --split-by org \
    --null-string '\\N' --null-non-string '\\N' \
    --hive-overwrite \
    --m 1
  done;

}



