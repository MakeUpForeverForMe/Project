#!/usr/bin/expect
##接收产品号
ST9=$1;
cluster_type="pro";

#beeline_commad='beeline -n hadoop -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --color=true --hiveconf hive.resultset.use.unique.column.names=false'
##1）数据中台在风控每个月跑存续去监控数据之前，把国银-汇通、国银大搜车两个项目的①项目编号、②借据号、③三要素、④是否已结清  的数据给风控，告知这些已结清的数据是可以出表的；
##2）数据中台会以FTP推文件的形式给；并在推送后给风控发送通知。
[[ -z $ST9 ]] && {
  echo "biz_date  is not allow empty!"
  exit 1
}

case $cluster_type in
"test")
beeline_commad='beeline -u jdbc:hive2://10.83.1.157:7001 -n hadoop --color=true --hiveconf hive.resultset.use.unique.column.names=false'
ftp_user=bigdataCenter
ftp_host=10.83.0.46
ftp_password="UnJxZvuQARHA^mq9"
ftp_data_dir="/upload/wind_data/xingyun_data"
service_path="http://10.83.0.132:8190/rms/duration/execute"
mysql_user=wscxq
mysql_password="weshare20210524"
mysql_host="10.83.16.31"
key='3ftt$#'
;;
"pro")
beeline_commad='beeline -n hadoop -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" --color=true --hiveconf hive.resultset.use.unique.column.names=false'
ftp_user=bigdataCenter
ftp_host=10.80.3.61
ftp_password="rbegxC0o^euoP5Ah"
ftp_data_dir="/upload/wind_data/xingyun_data"
service_path="http://10.80.1.1:8190/rms/duration/execute"
mysql_user=wscxq
mysql_password="weshare202106091744"
mysql_host="10.80.16.65"
key='8Wgnk9xqg6ce1Cea'
;;
esac
##请求参数处理
export_file_path=/data/data_shell/file_export
lib_path=/data/data_shell/lib

## 项目号配置信息
project_id_list=('CL202012280092')
#project_id_list=('CL202012280092' 'cl00297' 'cl00306' 'cl00309' 'CL201911130070' 'CL202002240081')
mysql_table_name="loan_monitor.duration_request"
sqoop_table_list=(duration_result)
current__month=$(date +'%Y-%m')
today="$(date +'%Y-%m')-01"
##导出数据 上传数据 到ftp服务器
exportToDisk(){
 for project_id in "${project_id_list[@]}"
do
   disk_file_name=wind_data_export_xingyun_data_${project_id}_${ST9}.csv
   query_mysql="select id,file_path,state,error_msg,file_name from ${mysql_table_name} where file_path='${ftp_data_dir}' and file_name='${disk_file_name}' and DATE_FORMAT(create_time,'%Y-%m')='${current__month}'  order by update_time desc limit 1"
   echo "query_mysql:${query_mysql}"
   response_data=$(mysql -u"${mysql_user}" -p"${mysql_password}" -h"${mysql_host}" -e "${query_mysql}" 2>>/dev/null | grep -v id |awk -F" " '{print $1,$3,$4}')
    echo $response_data
    request_id=$(echo "${response_data}" | grep -v grep | awk -F " " '{print $1}')
    state=$(echo "${response_data}" | grep -v grep | awk -F " " '{print $2}')
    if [ -z "$response_data" ] ; then
      echo "未进行调用开始调用${project_id},biz_date:${ST9}：数据导出目录:${export_file_path},文件名:${disk_file_name}"
      $beeline_commad  \
        --showHeader=true \
        --outputformat=csv2 --nullemptystring=true \
        -e "select project_id,due_bill_no,idcard_no,mobie,name,loan_status from eagle.wind_data_export_table where biz_date='${today}' and project_id in ('${project_id}')" 1>${export_file_path}/${disk_file_name} 2>>output.txt
        ftp_put ${disk_file_name}
        [[ -f "${export_file_path}/${disk_file_name}" ]] &&{
           echo "删除-三天前-数据------------------- find $export_file_path/wind_data_export_xingyun_data*.csv -type f -mtime +3 -delete "
          find $export_file_path/wind_data_export_xingyun_data*.csv -type f -mtime +3 -delete
        }
        executHttpPost ${disk_file_name}
    elif [ ${state} -eq 0 ] || [ ${state} -eq 3 ] ;then
      echo "重新发起调用：${project_id},biz_date:${ST9},state:${state}"
      executHttpPost ${disk_file_name}
    elif [ ${state} -eq 2 ] ; then
       ## 判断数据是否抽取成功 抽取成功则不进行数据抽取
       hive_num=$($beeline_commad  --showHeader=false --outputformat=csv2   -e "select count(1) from stage.duration_result where d_date='${today}' and project_id='${project_id}'" 2>>/dev/null )
       echo "处理成功!${project_id},biz_date:${today},${request_id},hive数据条数为${hive_num} "
       if [ $hive_num -eq 0  ] || [ -z "$hive_num"  ];then
       execue_sqoop ${today} ${project_id} ${request_id}
       else
          echo "数据已抽取成功!${project_id},biz_date:${today},${request_id},hive数据条数为${hive_num}"
       fi;
    else
        echo "处理中ing:${project_id},biz_date:${today},state:${state}"
    fi;
done;
}




##传FTP
ftp_put(){
  [[ ! -f "${export_file_path}/${1}" ]] && {
    echo "file not exists !"
    exit 1
  }
  [[ $(wc -l ${export_file_path}/${1} | awk -F " " '{print $1}') == 1 ]] &&{
     echo "file is empty !"
    exit 1
  }
  ftp_upload_file ${1}
}


##上传数据 到SFTP
ftp_upload_file(){
echo "put ${export_file_path}/${1} ${ftp_data_dir}\r"
expect <<EOF
set timeout 10
spawn sftp ${ftp_user}@${ftp_host}
expect "Are you sure you want to continue connecting (yes/no)?" {send "yes\r\n"}
expect "*password" {send "${ftp_password}\r\n"}
expect "*Please type 'yes' or 'no':" {send "yes\r\n"}
expect "sftp>"
send "mkdir ${ftp_data_dir}\r"
expect "*Please type 'yes' or 'no':" {send "yes\r\n"}
expect "sftp>"
send "put ${export_file_path}/${1} ${ftp_data_dir}\r"
expect "*Please type 'yes' or 'no':" {send "yes\r\n"}
expect "sftp>"
send "bye\r\n"
EOF
}

executHttpPost(){
   /usr/bin/java -cp ${lib_path}/monitor-1.0-SNAPSHOT-shaded.jar com.weshare.bigdata.wind.WindController "${key}" "${service_path}" "${ftp_data_dir}" ${1}
### response=$(curl -H "Content-type: application/json" -X POST -d '{"filePath":"${ftp_date_dir}","fileName":"${disk_file_name}"}' ${test_service_path})
### echo "curl -H "Content-type: application/json" -X POST -d '{"filePath":"${ftp_date_dir}","fileName":"${disk_file_name}"}' ${test_service_path}"
###  #echo "${response}"
###  status=$(echo ${response} | sed 's/,/\n/g' | grep status |awk -F ":" '{print $2}')
###  message=$(echo ${response} | sed 's/,/\n/g' | grep message |awk -F ":" '{print $2}')
###  # shellcheck disable=SC1073
###  # shellcheck disable=SC1073
###  [[ "${status}" != "200" ]]  &&{
###    echo  "调用服务失败，失败信息如下:message"
###  }
}
execue_sqoop(){
  for table_name in "${sqoop_table_list[@]}"
  do

      query_sql="select *, '$1' as d_date,'$2' as project_id from loan_monitor.$table_name where request_id='$3' and \$CONDITIONS"
    ##判断表是否存在
    sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
    -D mapreduce.map.memory.mb=4096 \
    --connect "jdbc:mysql://${mysql_host}:3306/loan_monitor?useUnicode=true&characterEncoding=utf8" \
    --username ${mysql_user} \
    --password "${mysql_password}" \
    --query "${query_sql}" \
    --hcatalog-database stage --hcatalog-table ${table_name} \
    --hcatalog-storage-stanza "STORED AS parquet" \
    --split-by id \
    --null-string '\\N' --null-non-string '\\N' \
    --hive-overwrite \
    --m 3
  done;
}
exportToDisk