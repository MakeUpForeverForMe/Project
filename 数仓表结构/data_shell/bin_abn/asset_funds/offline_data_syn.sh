#! /bin/bash

source /etc/profile
export LANG=zh_CN.UTF-8
export PYTHON_EGG_CACHE=/tmp/.python-eggs
############同步数据脚本需要参数###########
bus_type=$1
hive_database=$2
table_name=$3
echo "table_name=$table_name"
is_partitioned=$4
d_date=`date -d"1 day ago" +%Y-%m-%d`
echo "the table name is $table_name"
#date=`impala-shell -q "use ods;select business_date from temp_system_status_dd where org='15601';exit;"`
#d_date='2099-04-24'
echo ${date}
echo ${d_date}
#OLD_IFS="$IFS" 
#IFS="|" 
#arr=($date) 
#IFS="$OLD_IFS" 
#echo ${arr[3]}
#d_date=`echo ${arr[3]} | sed 's/ //g'`
########将需要的脚本下载到本地##########
hadoop fs -get /user/hadoop/sqoop/data_syn/create.py .
##########判断业务类型，根据业务类型，判断数据库###########
#11、滴滴、汇通核心 12、滴滴催收 21、凤金核心 22、凤金日志  32、汇通日志 33、汇通资产 34、汇通信诚国银一期 41、流水转接 51、乐信国民核心
#61、风控接口 #62、风控黑名单 #63、资信调查 71、乐信中铁核心 81、乐信国民二期 91、乐信国民三期
if [ ${bus_type} -eq 11 ] || [ ${bus_type} -eq 51 ] || [ ${bus_type} -eq 71 ] || [ ${bus_type} -eq 81 ] || [ ${bus_type} -eq 34 ] || [ ${bus_type} -eq 91 ];then
   file='didi_account'
elif [ ${bus_type} -eq 12 ];then
   file='didi_rmas'
elif [ ${bus_type} -eq 21 ];then
   file='fj_account'
elif [ ${bus_type} -eq 22 ];then
   file='fj_journal'
elif [ ${bus_type} -eq 32 ];then
   file='ht_journal'
elif [ ${bus_type} -eq 33 ];then
   file='ht_assets'
elif [ ${bus_type} -eq 41 ];then
   file='fund_trans'
elif [ ${bus_type} -eq 61 ];then
   file='risk_inter'
elif [ ${bus_type} -eq 62 ];then
   file='risk_blacklist'
elif [ ${bus_type} -eq 63 ];then
   file='credit_investigation'
else
   echo '业务类型输入错误，不存在此业务类型'
fi

#######乐信项目获取产品ID并组装,用来分开数据分开抽取#########
if [ ${bus_type} -eq 51 ];then
   product='lxgm_product'  ########乐信国民的产品ID
elif [ ${bus_type} -eq 71 ];then
   product='lxzt_product' ##########乐信中铁的产品ID
elif [ ${bus_type} -eq 81 ];then
   product='lxgm_product2'
elif [ ${bus_type} -eq 91 ];then
   product='lxgm_product3'
elif [ ${bus_type} -eq 11 ];then
   product='ddht_product'
elif [ ${bus_type} -eq 34 ];then
   product='htgy_product'
else 
   product='lx_product' ########乐信下所有产品ID，用来剔除乐信数据
fi


########组装文件中的产品ID#############
hadoop fs -get /user/hadoop/sqoop/data_syn/${product} .
while read line;do var="$var,'$line'";done < ${product}

product_list=`echo $var|sed 's/^,//g;s/,$//g'`
echo ${product_list}

#########乐信产品对应分区########
if [ ${bus_type} -eq 51 ];then
   partition='lx'  ########乐信国民的产品分区名称
elif [ ${bus_type} -eq 71 ];then
   partition='lxzt' #######乐信中铁的产品分区名称
elif [ ${bus_type} -eq 81 ];then
   partition='lx2'
elif [ ${bus_type} -eq 91 ];then
   partition='lx3'
elif [ ${bus_type} -eq 11 ];then
   partition='ddht'
elif [ ${bus_type} -eq 34 ];then
   partition='htgy'
else
   echo "不存在改产品"
fi
echo ${partition}

#####将数据库信息文件下载到本地########
hadoop fs -get /user/hadoop/sqoop/data_syn/${file} .

host=`cat ${file} | grep 'host' | cut -d '=' -f 2`
echo ${host}
user=`cat ${file} | grep 'user' | cut -d '=' -f 2`
echo ${user}
password=`cat ${file} | grep 'pass' | cut -d '=' -f 2`
echo ${password}
dbname=`cat ${file} | grep 'db' | cut -d '=' -f 2`
echo ${dbname}
############判断hive表是否存在##########
#hive -e"
#     desc ${hive_database}.${table_name};
#" 2>&1 | grep 'Table not found'


##########判断表是否存在，不存在则从mysql上获取对应表结构,并且执行相关建表语句#########
#rtstatus=$?
#if [ $rtstatus -ne 0 ]; then
#   echo "hive表已存在!"
#else
#   python create.py ${hive_database} ${table_name} ${host} ${user} ${password} ${dbname} ${is_partitioned}
#fi

###########获取当前mysql表记录数############
#mysql_count=`mysql -u${user} -p${password} -h${host} -P3306 -e "select count(0) count from ${dbname}.${table_name};" | grep -v 'count'`
#echo ${mysql_count}

###########sqoop同步数据到hive中############

hive -e "alter table ${hive_database}.${table_name} drop partition(d_date='${d_date}')"
hadoop fs -rm -r cosn://bigdatacenter-sit-1253824322/user/hadoop/warehouse/stage.db/t_transaction_blend_record/*
sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
-D mapreduce.map.memory.mb=4096 \
-D mapreduce.reduce.memory.mb=4096 \
--connect "jdbc:mysql://${host}:3306/${dbname}?useUnicode=true&characterEncoding=utf8&useSSL=false" \
--username ${user} \
--password ${password} \
--query "select *,'${d_date}' as D_DATE from ${table_name} where  \$CONDITIONS" \
--hcatalog-database ${hive_database} --hcatalog-table ${table_name} \
--hcatalog-storage-stanza "STORED AS parquet" \
--split-by id \
--null-string '\\N' --null-non-string '\\N' \
-m 1

if [ $? -eq 0 ];then
    exit 0
else
   exit 1
fi
###########获取同步到hive中的记录数###########
#if [ ${is_partitioned} = 'Y' ];then
#hive_count=`hive -e "select count(0) from ${hive_database}.${table_name} where d_date='${d_date}';exit;"`
#else
#hive_count=`hive -e "select count(0) from ${hive_database}.${table_name};exit;"`
#fi
#echo ${hive_count}

###########比较记录数大小，判断数据同步是否成功############

#if [ ${mysql_count} -eq 0 ];then
#   echo "mysql中数据为0"
#   exit 0
#elif [ ${hive_count} -eq 0 ];then
#   echo "hive表${hive_database}.${table_name}的${d_date}分区中数据量为0"
#   exit 0
#elif [ ${hive_count} -eq ${mysql_count} ];then
#   echo "${hive_database}.${table_name}的${d_date}分区数据同步成功，数据量为${hive_count}"
#   exit 0
#else
#   echo "hive中与mysql中数据量不一致，请查明原因"
#   exit 0
#fi