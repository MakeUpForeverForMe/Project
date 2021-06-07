local_file_dir='/data/data_shell/file_import/abs_cloud/bag_due_bill_no-csv'
hdfs_file_dir='cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db/bag_due_bill_no'


mysql -P3306 -h'10.80.16.21' -u'root' -p'EXYeaGVQZpsr@CR&' -D'abs-core' -s -N -e '
select distinct
  project_id,
  serial_number,
  0,
  0,
  asset_bag_id
from t_basic_asset
where asset_bag_id is not null
-- limit 10
;' | awk '{
  # print local_file_dir

  bag_file_dir = local_file_dir"/bag_id="$5

  # print bag_file_dir

  cmd = "[[ ! -d "bag_file_dir" ]] && mkdir "bag_file_dir

  # print cmd

  # print bag_file_dir"/"$5".csv"

  system(cmd)

  print $1","$2","$3","$4","$5 > bag_file_dir"/"$5".csv"
}' local_file_dir=$local_file_dir


sudo -u hadoop hdfs dfs -put -f $local_file_dir/* $hdfs_file_dir

beeline -n hadoop -u "jdbc:hive2://10.80.0.46:2181,10.80.0.255:2181,10.80.1.113:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -e '
MSCK REPAIR TABLE dim.bag_due_bill_no;
'
