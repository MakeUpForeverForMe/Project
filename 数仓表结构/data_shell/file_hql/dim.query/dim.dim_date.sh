for year in {2015..2030}; do
  s_date=$(date -d "${year}-01-01" +%F)
  {
    while [[ "$(date -d "$s_date" +%s)" -le "$(date -d "${year}-12-31" +%s)" ]]; do
      for product in 'DIDI201908161538' 'pl00282'; do
        echo "${s_date},${product}" >> /root/data_shell/data_file/dim_new.dim_date.csv.${year}
      done
      s_date=$(date -d "$s_date +1 day" +%F)
    done
  } &
  sleep 1
done

for pid in $(jobs -p); do
  wait $pid
done


cat /root/data_shell/data_file/dim_new.dim_date.csv.20* > /root/data_shell/data_file/dim_new.dim_date.csv

rm -f /root/data_shell/data_file/dim_new.dim_date.csv.20*

sudo -u hive hdfs dfs -put -f /root/data_shell/data_file/dim_new.dim_date.csv /user/hive/warehouse/dim_new.db/dim_date


select * from dim_new.dim_date limit 10;

select count(1) as cnt from dim_new.dim_date;
