##################### 临时库清理7天
#!/usr/bin/env bash

. ${data_manage_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh

set -e
echo '### 抽取已变更7天的表名'
result=$(mysql -h'10.80.16.75' -p3306 -u'bgp_admin' -p'U3$AHfp*a8M&'  -Dmetastore -s -N -e  "SELECT TBL_NAME from (
    SELECT
        t.TBL_NAME,
        p.PARAM_KEY,
        p.PARAM_VALUE,
        date_format(now(),'%Y-%m-%d') as today,
        from_unixtime(p.PARAM_VALUE) as lst_day,
        TIMESTAMPDIFF(DAY,from_unixtime(p.PARAM_VALUE),date_format(now(),'%Y-%m-%d')) as days
    FROM
        ( SELECT TBL_ID, TBL_NAME FROM TBLS WHERE DB_ID = '92985' ) t
        INNER JOIN TABLE_PARAMS p ON t.TBL_ID = p.TBL_ID
    WHERE
        p.PARAM_KEY = 'transient_lastDdlTime'
) tmp where tmp.days >=7;" )

echo '### 初始化清理表sql'
sql_run=$(echo "$result" | sed 's/^/drop table if exists &/; s/$/;&/')

echo '### 执行清理表sql'
#echo \
$beeline -e "use tmp_drop_tables;set role admin;" -e "$sql_run"
echo '### 运行成功'
