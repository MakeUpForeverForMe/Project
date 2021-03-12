#!/bin/bash -e

. ${data_eagle_funds_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh


echo -e "${date_s_aa:=$(date +'%F %T')} 资金任务  开始 当前脚本进程ID为：$(pid)\n"

sh $data_manage -s $(date +%F) -e $(date +%F) -f $dm_eagle_hql/dm_eagle.eagle_funds_table.hql -a $funds

echo -e "${date_e_a1:=$(date +'%F %T')} 资金任务  中间任务 1  结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_e_a1" "$date_s_aa")\n\n"

tables=(
  dm_eagle.eagle_acct_cost
  dm_eagle.eagle_asset_change
  dm_eagle.eagle_asset_change_comp
  dm_eagle.eagle_asset_change_comp_t1
  dm_eagle.eagle_asset_change_t1
  dm_eagle.eagle_asset_comp_info
  dm_eagle.eagle_funds
  dm_eagle.eagle_repayment_detail
  dm_eagle.eagle_unreach_funds
)


for table in ${tables[@]};do
  export_file "select * from ${table};" ${table}.tsv &>> $log &
  # export_file "select * from ${table};" ${table}.tsv 2> /dev/null &
done

echo -e "${date_e_aa:=$(date +'%F %T')} 资金任务  结束 当前脚本进程ID为：$(pid)    用时：$(during "$date_e_aa" "$date_s_aa")\n\n"

