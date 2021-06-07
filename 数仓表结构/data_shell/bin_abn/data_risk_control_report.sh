#!/bin/bash -e

. ${data_ods_new_s_dir:=$(cd `dirname "${BASH_SOURCE[0]}"`;pwd)}/../conf_env/env.sh
. $lib/function.sh

execute_sh_m(){

      #  sh $data_manage -s $1 -e $2 -f $eagle_hql/eagle.eagle_loan_apply_stat_m.hql                                               -n 1 -a $rd &
      #  sh $data_manage -s $1 -e $2 -f $eagle_hql/eagle.eagle_loan_base_stat_loan_num_m.hql                                       -n 1 -a $rd &
        
      #  sh $data_manage -s $1 -e $2 -f $eagle_hql/eagle.eagle_post_loan_scene_m.hql         -i $param_dir/dw.param.hql            -n 1 -a $rd &
      #  sh $data_manage -s $1 -e $2 -f $eagle_hql/eagle.eagle_post_loan_scene_m.hql         -i $param_dir/dw_cps.param.hql        -n 1 -a $rd &
                                                                                                                                  
        sh $data_manage -s $1 -e $2 -f $eagle_hql/eagle.eagle_post_loan_scene_static_m.hql  -i $param_dir/dw.param.hql            -n 1 -a $rd &
        sh $data_manage -s $1 -e $2 -f $eagle_hql/eagle.eagle_post_loan_scene_static_m.hql  -i $param_dir/dw_cps.param.hql        -n 1 -a $rd &
}

time_processing(){
        start_sec=$1
        end_sec=$2
        while [ $start_sec -le $end_sec ]; do

            day_curr=`date -d @$start_sec +%Y-%m-%d`
            month_curr=`date -d @$start_sec +%Y-%m`
            
            start_date=`date -d"${month_curr}-01" "+%Y-%m-01"`      #月份开始日期
            tmp_dt=`date -d"${month_curr}-01 +1 months" "+%Y-%m-01"`   
            end_date=`date -d "${tmp_dt} -1 day" "+%Y-%m-%d"`       #月份结束日期

            echo $month_curr $start_date $end_date
            ## 这里放要处理的过程
            execute_sh_m $end_date $end_date
            ## 一次结束重置月份
            let start_sec=`date -d "${month_curr}-01 +1 months" +%s`
        done  

}

s_date=$1
e_date=$2
base_file_name=$(basename "${BASH_SOURCE[0]}")

[[ -z $s_date || -z $e_date ]] && {
  echo "$(date +'%F %T') ${base_file_name} 请输入 开始和结束日期 参数！"
  exit 1
}

log=$log/${base_file_name}.${e_date}.log

echo -e "${date_s_aa:=$(date +'%F %T')} Emr 风控报表 eagle 开始 当前脚本进程ID为：$(pid)\n" &>> $log

#日任务
       # sh $data_manage -s ${s_date} -e ${e_date} -f $eagle_hql/eagle.eagle_loan_apply_stat_d.hql                                         -n 1 -a $rd &
       # sh $data_manage -s ${s_date} -e ${e_date} -f $eagle_hql/eagle.eagle_loan_base_stat_loan_num_d.hql                                 -n 1 -a $rd &
       # sh $data_manage -s ${s_date} -e ${e_date} -f $eagle_hql/eagle.eagle_post_loan_scene_d.hql          -i $param_dir/dw.param.hql     -n 1 -a $rd &
       # sh $data_manage -s ${s_date} -e ${e_date} -f $eagle_hql/eagle.eagle_post_loan_scene_d.hql          -i $param_dir/dw_cps.param.hql -n 1 -a $rd &

#wait_jobs

#周任务 确定当前日是否是周末
weekend_date=${s_date}
while [[ "$(date -d "${weekend_date}" +%s)" -le "$(date -d "${e_date}" +%s)" ]]; do

    if [ $(date -d "${weekend_date}" +%w) = 0 ]; then
       # sh $data_manage -s ${weekend_date} -e ${weekend_date} -f $eagle_hql/eagle.eagle_loan_apply_stat_w.hql                                               -n 1 -a $rd &
       # sh $data_manage -s ${weekend_date} -e ${weekend_date} -f $eagle_hql/eagle.eagle_loan_base_stat_loan_num_w.hql                                       -n 1 -a $rd &
        
       # sh $data_manage -s ${weekend_date} -e ${weekend_date} -f $eagle_hql/eagle.eagle_post_loan_scene_w.hql            -i $param_dir/dw.param.hql         -n 1 -a $rd &
       # sh $data_manage -s ${weekend_date} -e ${weekend_date} -f $eagle_hql/eagle.eagle_post_loan_scene_w.hql            -i $param_dir/dw_cps.param.hql     -n 1 -a $rd &
                                                                                                                         
        sh $data_manage -s ${weekend_date} -e ${weekend_date} -f $eagle_hql/eagle.eagle_post_loan_scene_static_w.hql     -i $param_dir/dw.param.hql         -n 1 -a $rd &
        sh $data_manage -s ${weekend_date} -e ${weekend_date} -f $eagle_hql/eagle.eagle_post_loan_scene_static_w.hql     -i $param_dir/dw_cps.param.hql     -n 1 -a $rd &       
    fi

  weekend_date=$(date -d "$weekend_date +1 day" +%F)
done

wait_jobs

#月任务 确定当前日是否是月末

#日期当月月末
if [[ "$(date -d "${s_date}" "+%Y-%m")" = "$(date -d "${e_date}" "+%Y-%m")" && $(date -d "${e_date} tomorrow" '+%d') = '01' ]];then
        execute_sh_m ${e_date} ${e_date}
    #日期当月非月末
    elif [[ "$(date -d "${s_date}" "+%Y-%m")" = "$(date -d "${e_date}" "+%Y-%m")" && $(date -d "${e_date} tomorrow" '+%d') != '01' ]];then
        echo "当月非月末，不进行跑批"
    #多个月且结束日期为月末
    elif [[ "$(date -d "${s_date}" "+%Y-%m")" != "$(date -d "${e_date}" "+%Y-%m")" && $(date -d "${e_date} tomorrow" '+%d') = '01' ]];then      
        start_sec=`date -d "${s_date}-01" +%s`
        end_sec=`date -d "${e_date}-01" +%s`
        time_processing $start_sec $end_sec
    #多个月且结束日期为非月末
    elif [[ "$(date -d "${s_date}" "+%Y-%m")" != "$(date -d "${e_date}" "+%Y-%m")" && $(date -d "${e_date} tomorrow" '+%d') != '01' ]];then
        start_sec=`date -d "$(date -d "${s_date}" "+%Y-%m")-01" +%s`
        end_sec=`date -d "$(date -d "${e_date}" "+%Y-%m")-01 -1 months" +%s`
        time_processing $start_sec $end_sec 
fi

wait_jobs




echo -e "${date_e_aa:=$(date +'%F %T')} Emr 风控报表 eagle  结束 当前脚本进程ID为：$(pid)    用时：${during_time:=$(during "$date_e_aa" "$date_s_aa")}\n\n" &>> $log

$mail $pm_rd 'Emr 风控报表 eagle 执行结束' "
  执行开始日期： $s_date
  执行结束日期： $e_date
  执行开始时间： $date_s_aa
  执行结束时间： $date_e_aa
  执行执行时长： $during_time
" &>> $log

