#!/usr/bin/env bash

. /etc/profile

root_dir=/data/data_shell

data_manage=$root_dir/bin/data_manage.sh
ods_cloud=$root_dir/file_hql/ods_cloud.query
ods=$root_dir/file_hql/ods.query

param=$root_dir/param_beeline

mail=$root_dir/conf_mail/data_receives_mail_ximing.config

log=$root_dir/log



#sh $data_manage -s 2017-06-01 -e 2021-03-22 -f $ods_cloud/ods.loan_info_inter.hql                                           -a $mail -n 10 &> $log/cloud_loan.log
#sh $data_manage -s 2020-12-02 -e 2021-03-22 -f $ods_cloud/ods.repay_schedule_inter.hql                                      -a $mail -n 10 &> $log/cloud_schedule.log



#sh $data_manage -s 2019-09-01 -e 2021-03-22 -f $ods/ods.order_info.hql                 -i $param/ods_new_s.param_ddhtgz.hql -a $mail -n 10 &> $log/order_ddhtgz.log
#sh $data_manage -s 2019-09-01 -e 2021-03-22 -f $ods/ods.repay_detail_ddhtgz.hql                                             -a $mail -n 10 &> $log/repay_detail_ddhtgz.log



#sh $data_manage -s 2020-06-02 -e 2021-03-22 -f $ods/ods.loan_info_inter.hql            -i $param/ods_new_s.param_lx.hql     -a $mail -n 10 &> $log/loan.log
#sh $data_manage -s 2020-06-02 -e 2021-03-22 -f $ods/ods.loan_info_inter.hql            -i $param/ods_new_s_cps.param_lx.hql -a $mail -n 10 &> $log/loan_cps.log

#sh $data_manage -s 2020-06-02 -e 2021-03-22 -f $ods/ods.repay_schedule_inter.hql       -i $param/ods_new_s.param_lx.hql     -a $mail -n 20 &> $log/schedule.log
#sh $data_manage -s 2020-06-02 -e 2021-03-22 -f $ods/ods.repay_schedule_inter.hql       -i $param/ods_new_s_cps.param_lx.hql -a $mail -n 30 &> $log/schedule_cps.log

#sh $data_manage -s 2020-06-02 -e 2021-03-22 -f $ods/ods.order_info.hql                 -i $param/ods_new_s.param_lx.hql     -a $mail -n 30 &> $log/order_lx.log
#sh $data_manage -s 2020-06-02 -e 2021-03-22 -f $ods/ods.order_info.hql                 -i $param/ods_new_s_cps.param_lx.hql -a $mail -n 30 &> $log/order_lx_cps.log

sh $data_manage -s 2020-06-02 -e 2021-03-22 -f $ods/ods.repay_detail_lx.hql            -i $param/ods_new_s.param_lx.hql     -a $mail -n 30 &> $log/repay_detail_lx.log
sh $data_manage -s 2020-06-02 -e 2021-03-22 -f $ods/ods.repay_detail_lx.hql            -i $param/ods_new_s_cps.param_lx.hql -a $mail -n 30 &> $log/repay_detail_lx_cps.log



# ---------------------------------- ods_new_s.param_lx.hql ---------------------------------- #
# set hivevar:product_id=
#   select distinct product_id
#   from (
#     select
#       max(if(col_name = 'channel_id',col_val,null)) as channel_id,
#       max(if(col_name = 'product_id',col_val,null)) as product_id
#     from dim.data_conf
#     where col_type = 'ac'
#     group by col_id
#   ) as tmp
#   where channel_id = '0006'
# ;
# set hivevar:db_suffix=;
# set hivevar:tb_suffix=_asset;
# ---------------------------------- ods_new_s.param_lx.hql ---------------------------------- #

# ---------------------------------- ods_new_s_cps.param_lx.hql ---------------------------------- #
# set hivevar:product_id=
#   select distinct product_id
#   from (
#     select
#       max(if(col_name = 'channel_id',col_val,null)) as channel_id,
#       max(if(col_name = 'product_id',col_val,null)) as product_id
#     from dim.data_conf
#     where col_type = 'ac'
#     group by col_id
#   ) as tmp
#   where channel_id = '0006'
# ;
# set hivevar:db_suffix=_cps;
# set hivevar:tb_suffix=;
# ---------------------------------- ods_new_s_cps.param_lx.hql ---------------------------------- #

# ---------------------------------- ods_new_s.param_ddhtgz.hql ---------------------------------- #
# set hivevar:product_id='DIDI201908161538','001601','001602','001603','001701','001702';
# set hivevar:db_suffix=;
# set hivevar:tb_suffix=;
# ---------------------------------- ods_new_s.param_ddhtgz.hql ---------------------------------- #
