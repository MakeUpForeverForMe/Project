#!/bin/bash -e

# didi_account
# host=10.80.16.9
# user=root
# pass=!mAkJTMI%lH5ONDw
# db=ecasdb

# fj_account
# host=10.80.16.9
# user=root
# pass=!mAkJTMI%lH5ONDw
# db=ccsdb

# ht_journal
# host=10.80.16.10
# user=root
# pass=LZWkT2lxze6x%1V(
# db=nmsdb

# didi_rmas
# host=10.80.16.87
# user=root
# pass=wH7Emvsrg&V5
# db=rmas_prod

# fj_journal
# host=10.80.16.5
# user=root
# pass=Xfx2018@)!*
# db=asset_status

# 10.80.16.5   星连
# 10.80.16.9   核心 旧
# 10.80.16.10  核心 新
# 10.80.16.87  核心 催收

# 31、汇通核心：ht_account

# 22、凤金日志：fj_journal
declare -A connects['10.80.16.5:asset_status']='root:Xfx2018@)!*:t_real_param-'
# 11、滴滴核心：didi_account
declare -A connects['10.80.16.9:ecasdb']='root:!mAkJTMI%lH5ONDw:ecas_loan,ecas_msg_log-,ecas_order,ecas_customer_lmt,ecas_repay_hst,ecas_repay_schedule,ecas_bind_card,ecas_bind_card_change,ecas_order_hst,ecas_customer-'
# 21、凤金核心：fj_account
declare -A connects['10.80.16.9:ccsdb']='root:!mAkJTMI%lH5ONDw:ccs_loan,ccs_order,ccs_order_hst,ccs_plan,ccs_repay_hst,ccs_repay_schedule,ccs_txn_hst'
# 32、汇通日志：ht_journal
declare -A connects['10.80.16.10:nmsdb']='root:LZWkT2lxze6x%1V(:nms_interface_resp_log-'
# 12、滴滴催收：didi_rmas
declare -A connects['10.80.16.87:rmas_prod']='root:wH7Emvsrg&V5:rmas_call_message_work,rmas_case_main,rmas_coll_promise,rmas_coll_rec,rmas_derate_apply,rmas_early_repay_check,rmas_message_work,rmas_sub_case_info,rmas_sub_case_out_info'

hive_db=ods

hiveserver2=10.80.1.47
