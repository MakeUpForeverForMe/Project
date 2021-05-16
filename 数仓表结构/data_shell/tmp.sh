#!/bin/bash

. /etc/profile
. ~/.bash_profile

. /data/data_shell/lib/function.sh


aa="$([[ -n $1 ]] && cat "${1}")"

aa=$(select_data "${aa:-"$(
  mysql -P3306 -h'10.83.16.15' -u'root' -p'Ws2018!07@' -D'abs-core' -e "
    select distinct
      project_name,
      project_stage,
      null as asset_side,
      null as fund_side,
      null as year,
      null as term,
      null as remarks,
      project_full_name,
      asset_type,
      project_type,
      mode,
      project_time,
      project_begin_date,
      project_end_date,
      asset_pool_type,
      public_offer,
      data_source,
      create_user,
      create_time,
      update_time,
      project_id
    from t_project
    where 1 > 0
      and project_id not in ('PL202102010096')
  ;"
)"}")

impala-shell -f tmp.file
