#!/bin/bash


# -------------------- 基础函数 -------------------- #
# 去除数据前后的字符（默认空格等）
trim(){ echo $1 | sed "s/^${2:-\s}*//; s/${2:-\s}*$//"; }


# 大小写转换
lowe_case(){ echo $1 | tr [A-Z] [a-z]; } # 小写
high_case(){ echo $1 | tr [a-z] [A-Z]; } # 大写


# 参数解析(analy)
a_a(){ echo ${1//@/ }; } # @符  at            a
b_a(){ echo ${1//-/ }; } # 横杠 bar           b
c_a(){ echo ${1//:/ }; } # 冒号 colon         c
e_a(){ echo ${1//=/ }; } # 等号 equal         e
f_a(){ echo ${1//;/ }; } # 分号 fenhao        m
m_a(){ echo ${1//,/ }; } # 逗号 comma         m
p_a(){ echo ${1//./ }; } # 点   point         p
u_a(){ echo ${1//_/ }; } # 下划线 underline   u
v_a(){ echo ${1//|/ }; } # 竖线 vertical      v
s_a(){ echo ${1//\// }; } # 斜杠 slash        s


# 获取字符串的某一部位
# 逗号   comma      m  ,
# 点     point      p  .
# 斜杠   slash      s  /
# 下划线 underline  u  _
# 空格   space      c
# @符    at         a  @
# 等号   equal      e  =

p_r_l(){ echo ${1%.*}; }  # 取最右侧的 点     的左侧内容
a_r_l(){ echo ${1%@*}; }  # 取最右侧的 @符    的左侧内容
s_r_l(){ echo ${1%/*}; }  # 取最右侧的 斜杠   的左侧内容
m_r_l(){ echo ${1%,*}; }  # 取最右侧的 逗号   的左侧内容
c_r_l(){ echo ${1% *}; }  # 取最右侧的 空格   的左侧内容
e_r_l(){ echo ${1%=*}; }  # 取最右侧的 等号   的左侧内容
u_r_l(){ echo ${1%_*}; }  # 取最右侧的 下划线 的左侧内容

p_l_r(){ echo ${1#*.}; }  # 取最左侧的 点     的右侧内容
a_l_r(){ echo ${1#*@}; }  # 取最左侧的 @符    的右侧内容
s_l_r(){ echo ${1#*/}; }  # 取最左侧的 斜杠   的右侧内容
m_l_r(){ echo ${1#*,}; }  # 取最左侧的 逗号   的右侧内容
c_l_r(){ echo ${1#* }; }  # 取最左侧的 空格   的右侧内容
e_l_r(){ echo ${1#*=}; }  # 取最左侧的 等号   的右侧内容
u_l_r(){ echo ${1#*_}; }  # 取最左侧的 下划线 的右侧内容

p_r_r(){ echo ${1##*.}; } # 取最右侧的 点     的右侧内容
a_r_r(){ echo ${1##*@}; } # 取最右侧的 @符    的右侧内容
s_r_r(){ echo ${1##*/}; } # 取最右侧的 斜杠   的右侧内容
m_r_r(){ echo ${1##*,}; } # 取最右侧的 逗号   的右侧内容
c_r_r(){ echo ${1##* }; } # 取最右侧的 空格   的右侧内容
e_r_r(){ echo ${1##*=}; } # 取最右侧的 等号   的右侧内容
u_r_r(){ echo ${1##*_}; } # 取最右侧的 下划线 的右侧内容

p_l_l(){ echo ${1%%.*}; } # 取最左侧的 点     的左侧内容
a_l_l(){ echo ${1%%@*}; } # 取最左侧的 @符    的左侧内容
s_l_l(){ echo ${1%%/*}; } # 取最左侧的 斜杠   的左侧内容
m_l_l(){ echo ${1%%,*}; } # 取最左侧的 逗号   的左侧内容
c_l_l(){ echo ${1%% *}; } # 取最左侧的 空格   的左侧内容
e_l_l(){ echo ${1%%=*}; } # 取最左侧的 等号   的左侧内容
u_l_l(){ echo ${1%%_*}; } # 取最左侧的 下划线 的左侧内容


# 获取两个时间之间的时长（不区分两个参数大小，返回都是正数）
during(){
  s_time=$(date -d "$1" +%s)
  e_time=$(date -d "$2" +%s)
  [[ $s_time > $e_time ]] && u=$(( $s_time - $e_time )) || u=$(( $e_time - $s_time ))
  s=$(( $u % 60 )) u=$(( $u / 60 ))
  m=$(( $u % 60 )) u=$(( $u / 60 ))
  h=$(( $u % 24 ))
  d=$(( $u / 24 ))
  printf '%d天%02d时%02d分%02d秒' $d $h $m $s
}

# 当前脚本的PID
pid(){ echo $$;}

# 并行限制（默认可以并行数为5个）
p_opera(){
  p_num=${1:-5}
  pids=(${pids[@]:-} $!)
  echo '并行限制输出显示' '并行任务的数量：'${#pids[@]} '并行任务的数组下标：'${!pids[@]} '并行任务的PID：'${pids[@]}
  while [[ ${#pids[@]} -ge $p_num ]]; do
    pids_old=(${pids[@]})
    pids=()
    for p in "${pids_old[@]}";do
      [[ -d "/proc/$p" ]] && pids=(${pids[@]} $p)
    done
    sleep 0.01
  done
}

# 等待后台任务执行结束，再执行之后的操作
wait_jobs(){
  for pid in $(jobs -p); do
    wait $pid
  done
}














# -------------------- 功能函数 -------------------- #
# 配置表
data_conf(){
  conf_sys="${1}"
  conf_ins="${2}"
  conf_diff="${3}"
  $mysql -s -N -e "
    select
      conf_value
    from data_conf
    where 1 > 0
      and conf_sys = '${conf_sys}'
      and conf_ins = '${conf_ins}'
      and conf_diff = '${conf_diff}'
  "
}

date_conf_update_offset(){
  col_1="${1}"
  col_2="${2}"
  col_3="${3}"
  col_4="${4}"
  $mysql -s -N -e "
    REPLACE INTO data_conf VALUES ('${col_1}','${col_2}','${col_3}','${col_4}');
  "
}

# MySQL 命令参数配置化扩展
mysql_fun(){
  init_file="$(grep -Ev '^\s*$' "${1}")"
  [[ $(echo "${init_file}" | wc -l) != 5 ]] && {
    echo "$(date +'%F %T') 输入文件中的参数不正确！要输入5个属性值（mysql_host、mysql_port、mysql_user、mysql_pass、mysql_db）"
    exit 1
  }
  eval "${init_file}"
  # -N Don't write column names in results.
  # -s Be more silent. Print results with a tab as separator,each row on new line.
  echo "mysql -h${mysql_host} -P${mysql_port} -u${mysql_user} -p${mysql_pass} -D${mysql_db}"
}

# 抽取 Hive 数据到 本地文件
export_file_from_hive(){
  [[ -z $1 ]] && {
    echo "$(date +'%F %T') 请输入 参数一：hql"
    exit 1
  }

  echo "${s_date_export:=$(date +'%F %T')} 从 Hive 拉取数据 开始 ${export_file}"

  # $impala -q "refresh ${db_tb};"

  # echo \
  $impala -B --output_delimiter='\t' \
  -q "${1}" > "${export_file}"

  # $beeline \
  # --color=true \
  # --nullemptystring=true \
  # --showHeader=false \
  # --outputformat=tsv2 \
  # -e "${1}" > "${export_file}"

  echo "${e_date_export:=$(date +'%F %T')} 从 Hive 拉取数据 结束 ${export_file}  用时：$(during "$e_date_export" "$s_date_export")"
}

# 从本地文件拉取数据到 MySQL
import_file_to_mysql(){
  fields=$(
    $impala -B --print_header \
    --output_delimiter=',' \
    -q "select * from ${db_tb} where false;" 2> /dev/null

    # $beeline \
    # --color=true \
    # --hiveconf hive.resultset.use.unique.column.names=false \
    # --outputformat=csv2 \
    # -e "select * from ${db}.${tb} where false;" 2> /dev/null
  )

  printf '%-20s%-80s%s\n' "${s_date_import:=$(date +'%F %T')}" "库表为：${db_tb}" "字段为：$fields"

  tb=$(p_r_r ${db_tb})
  # echo \
  $mysql -e "
  truncate table ${tb};
  LOAD DATA LOCAL INFILE '${export_file}' INTO TABLE ${tb}
  FIELDS TERMINATED BY '\t'
  (${fields})
  ;"
  echo "${e_date_import:=$(date +'%F %T')} 数据上传到 MySQL 结束 ${export_file}  用时：$(during "$e_date_import" "$s_date_import")"
}

# 将以文本数据，转化为SQL
select_data(){ # 参数应为具体的数据（不适用于大量数据）
  tmp_data="$(echo "${1}" | sed '/\-\-\-/d; s/^|//g; s/|$//g; s/|/\t/g; ')"

  echo "$tmp_data" | awk -F '\t' '
  function trim(str){
    sub("^[ ]*", "", str);
    sub("[ ]*$", "", str);
    # sub("[t]*$", "", str);
    # sub("[\\\\]*$", "", str);
    return str
  }
  {
    line = "select "
    for(i = 1; i <= NF; i++){
      if(NR == 1) {
        col[i] = trim($i)
      } else {
        line = line "\""trim($i)"\" as "col[i]
        if(NF != i){
          line = line ","
        }
      }
    }
    if(NR != 1){
      if(NR != rows){
        line = line" union all"
      }
      print line > "tmp.file"
    }
  }' rows=$(echo "$tmp_data" | wc -l)
}
