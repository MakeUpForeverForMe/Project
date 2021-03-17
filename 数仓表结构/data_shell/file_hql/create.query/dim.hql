-- 业务配置表 生产： ueagle 库 测试： ueagle-dev 库
-- 数据库主键 product_id
-- 业务主键 product_id
-- DROP TABLE IF EXISTS `t_biz_conf`;
CREATE TABLE IF NOT EXISTS `t_biz_conf`(
  `biz_name`                      varchar(255)   COMMENT '业务名称（中文）',
  `biz_name_en`                   varchar(255)   COMMENT '业务名称（英文）',
  `capital_id`                    varchar(255)   COMMENT '资金方编号',
  `capital_name`                  varchar(255)   COMMENT '资金方名称（中文）',
  `capital_name_en`               varchar(255)   COMMENT '资金方名称（英文）',
  `channel_id`                    varchar(255)   COMMENT '渠道方编号',
  `channel_name`                  varchar(255)   COMMENT '渠道方名称（中文）',
  `channel_name_en`               varchar(255)   COMMENT '渠道方名称（英文）',
  `trust_id`                      varchar(255)   COMMENT '信托计划编号',
  `trust_name`                    varchar(255)   COMMENT '信托计划名称（中文）',
  `trust_name_en`                 varchar(255)   COMMENT '信托计划名称（英文）',
  `project_id`                    varchar(255)   COMMENT '项目编号',
  `project_name`                  varchar(255)   COMMENT '项目名称（中文）',
  `project_name_en`               varchar(255)   COMMENT '项目名称（英文）',
  `project_amount`                decimal(15,4)  COMMENT '项目初始金额',
  `product_id`                    varchar(255)   COMMENT '产品编号',
  `product_name`                  varchar(255)   COMMENT '产品名称（中文）',
  `product_name_en`               varchar(255)   COMMENT '产品名称（英文）',
  `product_id_vt`                 varchar(255)   COMMENT '产品编号（虚拟）',
  `product_name_vt`               varchar(255)   COMMENT '产品名称（中文、虚拟）',
  `product_name_en_vt`            varchar(255)   COMMENT '产品名称（英文、虚拟）'
) COMMENT '业务配置表'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;


-- 投资人信息表
-- DROP TABLE IF EXISTS `t_investor_info`;
CREATE TABLE IF NOT EXISTS `t_investor_info`(
  `capital_id`                    varchar(255)   NOT NULL COMMENT '资金方编号',
  `capital_name`                  varchar(255)   NOT NULL COMMENT '资金方名称',
  `project_id`                    varchar(255)   NOT NULL COMMENT '项目编号',
  `project_name`                  varchar(255)   NOT NULL COMMENT '项目名称',
  `investor_id`                   varchar(255)   NOT NULL COMMENT '投资人编号',
  `investor_name`                 varchar(255)   NOT NULL COMMENT '投资人名称',
  `investment_funds`              decimal(20,5)  NOT NULL COMMENT '投资资金（元）',
  `investment_start_date`         varchar(255)   NOT NULL COMMENT '投资起始日期',
  `investment_maturity_date`      varchar(255)   NOT NULL COMMENT '投资到期日期',
  `annualized_days`               varchar(255)   NOT NULL COMMENT '年化天数（天）',
  `coupon_rate`                   decimal(25,10) NOT NULL COMMENT '收益率（%）',
  `Share_type`                    varchar(255)   NOT NULL COMMENT '份额类型',
  `Share_proportion`              decimal(25,10) NOT NULL COMMENT '份额占比（%）',
  `coupon_formula`                decimal(3,0)   NOT NULL COMMENT '收益公式',
  `coupon_formula_effective_date` varchar(255)   NOT NULL COMMENT '收益公式生效日期',
  `coupon_formula_expire_date`    varchar(255)   NOT NULL COMMENT '收益公式失效日期',
  `int_calc_rules`                varchar(255)   NOT NULL COMMENT '计息规则',
  `CREATE_time`                   timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`                   timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) COMMENT '投资人信息表'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

use flink_config;
-- DROP TABLE IF EXISTS `task_info`;
CREATE TABLE IF NOT EXISTS `task_info`(
  `id`                            int(11)      NOT NULL AUTO_INCREMENT            COMMENT '自增id',
  `batch_date`                    DATE         NOT NULL                           COMMENT '跑批日期',
  `job_id`                        VARCHAR(100) NOT NULL                           COMMENT '任务编号',
  `job_name`                      VARCHAR(100) DEFAULT '未定义'                   COMMENT '任务名称',
  `run_status`                    VARCHAR(100) NOT NULL                           COMMENT '任务状态',
  `start_time`                    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '开始时间',
  `end_time`                      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '结束时间',
  PRIMARY KEY (`id`)
) COMMENT '任务信息'
ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;







-- DROP DATABASE IF EXISTS dim;
CREATE DATABASE IF NOT EXISTS dim COMMENT '维度数据层' location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db';




-- 业务配置表
-- 数据库主键 product_id
-- 业务主键 product_id
-- DROP TABLE IF EXISTS `dim.biz_conf`;
CREATE EXTERNAL TABLE IF NOT EXISTS `dim.biz_conf`(
  `biz_name`                      string         COMMENT '业务名称（中文）',
  `biz_name_en`                   string         COMMENT '业务名称（英文）',
  `capital_id`                    string         COMMENT '资金方编号',
  `capital_name`                  string         COMMENT '资金方名称（中文）',
  `capital_name_en`               string         COMMENT '资金方名称（英文）',
  `channel_id`                    string         COMMENT '渠道方编号',
  `channel_name`                  string         COMMENT '渠道方名称（中文）',
  `channel_name_en`               string         COMMENT '渠道方名称（英文）',
  `trust_id`                      string         COMMENT '信托计划编号',
  `trust_name`                    string         COMMENT '信托计划名称（中文）',
  `trust_name_en`                 string         COMMENT '信托计划名称（英文）',
  `abs_project_id`                string         COMMENT 'ABS项目编号',
  `abs_project_name`              string         COMMENT 'ABS项目名称（中文）',
  `project_id`                    string         COMMENT '项目编号',
  `project_name`                  string         COMMENT '项目名称（中文）',
  `project_name_en`               string         COMMENT '项目名称（英文）',
  `project_amount`                decimal(15,4)  COMMENT '项目初始金额',
  `product_id`                    string         COMMENT '产品编号',
  `product_name`                  string         COMMENT '产品名称（中文）',
  `product_name_en`               string         COMMENT '产品名称（英文）',
  `product_id_vt`                 string         COMMENT '产品编号（虚拟）',
  `product_name_vt`               string         COMMENT '产品名称（中文、虚拟）',
  `product_name_en_vt`            string         COMMENT '产品名称（英文、虚拟）'
) COMMENT '业务配置表'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
-- field.delim是表的两个列字段之间的文件中的字段分隔符.
-- 其中serialization.format是文件序列化时表中两个列字段之间的文件中的字段分隔符.
WITH SERDEPROPERTIES ('field.delim' = '\t','serialization.format' = '\t','serialization.null.format' = '')
location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db/biz_conf'
STORED AS TEXTFILE;


-- 配置信息
-- DROP TABLE IF EXISTS `dim.data_conf`;
CREATE TABLE IF NOT EXISTS `dim.data_conf`(
  `col_type`                      string         COMMENT '数据类型（ac：astrology_conf、星象配置表，ai：astrology_investor、星象投资人信息表，pp：abs_project_partition_id、星云项目与数据分区的映射，ap：abs_project_info、星云项目表，ab：abs_bag_info、星云包信息表）',
  `col_id`                        string         COMMENT '关联编号',
  `col_name`                      string         COMMENT '字段名称',
  `col_val`                       string         COMMENT '字段内容',
  `col_COMMENT`                   string         COMMENT '字段注释',
  `CREATE_user`                   string         COMMENT '创建用户',
  `CREATE_time`                   timestamp      COMMENT '创建时间',
  `update_user`                   string         COMMENT '更新用户',
  `update_time`                   timestamp      COMMENT '更新时间'
) COMMENT '配置信息'
STORED AS PARQUET;



-- 身份证地区
-- 数据库主键 idno_addr
-- 业务主键 idno_addr
-- DROP TABLE IF EXISTS `dim.dim_idno`;
CREATE EXTERNAL TABLE IF NOT EXISTS `dim.dim_idno`(
  `idno_addr`                     string         COMMENT '身份证前6位编码',
  `idno_area`                     string         COMMENT '身份证大区（编码）',
  `idno_area_cn`                  string         COMMENT '身份证大区（解释）（华北地区、东北地区、华东地区、中南地区、西南地区、西北地区、港澳台地区）',
  `idno_province`                 string         COMMENT '身份证省级（编码）',
  `idno_province_cn`              string         COMMENT '身份证省级（解释）（省、直辖市、特别行政区）',
  `idno_city`                     string         COMMENT '身份证地级（编码）',
  `idno_city_cn`                  string         COMMENT '身份证地级（解释）（地级市、自治州）',
  `idno_county`                   string         COMMENT '身份证县级（编码）',
  `idno_county_cn`                string         COMMENT '身份证县级（解释）（区县）'
) COMMENT '身份证地区'
STORED AS PARQUET
location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db/dim_idno'
;


-- 加密信息表
-- DROP TABLE IF EXISTS `dim.dim_encrypt_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `dim.dim_encrypt_info`(
  `dim_type`                      string         COMMENT '数据类型',
  `dim_encrypt`                   string         COMMENT '加密字段',
  `dim_decrypt`                   string         COMMENT '明文字段'
) COMMENT '加密信息表'
STORED AS PARQUET
location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db/dim_encrypt_info'
;


-- 日期维度表
-- DROP TABLE IF EXISTS `dim.dim_date`;
CREATE TABLE IF NOT EXISTS `dim.dim_date`(
  `biz_date`                      string         COMMENT '日期'
) COMMENT '日期维度表'
STORED AS TEXTFILE;


-- 投资人信息表
-- DROP TABLE IF EXISTS `dim.dim_investor_info`;
CREATE EXTERNAL TABLE IF NOT EXISTS `dim.dim_investor_info`(
  `capital_id`                    string         COMMENT '资金方编号',
  `capital_name`                  string         COMMENT '资金方名称',
  `project_id`                    string         COMMENT '项目编号',
  `project_name`                  string         COMMENT '项目名称',
  `investor_id`                   string         COMMENT '投资人编号',
  `investor_name`                 string         COMMENT '投资人名称',
  `investment_funds`              decimal(20,5)  COMMENT '投资资金（元）',
  `investment_start_date`         string         COMMENT '投资起始日期',
  `investment_maturity_date`      string         COMMENT '投资到期日期',
  `annualized_days`               string         COMMENT '年化天数（天）',
  `coupon_rate`                   decimal(25,10) COMMENT '收益率（%）',
  `Share_type`                    string         COMMENT '份额类型',
  `Share_proportion`              decimal(25,10) COMMENT '份额占比（%）',
  `coupon_formula`                decimal(3,0)   COMMENT '收益公式',
  `coupon_formula_effective_date` string         COMMENT '收益公式生效日期',
  `coupon_formula_expire_date`    string         COMMENT '收益公式失效日期',
  `int_calc_rules`                string         COMMENT '计息规则'
) COMMENT '投资人信息表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db/dim_investor_info'
;


-- 车辆品牌
-- DROP TABLE IF EXISTS `dim.dim_car_brand`;
CREATE EXTERNAL TABLE IF NOT EXISTS `dim.dim_car_brand`(
  `car_brand`                     string         COMMENT '车辆品牌（英语）',
  `car_brand_cn`                  string         COMMENT '车辆品牌（汉语）'
) COMMENT '车辆品牌'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db/dim_car_brand'
;










-- 静态逾期表
-- DROP TABLE IF EXISTS `dim.dim_static_overdue_bill`;
CREATE EXTERNAL TABLE IF NOT EXISTS `dim.dim_static_overdue_bill`(
  `product_id`                    string         COMMENT '产品编号',
  `due_bill_no`                   string         COMMENT '借据编号',
  `loan_term`                     decimal(3,0)   COMMENT '当前期数',
  `loan_active_date`              string         COMMENT '放款日期',
  `loan_init_principal`           decimal(15,4)  COMMENT '贷款本金',
  `loan_init_term`                decimal(3,0)   COMMENT '贷款期数（3、6、9等）',
  `remain_term`                   decimal(3,0)   COMMENT '截止当前剩余未还期数，不做更新',
  `remain_principal`              decimal(15,4)  COMMENT '截止当前本金余额，不做更新',
  `remain_interest`               decimal(15,4)  COMMENT '截止当前剩余应还利息，不做更新',
  `paid_term`                     decimal(3,0)   COMMENT '截止当前已还期数不做更新',
  `paid_principal`                decimal(15,4)  COMMENT '截止当前已还本金，不做更新',
  `paid_interest`                 decimal(15,4)  COMMENT '截止当前已还利息，不做更新',
  `overdue_date_start`            string         COMMENT '逾期起始日期，不更新',
  `overdue_days`                  decimal(5,0)   COMMENT '逾期天数，不更新，只取：31、61、91',
  `loan_status`                   string         COMMENT '借据状态，不更新（N：正常，O：逾期，F：已还清）'
) COMMENT '借据静态逾期表，只记录第一次逾期31天、61天、91天的数据，只统计代偿前'
PARTITIONED BY (`biz_date` string COMMENT '观察日期',`prj_type` string COMMENT '项目类别：DD、HT、LXGM、LXZT')
STORED AS PARQUET
location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db/dim_static_overdue_bill'
;


---汇通服务报告资产包
-- DROP TABLE IF EXISTS `dim.dim_ht_bag_asset`;
CREATE EXTERNAL TABLE IF NOT EXISTS `dim.dim_ht_bag_asset`(
  `project_id`                    string         COMMENT '项目ID',
  `due_bill_no`                   string         COMMENT '借据号',
  `asset_date`                    string         COMMENT '借据入包日期',
  `name`                          string         COMMENT '客户姓名',
  `total_bag_remain_principal`    decimal(15,4)  COMMENT '借据入包时剩余本金',
  `total_bag_remain_interest`     decimal(15,4)  COMMENT '借据入包时剩余利息'
) COMMENT 'dim 汇通资产包信息'
PARTITIONED BY (`biz_date` string COMMENT '封包日期')
STORED AS PARQUET
location 'cosn://bigdata-center-prod-1253824322/user/hadoop/warehouse/dim.db/dim_ht_bag_asset'
;










-- 项目属性
-- DROP TABLE IF EXISTS `dim.project_info`;
CREATE TABLE IF NOT EXISTS `dim.project_info`(
  `project_name`                  string         COMMENT '项目名称',
  `project_stage`                 string         COMMENT '项目阶段',
  `asset_side`                    string         COMMENT '资产方',
  `fund_side`                     string         COMMENT '资金方',
  `year`                          string         COMMENT '年份',
  `term`                          string         COMMENT '期数',
  `remarks`                       string         COMMENT '备注',
  `project_full_name`             string         COMMENT '项目全名称',
  `asset_type`                    string         COMMENT '资产类别（1：汽车贷，2：房贷，3：消费贷）',
  `project_type`                  string         COMMENT '业务模式（1：存量，2：增量）',
  `mode`                          string         COMMENT '模型归属',
  `project_time`                  string         COMMENT '立项时间',
  `project_begin_date`            string         COMMENT '项目开始时间',
  `project_end_date`              string         COMMENT '项目结束时间',
  `asset_pool_type`               string         COMMENT '资产池类型',
  `public_offer`                  string         COMMENT '公募名称',
  `data_source`                   string         COMMENT '数据来源',
  `CREATE_user`                   string         COMMENT '创建人',
  `CREATE_time`                   string         COMMENT '创建时间',
  `update_time`                   string         COMMENT '更新时间'
) COMMENT '项目属性'
PARTITIONED BY (`project_id` string COMMENT '项目编号')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


-- 项目借据映射
-- DROP TABLE IF EXISTS `dim.project_due_bill_no`;
CREATE TABLE IF NOT EXISTS `dim.project_due_bill_no`(
  `due_bill_no`                   string         COMMENT '借据编号',
  `related_project_id`            string         COMMENT '债转前项目编号',
  `related_date`                  string         COMMENT '债转发生日期',
  `partition_id`                  string         COMMENT '数据分区编号'
) COMMENT '项目借据映射'
PARTITIONED BY (`project_id` string COMMENT '项目编号')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


-- 包属性
-- DROP TABLE IF EXISTS `dim.bag_info`;
CREATE TABLE IF NOT EXISTS `dim.bag_info`(
  `project_id`                    string         COMMENT '项目编号',
  `bag_name`                      string         COMMENT '包名称',
  `bag_status`                    string         COMMENT '包状态',
  `bag_remain_principal`          string         COMMENT '封包总本金余额',
  `bag_date`                      string         COMMENT '封包日期',
  `insert_date`                   string         COMMENT '封包操作日期'
) COMMENT '包属性'
PARTITIONED BY (`bag_id` string COMMENT '包编号')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
;


-- 包借据映射
-- DROP TABLE IF EXISTS `dim.bag_due_bill_no`;
CREATE TABLE IF NOT EXISTS `dim.bag_due_bill_no`(
  `due_bill_no`                   string         COMMENT '借据编号',
  `package_remain_principal`      decimal(15,4)  COMMENT '封包时本金余额',
  `package_remain_periods`        int            COMMENT '封包时剩余期数'
) COMMENT '包借据映射'
PARTITIONED BY (`bag_id` string COMMENT '包编号')
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
;











-- DROP DATABASE IF EXISTS `hivemetastore`;
CREATE DATABASE IF NOT EXISTS `hivemetastore` COMMENT 'Hive 元数据库';


-- DROP TABLE IF EXISTS `hivemetastore.dbs`;
CREATE EXTERNAL TABLE IF NOT EXISTS `hivemetastore.dbs`(
  `db_id`                     bigint COMMENT '数据库ID',
  `desc`                      string COMMENT '数据库注释',
  `db_location_uri`           string COMMENT '数据库地址',
  `name`                      string COMMENT '数据库名称',
  `owner_name`                string COMMENT '创建者',
  `owner_type`                string COMMENT '创建者类型',
  `ctlg_name`                 string COMMENT ''
) COMMENT 'Hive库的元数据'
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://10.80.1.104/hivemetastore",
  "hive.sql.dbcp.username"  = "root",
  "hive.sql.dbcp.password"  = "Ws@ProEmr1QSC@",
  "hive.sql.table"          = "DBS",
  "hive.sql.dbcp.maxActive" = "1"
);


-- DROP TABLE IF EXISTS `hivemetastore.tbls`;
CREATE EXTERNAL TABLE IF NOT EXISTS `hivemetastore.tbls`(
  `tbl_id`                    bigint COMMENT '表ID',
  `create_time`               int    COMMENT '创建时间',
  `db_id`                     bigint COMMENT '数据库ID',
  `last_access_time`          int    COMMENT '最后修改时间',
  `owner`                     string COMMENT '创建者',
  `owner_type`                string COMMENT '创建者类型',
  `retention`                 int    COMMENT '保留字段',
  `sd_id`                     bigint COMMENT '字段关联ID',
  `tbl_name`                  string COMMENT '表名',
  `tbl_type`                  string COMMENT '表类型',
  `view_expanded_text`        string COMMENT '',
  `view_original_text`        string COMMENT '',
  `is_rewrite_enabled`        int    COMMENT ''
) COMMENT 'Hive表名称的元数据'
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://10.80.1.104/hivemetastore",
  "hive.sql.dbcp.username"  = "root",
  "hive.sql.dbcp.password"  = "Ws@ProEmr1QSC@",
  "hive.sql.table"          = "TBLS",
  "hive.sql.dbcp.maxActive" = "1"
);


-- DROP TABLE IF EXISTS `hivemetastore.table_params`;
CREATE EXTERNAL TABLE IF NOT EXISTS `hivemetastore.table_params`(
  `tbl_id`                    bigint COMMENT '表ID',
  `param_key`                 string COMMENT '表属性名',
  `param_value`               string COMMENT '表属性值'
) COMMENT 'Hive表属性的元数据'
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://10.80.1.104/hivemetastore",
  "hive.sql.dbcp.username"  = "root",
  "hive.sql.dbcp.password"  = "Ws@ProEmr1QSC@",
  "hive.sql.table"          = "TABLE_PARAMS",
  "hive.sql.dbcp.maxActive" = "1"
);


-- DROP TABLE IF EXISTS `hivemetastore.sds`;
CREATE EXTERNAL TABLE IF NOT EXISTS `hivemetastore.sds`(
  `sd_id`                     bigint COMMENT '字段关联ID',
  `cd_id`                     bigint COMMENT '字段ID',
  `input_format`              string COMMENT '输入类型',
  `is_compressed`             int    COMMENT '',
  `is_storedassubdirectories` int    COMMENT '',
  `location`                  string COMMENT '表地址',
  `num_buckets`               int    COMMENT '分桶',
  `output_format`             string COMMENT '输出类型',
  `serde_id`                  bigint COMMENT '排序'
) COMMENT 'Hive字段的元数据'
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://10.80.1.104/hivemetastore",
  "hive.sql.dbcp.username"  = "root",
  "hive.sql.dbcp.password"  = "Ws@ProEmr1QSC@",
  "hive.sql.table"          = "SDS",
  "hive.sql.dbcp.maxActive" = "1"
);


-- DROP TABLE IF EXISTS `hivemetastore.columns_v2`;
CREATE EXTERNAL TABLE IF NOT EXISTS `hivemetastore.columns_v2`(
  `cd_id`                     bigint COMMENT '字段ID',
  `comment`                   string COMMENT '字段注释',
  `column_name`               string COMMENT '字段名称',
  `type_name`                 string COMMENT '字段类型',
  `integer_idx`               int    COMMENT '字段排序'
) COMMENT 'Hive字段的元数据'
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://10.80.1.104/hivemetastore",
  "hive.sql.dbcp.username"  = "root",
  "hive.sql.dbcp.password"  = "Ws@ProEmr1QSC@",
  "hive.sql.table"          = "COLUMNS_V2",
  "hive.sql.dbcp.maxActive" = "1"
);


-- DROP TABLE IF EXISTS `hivemetastore.partition_keys`;
CREATE EXTERNAL TABLE IF NOT EXISTS `hivemetastore.partition_keys`(
  `tbl_id`                    bigint COMMENT '表ID',
  `pkey_comment`              string COMMENT '分区字段注释',
  `pkey_name`                 string COMMENT '分区字段名称',
  `pkey_type`                 string COMMENT '分区字段类型',
  `integer_idx`               int    COMMENT '分区字段排序'
) COMMENT 'Hive分区的元数据'
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
  "hive.sql.database.type"  = "MYSQL",
  "hive.sql.jdbc.driver"    = "com.mysql.jdbc.Driver",
  "hive.sql.jdbc.url"       = "jdbc:mysql://10.80.1.104/hivemetastore",
  "hive.sql.dbcp.username"  = "root",
  "hive.sql.dbcp.password"  = "Ws@ProEmr1QSC@",
  "hive.sql.table"          = "PARTITION_KEYS",
  "hive.sql.dbcp.maxActive" = "1"
);
