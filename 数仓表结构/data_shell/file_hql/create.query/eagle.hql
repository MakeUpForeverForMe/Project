create table eagle.wind_data_export_table(
product_id                          string comment '产品号',
due_bill_no                         string comment '借据号',
name                                string comment 'aes加密后的姓名',
idcard_no                           string comment 'aes 加密后的身份证号码',
mobie                               string comment 'aes加密后的手机号码',
loan_status                         int comment '借据状态结清1 未结清0'
)partitioned by (biz_date string comment '数据提取日',project_id string comment  '项目id')
STORED AS PARQUET;


create table eagle.wind_data_export_table(
product_id                          string comment '产品号',
due_bill_no                         string comment '借据号',
name                                string comment 'aes加密后的姓名',
idcard_no                           string comment 'aes 加密后的身份证号码',
mobie                               string comment 'aes加密后的手机号码',
loan_status                         int comment '借据状态结清1 未结清0',
biz_date                            string comment '数据提取日',
project_id                          string comment  '项目id'
)
row format delimited fields terminated by '\t'
STORED AS textfile ;


load data local  inpath "/root/wind_file.tsv" overwrite into table eagle.wind_data_export_table  ;