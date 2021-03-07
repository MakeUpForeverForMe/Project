-- /**
--  * 加密信息表
--  */
-- DROP TABLE IF EXISTS `dim_new.dim_encrypt_info`;
CREATE TABLE IF NOT EXISTS dim_new.dim_encrypt_info(
  `dim_type`                      string        COMMENT '数据类型',
  `dim_encrypt`                   string        COMMENT '加密字段',
  `dim_decrypt`                   string        COMMENT '明文字段',
  `create_time`                   timestamp     COMMENT '创建时间（yyyy—MM—dd HH:mm:ss）',
  `update_time`                   timestamp     COMMENT '更新时间（yyyy—MM—dd HH:mm:ss）'
) COMMENT '加密信息表'
PARTITIONED BY (`product_id` string COMMENT ')产 品,编号', aa int)
STORED AS PARQUET;

-- CREATE TABLE `dim_new.dim_encrypt_info`(
--   `dim_type`    string    COMMENT '数据类型',
--   `dim_encrypt` string    COMMENT '加密字段',
--   `dim_decrypt` string    COMMENT '明文字段',
--   `create_time` timestamp COMMENT '创建时间（yyyy—MM—dd HH:mm:ss）',
--   `update_time` timestamp COMMENT '更新时间（yyyy—MM—dd HH:mm:ss）'
-- ) COMMENT '加密信息表'
-- PARTITIONED BY (`product_id` string COMMENT '产品编号')
-- STORED AS PARQUET
-- ;

CREATE TABLE `dim_new.dim_encrypt_info` like dim_new.dim_encrypt_info;

CREATE TABLE `dim_new.dim_encrypt_info` as select aa from (
    select * from dim_new.dim_encrypt_info) as tmp;

set hivevar:biz_date=2020-07-14;
