-- /**
-- * 标题头信息
-- */
drop table if exists `dm_eagle.eagle_title_info`;
create table if not exists `dm_eagle.eagle_title_info`(
  `capital_id`                                    string        COMMENT '资金方编号',
  `channel_id`                                    string        COMMENT '渠道方编号',
  `project_id`                                    string        COMMENT '项目编号',
  `product_id`                                    string        COMMENT '产品编号',
  `project_init_amount`                           decimal(15,4) COMMENT '初始放款规模（项目级金额）',
  `loan_month_map`                                string        COMMENT '放款月范围（含期数。结构：{"2020-07":[3,6,9],"2020-06":[9]}）',
  `biz_month_list`                                string        COMMENT '快照月范围（结构：["2020-01","2020-02"]）',
  `loan_terms_list`                               string        COMMENT '期数范围（结构：[3,6,9]）',
  `create_date`                                   string        COMMENT '插入日期'
) COMMENT '标题头信息'
stored as parquet;

-- /**
-- * 标题头信息
-- */
drop table if exists `dm_eagle_cps.eagle_title_info`;
create table if not exists `dm_eagle_cps.eagle_title_info`(
  `capital_id`                                    string        COMMENT '资金方编号',
  `channel_id`                                    string        COMMENT '渠道方编号',
  `project_id`                                    string        COMMENT '项目编号',
  `product_id`                                    string        COMMENT '产品编号',
  `project_init_amount`                           decimal(15,4) COMMENT '初始放款规模（项目级金额）',
  `loan_month_map`                                string        COMMENT '放款月范围（含期数。结构：{"2020-07":[3,6,9],"2020-06":[9]}）',
  `biz_month_list`                                string        COMMENT '快照月范围（结构：["2020-01","2020-02"]）',
  `loan_terms_list`                               string        COMMENT '期数范围（结构：[3,6,9]）',
  `create_date`                                   string        COMMENT '插入日期'
) COMMENT '标题头信息'
stored as parquet;
