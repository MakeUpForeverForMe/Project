/*
 Navicat Premium Data Transfer

 Source Server         : 测试大机器
 Source Server Type    : MySQL
 Source Server Version : 50718
 Source Host           : 10.83.16.43:3306
 Source Schema         : ws_adapter

 Target Server Type    : MySQL
 Target Server Version : 50718
 File Encoding         : 65001

 Date: 10/04/2020 15:34:48
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for t_transaction_blend_record
-- ----------------------------
DROP TABLE IF EXISTS `t_transaction_blend_record`;
CREATE TABLE `t_transaction_blend_record`  (
  `id`                    int(11) NOT NULL,
  `blend_serial_no`       varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL           COMMENT '流水勾兑编号',
  `record_type`           varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL            COMMENT '记录类型  D:借方 C:贷方 M:手工调整值 F:结果值',
  `loan_amt`              decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '放款金额',
  `cust_repay_amt`        decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '客户还款',
  `comp_bak_amt`          decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '代偿回款',
  `buy_bak_amt`           decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '回购回款',
  `deduct_sve_fee`        decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '划扣手续费',
  `invest_amt`            decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '投资金额',
  `invest_redeem_amt`     decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '投资赎回金额',
  `invest_earning`        decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '投资收益',
  `acct_int`              decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '账户利息',
  `acct_fee`              decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '账户费用',
  `tax_amt`               decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '税费支付',
  `invest_cash`           decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '投资兑付',
  `ci_fund`               decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '信保基金',
  `ci_redeem_amt`         decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '信保赎回',
  `ci_earning`            decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '信保收益',
  `other_amt`             decimal(18, 2) NULL DEFAULT NULL                                          COMMENT '其他金额',
  `trade_day_bal`         decimal(18, 2) NULL DEFAULT NULL                                          COMMENT 'T日余额',
  `trade_yesterday_bal`   decimal(18, 2) NULL DEFAULT NULL                                          COMMENT 'T-1日余额',
  `trade_day__bal_diff`   decimal(18, 2) NULL DEFAULT NULL                                          COMMENT 'T日余额差异',
  `remark`                varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '备注',
  `create_date`           datetime(0) NULL DEFAULT NULL                                             COMMENT '创建时间',
  `update_date`           datetime(0) NULL DEFAULT NULL                                             COMMENT '创建时间',
  `calc_date`             varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL  COMMENT '勾兑记录日期',
  `product_code`          varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL  COMMENT '信托产品编号',
  `product_name`          varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL  COMMENT '信托产品名称',
  `return_ticket_bak_amt` decimal(18,2) NULL DEFAULT NULL                                           COMMENT '退票回款',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '流水勾兑记录表表' ROW_FORMAT = Dynamic;

SET FOREIGN_KEY_CHECKS = 1;
