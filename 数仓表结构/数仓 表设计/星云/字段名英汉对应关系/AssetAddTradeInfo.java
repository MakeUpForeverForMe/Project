package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.math.BigDecimal;
import java.util.Date;

/**
 *  资产补充交易信息 File09
 **/
public class AssetAddTradeInfo extends BaseEntity {

    /**
     * 项目编号
     * 表字段 : t_asset_add_trade_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_asset_add_trade_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_asset_add_trade_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 交易类型
     * 表字段 : t_asset_add_trade_info.TRADE_TYPE
     */
    @SerializedName("交易类型")
    private String tradeType;

    /**
     * 交易原因
     * 表字段 : t_asset_add_trade_info.TRADE_REASON
     */
    @SerializedName("交易原因")
    private String tradeReason;

    /**
     * 交易日期
     * 表字段 : t_asset_add_trade_info.TRADE_TIME
     */
    @SerializedName("交易日期")
    private Date tradeTime;

    /**
     * 交易总金额
     * 表字段 : t_asset_add_trade_info.TRADE_TOTAL_AMOUNT
     */
    @SerializedName("交易总金额")
    private BigDecimal tradeTotalAmount;

    /**
     * 本金
     * 表字段 : t_asset_add_trade_info.PRINCIPAL
     */
    @SerializedName("本金")
    private BigDecimal principal;

    /**
     * 利息
     * 表字段 : t_asset_add_trade_info.INTEREST
     */
    @SerializedName("利息")
    private BigDecimal interest;

    /**
     * 罚息
     * 表字段 : t_asset_add_trade_info.PENALTY_INTEREST
     */
    @SerializedName("罚息")
    private BigDecimal penaltyInterest;

    /**
     * 其他费用
     * 表字段 : t_asset_add_trade_info.OTHER_FEE
     */
    @SerializedName("其他费用")
    private BigDecimal otherFee;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_asset_add_trade_info.PROJECT_ID
     *
     * @return t_asset_add_trade_info.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_asset_add_trade_info.PROJECT_ID
     *
     * @param projectId t_asset_add_trade_info.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_asset_add_trade_info.AGENCY_ID
     *
     * @return t_asset_add_trade_info.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_asset_add_trade_info.AGENCY_ID
     *
     * @param agencyId t_asset_add_trade_info.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_asset_add_trade_info.SERIAL_NUMBER
     *
     * @return t_asset_add_trade_info.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_asset_add_trade_info.SERIAL_NUMBER
     *
     * @param serialNumber t_asset_add_trade_info.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 交易类型 字段:t_asset_add_trade_info.TRADE_TYPE
     *
     * @return t_asset_add_trade_info.TRADE_TYPE, 交易类型
     */
    public String getTradeType() {
        return tradeType;
    }

    /**
     * 设置 交易类型 字段:t_asset_add_trade_info.TRADE_TYPE
     *
     * @param tradeType t_asset_add_trade_info.TRADE_TYPE, 交易类型
     */
    public void setTradeType(String tradeType) {
        this.tradeType = tradeType == null ? null : tradeType.trim();
    }

    /**
     * 获取 交易原因 字段:t_asset_add_trade_info.TRADE_REASON
     *
     * @return t_asset_add_trade_info.TRADE_REASON, 交易原因
     */
    public String getTradeReason() {
        return tradeReason;
    }

    /**
     * 设置 交易原因 字段:t_asset_add_trade_info.TRADE_REASON
     *
     * @param tradeReason t_asset_add_trade_info.TRADE_REASON, 交易原因
     */
    public void setTradeReason(String tradeReason) {
        this.tradeReason = tradeReason == null ? null : tradeReason.trim();
    }

    /**
     * 获取 交易日期 字段:t_asset_add_trade_info.TRADE_TIME
     *
     * @return t_asset_add_trade_info.TRADE_TIME, 交易日期
     */
    public Date getTradeTime() {
        return tradeTime;
    }

    /**
     * 设置 交易日期 字段:t_asset_add_trade_info.TRADE_TIME
     *
     * @param tradeTime t_asset_add_trade_info.TRADE_TIME, 交易日期
     */
    public void setTradeTime(Date tradeTime) {
        this.tradeTime = tradeTime == null ? null : tradeTime;
    }

    /**
     * 获取 交易总金额 字段:t_asset_add_trade_info.TRADE_TOTAL_AMOUNT
     *
     * @return t_asset_add_trade_info.TRADE_TOTAL_AMOUNT, 交易总金额
     */
    public BigDecimal getTradeTotalAmount() {
        return tradeTotalAmount;
    }

    /**
     * 设置 交易总金额 字段:t_asset_add_trade_info.TRADE_TOTAL_AMOUNT
     *
     * @param tradeTotalAmount t_asset_add_trade_info.TRADE_TOTAL_AMOUNT, 交易总金额
     */
    public void setTradeTotalAmount(BigDecimal tradeTotalAmount) {
        this.tradeTotalAmount = tradeTotalAmount;
    }

    /**
     * 获取 本金 字段:t_asset_add_trade_info.PRINCIPAL
     *
     * @return t_asset_add_trade_info.PRINCIPAL, 本金
     */
    public BigDecimal getPrincipal() {
        return principal;
    }

    /**
     * 设置 本金 字段:t_asset_add_trade_info.PRINCIPAL
     *
     * @param principal t_asset_add_trade_info.PRINCIPAL, 本金
     */
    public void setPrincipal(BigDecimal principal) {
        this.principal = principal;
    }

    /**
     * 获取 利息 字段:t_asset_add_trade_info.INTEREST
     *
     * @return t_asset_add_trade_info.INTEREST, 利息
     */
    public BigDecimal getInterest() {
        return interest;
    }

    /**
     * 设置 利息 字段:t_asset_add_trade_info.INTEREST
     *
     * @param interest t_asset_add_trade_info.INTEREST, 利息
     */
    public void setInterest(BigDecimal interest) {
        this.interest = interest;
    }

    /**
     * 获取 罚息 字段:t_asset_add_trade_info.PENALTY_INTEREST
     *
     * @return t_asset_add_trade_info.PENALTY_INTEREST, 罚息
     */
    public BigDecimal getPenaltyInterest() {
        return penaltyInterest;
    }

    /**
     * 设置 罚息 字段:t_asset_add_trade_info.PENALTY_INTEREST
     *
     * @param penaltyInterest t_asset_add_trade_info.PENALTY_INTEREST, 罚息
     */
    public void setPenaltyInterest(BigDecimal penaltyInterest) {
        this.penaltyInterest = penaltyInterest;
    }

    /**
     * 获取 其他费用 字段:t_asset_add_trade_info.OTHER_FEE
     *
     * @return t_asset_add_trade_info.OTHER_FEE, 其他费用
     */
    public BigDecimal getOtherFee() {
        return otherFee;
    }

    /**
     * 设置 其他费用 字段:t_asset_add_trade_info.OTHER_FEE
     *
     * @param otherFee t_asset_add_trade_info.OTHER_FEE, 其他费用
     */
    public void setOtherFee(BigDecimal otherFee) {
        this.otherFee = otherFee;
    }

    public Integer getImportId() {
        return importId;
    }

    public void setImportId(Integer importId) {
        this.importId = importId;
    }

    public Integer getDataSource() {
        return dataSource;
    }

    public void setDataSource(Integer dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public String toString() {
        return "AssetAddTradeInfo{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", tradeType='" + tradeType + '\'' +
                ", tradeReason='" + tradeReason + '\'' +
                ", tradeTime=" + tradeTime +
                ", tradeTotalAmount=" + tradeTotalAmount +
                ", principal=" + principal +
                ", interest=" + interest +
                ", penaltyInterest=" + penaltyInterest +
                ", otherFee=" + otherFee +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}