package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.math.BigDecimal;
import java.util.Date;

/**
 *  资产交易支付流水信息 File06
 **/
public class AssetTradeFlow extends BaseEntity {
    /**
     * 项目编号
     * 表字段 : t_asset_trade_flow.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_asset_trade_flow.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_asset_trade_flow.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 交易渠道
     * 表字段 : t_asset_trade_flow.TRADE_CHANNEL
     */
    @SerializedName("交易渠道")
    private String tradeChannel;

    /**
     * 交易类型
     * 表字段 : t_asset_trade_flow.TRADE_TYPE
     */
    @SerializedName("交易类型")
    private String tradeType;

    /**
     * 订单编号
     * 表字段 : t_asset_trade_flow.ORDER_NUMBER
     */
    @SerializedName("订单号")
    private String orderNumber;

    /**
     * 订单金额
     * 表字段 : t_asset_trade_flow.ORDER_AMOUNT
     */
    @SerializedName("订单金额(元)")
    private BigDecimal orderAmount;

    /**
     * 币种
     * 表字段 : t_asset_trade_flow.TRADE_CURRENCY
     */
    @SerializedName("交易币种")
    private String tradeCurrency;

    /**
     * 姓名
     * 表字段 : t_asset_trade_flow.NAME
     */
    @SerializedName("姓名")
    private String name;

    /**
     * 银行账号
     * 表字段 : t_asset_trade_flow.BANK_ACCOUNT
     */
    @SerializedName("银行帐号")
    private String bankAccount;

    /**
     * 交易日期
     * 表字段 : t_asset_trade_flow.TRADE_TIME
     */
    @SerializedName("交易时间")
    private Date tradeTime;

    /**
     * 交易状态
     * 表字段 : t_asset_trade_flow.TRADE_STATUS
     */
    @SerializedName("交易状态")
    private String tradeStatus;

    /**
     * 交易摘要
     * 表字段 : t_asset_trade_flow.TRADE_DIGEST
     */
    @SerializedName("交易摘要")
    private String tradeDigest;

    /**
     * 确认还款日期
     * 表字段 : t_asset_trade_flow.CONFIRM_REPAY_TIME
     */
    @SerializedName("确认还款日期")
    private Date confirmRepayTime;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_asset_trade_flow.PROJECT_ID
     *
     * @return t_asset_trade_flow.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_asset_trade_flow.PROJECT_ID
     *
     * @param projectId t_asset_trade_flow.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_asset_trade_flow.AGENCY_ID
     *
     * @return t_asset_trade_flow.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_asset_trade_flow.AGENCY_ID
     *
     * @param agencyId t_asset_trade_flow.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_asset_trade_flow.SERIAL_NUMBER
     *
     * @return t_asset_trade_flow.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_asset_trade_flow.SERIAL_NUMBER
     *
     * @param serialNumber t_asset_trade_flow.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 交易渠道 字段:t_asset_trade_flow.TRADE_CHANNEL
     *
     * @return t_asset_trade_flow.TRADE_CHANNEL, 交易渠道
     */
    public String getTradeChannel() {
        return tradeChannel;
    }

    /**
     * 设置 交易渠道 字段:t_asset_trade_flow.TRADE_CHANNEL
     *
     * @param tradeChannel t_asset_trade_flow.TRADE_CHANNEL, 交易渠道
     */
    public void setTradeChannel(String tradeChannel) {
        this.tradeChannel = tradeChannel == null ? null : tradeChannel.trim();
    }

    /**
     * 获取 交易类型 字段:t_asset_trade_flow.TRADE_TYPE
     *
     * @return t_asset_trade_flow.TRADE_TYPE, 交易类型
     */
    public String getTradeType() {
        return tradeType;
    }

    /**
     * 设置 交易类型 字段:t_asset_trade_flow.TRADE_TYPE
     *
     * @param tradeType t_asset_trade_flow.TRADE_TYPE, 交易类型
     */
    public void setTradeType(String tradeType) {
        this.tradeType = tradeType == null ? null : tradeType.trim();
    }

    /**
     * 获取 订单编号 字段:t_asset_trade_flow.ORDER_NUMBER
     *
     * @return t_asset_trade_flow.ORDER_NUMBER, 订单编号
     */
    public String getOrderNumber() {
        return orderNumber;
    }

    /**
     * 设置 订单编号 字段:t_asset_trade_flow.ORDER_NUMBER
     *
     * @param orderNumber t_asset_trade_flow.ORDER_NUMBER, 订单编号
     */
    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber == null ? null : orderNumber.trim();
    }

    /**
     * 获取 订单金额 字段:t_asset_trade_flow.ORDER_AMOUNT
     *
     * @return t_asset_trade_flow.ORDER_AMOUNT, 订单金额
     */
    public BigDecimal getOrderAmount() {
        return orderAmount;
    }

    /**
     * 设置 订单金额 字段:t_asset_trade_flow.ORDER_AMOUNT
     *
     * @param orderAmount t_asset_trade_flow.ORDER_AMOUNT, 订单金额
     */
    public void setOrderAmount(BigDecimal orderAmount) {
        this.orderAmount = orderAmount;
    }

    /**
     * 获取 币种 字段:t_asset_trade_flow.TRADE_CURRENCY
     *
     * @return t_asset_trade_flow.TRADE_CURRENCY, 币种
     */
    public String getTradeCurrency() {
        return tradeCurrency;
    }

    /**
     * 设置 币种 字段:t_asset_trade_flow.TRADE_CURRENCY
     *
     * @param tradeCurrency t_asset_trade_flow.TRADE_CURRENCY, 币种
     */
    public void setTradeCurrency(String tradeCurrency) {
        this.tradeCurrency = tradeCurrency == null ? null : tradeCurrency.trim();
    }

    /**
     * 获取 姓名 字段:t_asset_trade_flow.NAME
     *
     * @return t_asset_trade_flow.NAME, 姓名
     */
    public String getName() {
        return name;
    }

    /**
     * 设置 姓名 字段:t_asset_trade_flow.NAME
     *
     * @param name t_asset_trade_flow.NAME, 姓名
     */
    public void setName(String name) {
        this.name = name == null ? null : name.trim();
    }

    /**
     * 获取 银行账号 字段:t_asset_trade_flow.BANK_ACCOUNT
     *
     * @return t_asset_trade_flow.BANK_ACCOUNT, 银行账号
     */
    public String getBankAccount() {
        return bankAccount;
    }

    /**
     * 设置 银行账号 字段:t_asset_trade_flow.BANK_ACCOUNT
     *
     * @param bankAccount t_asset_trade_flow.BANK_ACCOUNT, 银行账号
     */
    public void setBankAccount(String bankAccount) {
        this.bankAccount = bankAccount == null ? null : bankAccount.trim();
    }

    /**
     * 获取 交易日期 字段:t_asset_trade_flow.TRADE_TIME
     *
     * @return t_asset_trade_flow.TRADE_TIME, 交易日期
     */
    public Date getTradeTime() {
        return tradeTime;
    }

    /**
     * 设置 交易日期 字段:t_asset_trade_flow.TRADE_TIME
     *
     * @param tradeTime t_asset_trade_flow.TRADE_TIME, 交易日期
     */
    public void setTradeTime(Date tradeTime) {
        this.tradeTime = tradeTime == null ? null : tradeTime;
    }

    /**
     * 获取 交易状态 字段:t_asset_trade_flow.TRADE_STATUS
     *
     * @return t_asset_trade_flow.TRADE_STATUS, 交易状态
     */
    public String getTradeStatus() {
        return tradeStatus;
    }

    /**
     * 设置 交易状态 字段:t_asset_trade_flow.TRADE_STATUS
     *
     * @param tradeStatus t_asset_trade_flow.TRADE_STATUS, 交易状态
     */
    public void setTradeStatus(String tradeStatus) {
        this.tradeStatus = tradeStatus == null ? null : tradeStatus.trim();
    }

    /**
     * 获取 交易摘要 字段:t_asset_trade_flow.TRADE_DIGEST
     *
     * @return t_asset_trade_flow.TRADE_DIGEST, 交易摘要
     */
    public String getTradeDigest() {
        return tradeDigest;
    }

    /**
     * 设置 交易摘要 字段:t_asset_trade_flow.TRADE_DIGEST
     *
     * @param tradeDigest t_asset_trade_flow.TRADE_DIGEST, 交易摘要
     */
    public void setTradeDigest(String tradeDigest) {
        this.tradeDigest = tradeDigest == null ? null : tradeDigest.trim();
    }

    /**
     * 获取 确认还款日期 字段:t_asset_trade_flow.CONFIRM_REPAY_TIME
     *
     * @return t_asset_trade_flow.CONFIRM_REPAY_TIME, 确认还款日期
     */
    public Date getConfirmRepayTime() {
        return confirmRepayTime;
    }

    /**
     * 设置 确认还款日期 字段:t_asset_trade_flow.CONFIRM_REPAY_TIME
     *
     * @param confirmRepayTime t_asset_trade_flow.CONFIRM_REPAY_TIME, 确认还款日期
     */
    public void setConfirmRepayTime(Date confirmRepayTime) {
        this.confirmRepayTime = confirmRepayTime == null ? null : confirmRepayTime;
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
        return "AssetTradeFlow{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", tradeChannel='" + tradeChannel + '\'' +
                ", tradeType='" + tradeType + '\'' +
                ", orderNumber='" + orderNumber + '\'' +
                ", orderAmount=" + orderAmount +
                ", tradeCurrency='" + tradeCurrency + '\'' +
                ", name='" + name + '\'' +
                ", bankAccount='" + bankAccount + '\'' +
                ", tradeTime=" + tradeTime +
                ", tradeStatus='" + tradeStatus + '\'' +
                ", tradeDigest='" + tradeDigest + '\'' +
                ", confirmRepayTime=" + confirmRepayTime +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}