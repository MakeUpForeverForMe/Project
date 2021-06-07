package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.common.excel.annotation.ExcelEntity;
import com.weshare.abscore.common.excel.annotation.ExcelProperty;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.math.BigDecimal;
import java.util.Date;

/**
 * excel导入复用
 * 还款计划  File05
 **/
@ExcelEntity
public class RepaymentPlan extends BaseEntity {

    /**
     * excel中的行数
     **/
    private int location;

    /**
     * 项目编号
     * 表字段 : t_repayment_plan.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_repayment_plan.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_repayment_plan.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    @ExcelProperty(index = 0, value = "借据号", required = true)
    private String serialNumber;

    /**
     * 期次
     * 表字段 : t_repayment_plan.PERIOD
     */
    @SerializedName("期次")
    @ExcelProperty(index = 1, value = "期次", required = true, checkPositiveInteger = true)
    private Integer period;

    /**
     * 应还款日
     * 表字段 : t_repayment_plan.SHOULD_REPAY_DATE
     */
    @SerializedName("应还款日")
    @ExcelProperty(index = 2, value = "还款日", required = true)
    private Date shouldRepayDate;

    /**
     * 区间开始日(非表字段)
     */
    private Date beginDate;

    /**
     * 合同编号(非表字段)
     */
    private String contractCode;
    /**
     * 期初剩余本金
     */
    private BigDecimal beginPrincipalBalance;
    /**
     * 应还本金
     * 表字段 : t_repayment_plan.SHOULD_REPAY_PRINCIPAL
     */
    @SerializedName("应还本金(元)")
    @ExcelProperty(index = 3, value = "还本金额(元)", required = true)
    private BigDecimal shouldRepayPrincipal;

    /**
     * 应还利息
     * 表字段 : t_repayment_plan.SHOULD_REPAY_INTEREST
     */
    @SerializedName("应还利息(元)")
    @ExcelProperty(index = 4, value = "付息金额(元)", required = true)
    private BigDecimal shouldRepayInterest;

    /**
     * 应还费用
     * 表字段 : t_repayment_plan.SHOULD_REPAY_COST
     */
    @SerializedName("应还费用(元)")
    @ExcelProperty(index = 5, value = "费用金额(元)", required = true)
    private BigDecimal shouldRepayCost;

    /**
     * 期末剩余本金
     */
    private BigDecimal endPrincipalBalance;
    /**
     * 生效日期
     * 表字段 : t_repayment_plan.EFFECTIVE_DATE
     */
    @SerializedName("生效日期")
    private Date effectiveDate;

    /**
     * 还款状态 1:入池前已还,2:入池前未还
     */
    private Integer repayStatus;

    /**
     * 导入Id
     **/
    //private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport,3:systemgenerated
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_repayment_plan.PROJECT_ID
     *
     * @return t_repayment_plan.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_repayment_plan.PROJECT_ID
     *
     * @param projectId t_repayment_plan.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_repayment_plan.AGENCY_ID
     *
     * @return t_repayment_plan.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_repayment_plan.AGENCY_ID
     *
     * @param agencyId t_repayment_plan.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_repayment_plan.SERIAL_NUMBER
     *
     * @return t_repayment_plan.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_repayment_plan.SERIAL_NUMBER
     *
     * @param serialNumber t_repayment_plan.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 期次 字段:t_repayment_plan.PERIOD
     *
     * @return t_repayment_plan.PERIOD, 期次
     */
    public Integer getPeriod() {
        return period;
    }

    /**
     * 设置 期次 字段:t_repayment_plan.PERIOD
     *
     * @param period t_repayment_plan.PERIOD, 期次
     */
    public void setPeriod(Integer period) {
        this.period = period;
    }

    /**
     * 获取 应还款日 字段:t_repayment_plan.SHOULD_REPAY_DATE
     *
     * @return t_repayment_plan.SHOULD_REPAY_DATE, 应还款日
     */
    public Date getShouldRepayDate() {
        return shouldRepayDate;
    }

    /**
     * 设置 应还款日 字段:t_repayment_plan.SHOULD_REPAY_DATE
     *
     * @param shouldRepayDate t_repayment_plan.SHOULD_REPAY_DATE, 应还款日
     */
    public void setShouldRepayDate(Date shouldRepayDate) {
        this.shouldRepayDate = shouldRepayDate == null ? null : shouldRepayDate;
    }

    /**
     * 获取 应还本金 字段:t_repayment_plan.SHOULD_REPAY_PRINCIPAL
     *
     * @return t_repayment_plan.SHOULD_REPAY_PRINCIPAL, 应还本金
     */
    public BigDecimal getShouldRepayPrincipal() {
        return shouldRepayPrincipal;
    }

    /**
     * 设置 应还本金 字段:t_repayment_plan.SHOULD_REPAY_PRINCIPAL
     *
     * @param shouldRepayPrincipal t_repayment_plan.SHOULD_REPAY_PRINCIPAL, 应还本金
     */
    public void setShouldRepayPrincipal(BigDecimal shouldRepayPrincipal) {
        this.shouldRepayPrincipal = shouldRepayPrincipal;
    }

    /**
     * 获取 应还利息 字段:t_repayment_plan.SHOULD_REPAY_INTEREST
     *
     * @return t_repayment_plan.SHOULD_REPAY_INTEREST, 应还利息
     */
    public BigDecimal getShouldRepayInterest() {
        return shouldRepayInterest;
    }

    /**
     * 设置 应还利息 字段:t_repayment_plan.SHOULD_REPAY_INTEREST
     *
     * @param shouldRepayInterest t_repayment_plan.SHOULD_REPAY_INTEREST, 应还利息
     */
    public void setShouldRepayInterest(BigDecimal shouldRepayInterest) {
        this.shouldRepayInterest = shouldRepayInterest;
    }

    /**
     * 获取 应还费用 字段:t_repayment_plan.SHOULD_REPAY_COST
     *
     * @return t_repayment_plan.SHOULD_REPAY_COST, 应还费用
     */
    public BigDecimal getShouldRepayCost() {
        return shouldRepayCost;
    }

    /**
     * 设置 应还费用 字段:t_repayment_plan.SHOULD_REPAY_COST
     *
     * @param shouldRepayCost t_repayment_plan.SHOULD_REPAY_COST, 应还费用
     */
    public void setShouldRepayCost(BigDecimal shouldRepayCost) {
        this.shouldRepayCost = shouldRepayCost;
    }

    /**
     * 获取 生效日期 字段:t_repayment_plan.EFFECTIVE_DATE
     *
     * @return t_repayment_plan.EFFECTIVE_DATE, 生效日期
     */
    public Date getEffectiveDate() {
        return effectiveDate;
    }

    /**
     * 设置 生效日期 字段:t_repayment_plan.EFFECTIVE_DATE
     *
     * @param effectiveDate t_repayment_plan.EFFECTIVE_DATE, 生效日期
     */
    public void setEffectiveDate(Date effectiveDate) {
        this.effectiveDate = effectiveDate == null ? null : effectiveDate;
    }

    public BigDecimal getBeginPrincipalBalance() {
        return beginPrincipalBalance;
    }

    public void setBeginPrincipalBalance(BigDecimal beginPrincipalBalance) {
        this.beginPrincipalBalance = beginPrincipalBalance;
    }

    public BigDecimal getEndPrincipalBalance() {
        return endPrincipalBalance;
    }

    public void setEndPrincipalBalance(BigDecimal endPrincipalBalance) {
        this.endPrincipalBalance = endPrincipalBalance;
    }

    public Date getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(Date beginDate) {
        this.beginDate = beginDate;
    }

    public String getContractCode() {
        return contractCode;
    }

    public void setContractCode(String contractCode) {
        this.contractCode = contractCode;
    }

    public Integer getRepayStatus() {
        return repayStatus;
    }

    public void setRepayStatus(Integer repayStatus) {
        this.repayStatus = repayStatus;
    }

    public Integer getDataSource() {
        return dataSource;
    }

    public void setDataSource(Integer dataSource) {
        this.dataSource = dataSource;
    }

    public int getLocation() {
        return location;
    }

    public void setLocation(int location) {
        this.location = location;
    }

    @Override
    public String toString() {
        return "RepaymentPlan{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", period=" + period +
                ", shouldRepayDate=" + shouldRepayDate +
                ", beginDate=" + beginDate +
                ", contractCode='" + contractCode + '\'' +
                ", beginPrincipalBalance=" + beginPrincipalBalance +
                ", shouldRepayPrincipal=" + shouldRepayPrincipal +
                ", shouldRepayInterest=" + shouldRepayInterest +
                ", shouldRepayCost=" + shouldRepayCost +
                ", endPrincipalBalance=" + endPrincipalBalance +
                ", effectiveDate=" + effectiveDate +
                ", repayStatus=" + repayStatus +
                ", importId=" + "" +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}