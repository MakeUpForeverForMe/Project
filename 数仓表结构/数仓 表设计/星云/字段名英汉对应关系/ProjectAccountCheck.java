package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.math.BigDecimal;
import java.util.Date;

/**
 *  项目对账信息  File11
 **/
public class ProjectAccountCheck extends BaseEntity {

    /**
     * 项目编号
     * 表字段 : t_project_account_check.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_project_account_check.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 贷款总数量
     * 表字段 : t_project_account_check.LOAN_SUM
     */
    @SerializedName("贷款总笔数")
    private Integer loanSum;

    /**
     * 贷款剩余本金
     * 表字段 : t_project_account_check.LOAN_REMAINING_PRINCIPAL
     */
    @SerializedName("贷款剩余本金")
    private BigDecimal loanRemainingPrincipal;

    /**
     * 贷款合同总金额
     * 表字段 : t_project_account_check.LOAN_TOTALAMOUNT
     */
    @SerializedName("贷款合同总金额")
    private BigDecimal loanTotalamount;

    /**
     * 1~7逾期资产数
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_SUM1
     */
    @SerializedName("1~7逾期资产数")
    private Integer overdueAssetsSum1;

    /**
     * 1~7逾期资产剩余未还本金
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_OUTSTANDING_PRINCIPAL1
     */
    @SerializedName("1~7逾期资产剩余未还本金")
    private BigDecimal overdueAssetsOutstandingPrincipal1;

    /**
     * 8~30逾期资产数
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_SUM8
     */
    @SerializedName("8~30逾期资产数")
    private Integer overdueAssetsSum8;

    /**
     * 8~30逾期资产剩余未还本金
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_OUTSTANDING_PRINCIPAL8
     */
    @SerializedName("8~30逾期资产剩余未还本金")
    private BigDecimal overdueAssetsOutstandingPrincipal8;

    /**
     * 31~60逾期资产数
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_SUM31
     */
    @SerializedName("31~60逾期资产数")
    private Integer overdueAssetsSum31;

    /**
     * 31~60逾期资产剩余未还本金
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_OUTSTANDING_PRINCIPAL31
     */
    @SerializedName("31~60逾期资产剩余未还本金")
    private BigDecimal overdueAssetsOutstandingPrincipal31;

    /**
     * 61~90逾期资产数
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_SUM61
     */
    @SerializedName("61~90逾期资产数")
    private Integer overdueAssetsSum61;

    /**
     * 61~90逾期资产剩余未还本金
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_OUTSTANDING_PRINCIPAL61
     */
    @SerializedName("61~90逾期资产剩余未还本金")
    private BigDecimal overdueAssetsOutstandingPrincipal61;

    /**
     * 91~180逾期资产数
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_SUM91
     */
    @SerializedName("91~180逾期资产数")
    private Integer overdueAssetsSum91;

    /**
     * 91~180逾期资产剩余未还本金
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_OUTSTANDING_PRINCIPAL91
     */
    @SerializedName("91~180逾期资产剩余未还本金")
    private BigDecimal overdueAssetsOutstandingPrincipal91;

    /**
     * 180+逾期资产数
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_SUM180
     */
    @SerializedName("180+逾期资产数")
    private Integer overdueAssetsSum180;

    /**
     * 180+逾期资产剩余未还本金
     * 表字段 : t_project_account_check.OVERDUE_ASSETS_OUTSTANDING_PRINCIPAL180
     */
    @SerializedName("180+逾期资产剩余未还本金")
    private BigDecimal overdueAssetsOutstandingPrincipal180;

    /**
     * 当日新增贷款笔数
     * 表字段 : t_project_account_check.ADD_LOAN_SUM
     */
    @SerializedName("当日新增贷款笔数")
    private Integer addLoanSum;

    /**
     * 当日新增贷款总金额
     * 表字段 : t_project_account_check.ADD_LOAN_TOTALAMOUNT
     */
    @SerializedName("当日新增贷款总金额")
    private BigDecimal addLoanTotalamount;

    /**
     * 当日实还资产笔数
     * 表字段 : t_project_account_check.ACTUAL_PAYMENT_SUM
     */
    @SerializedName("当日实还资产笔数")
    private Integer actualPaymentSum;

    /**
     * 当日实还总金额
     * 表字段 : t_project_account_check.ACTUAL_PAYMENT_TOTALAMOUNT
     */
    @SerializedName("当日实还总金额")
    private BigDecimal actualPaymentTotalamount;

    /**
     * 当日回购资产笔数
     * 表字段 : t_project_account_check.BACK_ASSETS_SUM
     */
    @SerializedName("当日回购资产笔数")
    private Integer backAssetsSum;

    /**
     * 当日回购总金额
     * 表字段 : t_project_account_check.BACK_ASSETS_TOTALAMOUNT
     */
    @SerializedName("当日回购总金额")
    private BigDecimal backAssetsTotalamount;

    /**
     * 当日处置资产笔数
     * 表字段 : t_project_account_check.DISPOSAL_ASSETS_SUM
     */
    @SerializedName("当日处置资产笔数")
    private Integer disposalAssetsSum;

    /**
     * 当日处置回收总金额
     * 表字段 : t_project_account_check.DISPOSAL_ASSETS_TOTALAMOUNT
     */
    @SerializedName("当日处置回收总金额")
    private BigDecimal disposalAssetsTotalamount;

    /**
     * 当日差额补足资产笔数
     * 表字段 : t_project_account_check.DIFFERENTIA_COMPLEMENT_ASSETS_SUM
     */
    @SerializedName("当日差额补足资产笔数")
    private Integer differentiaComplementAssetsSum;

    /**
     * 当日差额补足总金额
     * 表字段 : t_project_account_check.DIFFERENTIA_COMPLEMENT_ASSETS_TOTALAMOUNT
     */
    @SerializedName("当日差额补足总金额")
    private BigDecimal differentiaComplementAssetsTotalamount;

    /**
     * 当日代偿资产笔数
     * 表字段 : t_project_account_check.COMPENSATORY_ASSETS_SUM
     */
    @SerializedName("当日代偿资产笔数")
    private Integer compensatoryAssetsSum;

    /**
     * 当日代偿总金额
     * 表字段 : t_project_account_check.COMPENSATORY_ASSETS_TOTALAMOUNT
     */
    @SerializedName("当日代偿总金额")
    private BigDecimal compensatoryAssetsTotalamount;

    /**
     * 数据提取日
     * 表字段 : t_project_account_check.DATA_EXTRACTION_DAY
     */
    @SerializedName("数据提取日")
    private Date dataExtractionDay;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getAgencyId() {
        return agencyId;
    }

    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId;
    }

    public Integer getLoanSum() {
        return loanSum;
    }

    public void setLoanSum(Integer loanSum) {
        this.loanSum = loanSum;
    }

    public BigDecimal getLoanRemainingPrincipal() {
        return loanRemainingPrincipal;
    }

    public void setLoanRemainingPrincipal(BigDecimal loanRemainingPrincipal) {
        this.loanRemainingPrincipal = loanRemainingPrincipal;
    }

    public BigDecimal getLoanTotalamount() {
        return loanTotalamount;
    }

    public void setLoanTotalamount(BigDecimal loanTotalamount) {
        this.loanTotalamount = loanTotalamount;
    }

    public Integer getOverdueAssetsSum1() {
        return overdueAssetsSum1;
    }

    public void setOverdueAssetsSum1(Integer overdueAssetsSum1) {
        this.overdueAssetsSum1 = overdueAssetsSum1;
    }

    public BigDecimal getOverdueAssetsOutstandingPrincipal1() {
        return overdueAssetsOutstandingPrincipal1;
    }

    public void setOverdueAssetsOutstandingPrincipal1(BigDecimal overdueAssetsOutstandingPrincipal1) {
        this.overdueAssetsOutstandingPrincipal1 = overdueAssetsOutstandingPrincipal1;
    }

    public Integer getOverdueAssetsSum8() {
        return overdueAssetsSum8;
    }

    public void setOverdueAssetsSum8(Integer overdueAssetsSum8) {
        this.overdueAssetsSum8 = overdueAssetsSum8;
    }

    public BigDecimal getOverdueAssetsOutstandingPrincipal8() {
        return overdueAssetsOutstandingPrincipal8;
    }

    public void setOverdueAssetsOutstandingPrincipal8(BigDecimal overdueAssetsOutstandingPrincipal8) {
        this.overdueAssetsOutstandingPrincipal8 = overdueAssetsOutstandingPrincipal8;
    }

    public Integer getOverdueAssetsSum31() {
        return overdueAssetsSum31;
    }

    public void setOverdueAssetsSum31(Integer overdueAssetsSum31) {
        this.overdueAssetsSum31 = overdueAssetsSum31;
    }

    public BigDecimal getOverdueAssetsOutstandingPrincipal31() {
        return overdueAssetsOutstandingPrincipal31;
    }

    public void setOverdueAssetsOutstandingPrincipal31(BigDecimal overdueAssetsOutstandingPrincipal31) {
        this.overdueAssetsOutstandingPrincipal31 = overdueAssetsOutstandingPrincipal31;
    }

    public Integer getOverdueAssetsSum61() {
        return overdueAssetsSum61;
    }

    public void setOverdueAssetsSum61(Integer overdueAssetsSum61) {
        this.overdueAssetsSum61 = overdueAssetsSum61;
    }

    public BigDecimal getOverdueAssetsOutstandingPrincipal61() {
        return overdueAssetsOutstandingPrincipal61;
    }

    public void setOverdueAssetsOutstandingPrincipal61(BigDecimal overdueAssetsOutstandingPrincipal61) {
        this.overdueAssetsOutstandingPrincipal61 = overdueAssetsOutstandingPrincipal61;
    }

    public Integer getOverdueAssetsSum91() {
        return overdueAssetsSum91;
    }

    public void setOverdueAssetsSum91(Integer overdueAssetsSum91) {
        this.overdueAssetsSum91 = overdueAssetsSum91;
    }

    public BigDecimal getOverdueAssetsOutstandingPrincipal91() {
        return overdueAssetsOutstandingPrincipal91;
    }

    public void setOverdueAssetsOutstandingPrincipal91(BigDecimal overdueAssetsOutstandingPrincipal91) {
        this.overdueAssetsOutstandingPrincipal91 = overdueAssetsOutstandingPrincipal91;
    }

    public Integer getOverdueAssetsSum180() {
        return overdueAssetsSum180;
    }

    public void setOverdueAssetsSum180(Integer overdueAssetsSum180) {
        this.overdueAssetsSum180 = overdueAssetsSum180;
    }

    public BigDecimal getOverdueAssetsOutstandingPrincipal180() {
        return overdueAssetsOutstandingPrincipal180;
    }

    public void setOverdueAssetsOutstandingPrincipal180(BigDecimal overdueAssetsOutstandingPrincipal180) {
        this.overdueAssetsOutstandingPrincipal180 = overdueAssetsOutstandingPrincipal180;
    }

    public Integer getAddLoanSum() {
        return addLoanSum;
    }

    public void setAddLoanSum(Integer addLoanSum) {
        this.addLoanSum = addLoanSum;
    }

    public BigDecimal getAddLoanTotalamount() {
        return addLoanTotalamount;
    }

    public void setAddLoanTotalamount(BigDecimal addLoanTotalamount) {
        this.addLoanTotalamount = addLoanTotalamount;
    }

    public Integer getActualPaymentSum() {
        return actualPaymentSum;
    }

    public void setActualPaymentSum(Integer actualPaymentSum) {
        this.actualPaymentSum = actualPaymentSum;
    }

    public BigDecimal getActualPaymentTotalamount() {
        return actualPaymentTotalamount;
    }

    public void setActualPaymentTotalamount(BigDecimal actualPaymentTotalamount) {
        this.actualPaymentTotalamount = actualPaymentTotalamount;
    }

    public Integer getBackAssetsSum() {
        return backAssetsSum;
    }

    public void setBackAssetsSum(Integer backAssetsSum) {
        this.backAssetsSum = backAssetsSum;
    }

    public BigDecimal getBackAssetsTotalamount() {
        return backAssetsTotalamount;
    }

    public void setBackAssetsTotalamount(BigDecimal backAssetsTotalamount) {
        this.backAssetsTotalamount = backAssetsTotalamount;
    }

    public Integer getDisposalAssetsSum() {
        return disposalAssetsSum;
    }

    public void setDisposalAssetsSum(Integer disposalAssetsSum) {
        this.disposalAssetsSum = disposalAssetsSum;
    }

    public BigDecimal getDisposalAssetsTotalamount() {
        return disposalAssetsTotalamount;
    }

    public void setDisposalAssetsTotalamount(BigDecimal disposalAssetsTotalamount) {
        this.disposalAssetsTotalamount = disposalAssetsTotalamount;
    }

    public Integer getDifferentiaComplementAssetsSum() {
        return differentiaComplementAssetsSum;
    }

    public void setDifferentiaComplementAssetsSum(Integer differentiaComplementAssetsSum) {
        this.differentiaComplementAssetsSum = differentiaComplementAssetsSum;
    }

    public BigDecimal getDifferentiaComplementAssetsTotalamount() {
        return differentiaComplementAssetsTotalamount;
    }

    public void setDifferentiaComplementAssetsTotalamount(BigDecimal differentiaComplementAssetsTotalamount) {
        this.differentiaComplementAssetsTotalamount = differentiaComplementAssetsTotalamount;
    }

    public Integer getCompensatoryAssetsSum() {
        return compensatoryAssetsSum;
    }

    public void setCompensatoryAssetsSum(Integer compensatoryAssetsSum) {
        this.compensatoryAssetsSum = compensatoryAssetsSum;
    }

    public BigDecimal getCompensatoryAssetsTotalamount() {
        return compensatoryAssetsTotalamount;
    }

    public void setCompensatoryAssetsTotalamount(BigDecimal compensatoryAssetsTotalamount) {
        this.compensatoryAssetsTotalamount = compensatoryAssetsTotalamount;
    }

    public Date getDataExtractionDay() {
        return dataExtractionDay;
    }

    public void setDataExtractionDay(Date dataExtractionDay) {
        this.dataExtractionDay = dataExtractionDay;
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
        return "ProjectAccountCheck{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", loanSum=" + loanSum +
                ", loanRemainingPrincipal=" + loanRemainingPrincipal +
                ", loanTotalamount=" + loanTotalamount +
                ", overdueAssetsSum1=" + overdueAssetsSum1 +
                ", overdueAssetsOutstandingPrincipal1=" + overdueAssetsOutstandingPrincipal1 +
                ", overdueAssetsSum8=" + overdueAssetsSum8 +
                ", overdueAssetsOutstandingPrincipal8=" + overdueAssetsOutstandingPrincipal8 +
                ", overdueAssetsSum31=" + overdueAssetsSum31 +
                ", overdueAssetsOutstandingPrincipal31=" + overdueAssetsOutstandingPrincipal31 +
                ", overdueAssetsSum61=" + overdueAssetsSum61 +
                ", overdueAssetsOutstandingPrincipal61=" + overdueAssetsOutstandingPrincipal61 +
                ", overdueAssetsSum91=" + overdueAssetsSum91 +
                ", overdueAssetsOutstandingPrincipal91=" + overdueAssetsOutstandingPrincipal91 +
                ", overdueAssetsSum180=" + overdueAssetsSum180 +
                ", overdueAssetsOutstandingPrincipal180=" + overdueAssetsOutstandingPrincipal180 +
                ", addLoanSum=" + addLoanSum +
                ", addLoanTotalamount=" + addLoanTotalamount +
                ", actualPaymentSum=" + actualPaymentSum +
                ", actualPaymentTotalamount=" + actualPaymentTotalamount +
                ", backAssetsSum=" + backAssetsSum +
                ", backAssetsTotalamount=" + backAssetsTotalamount +
                ", disposalAssetsSum=" + disposalAssetsSum +
                ", disposalAssetsTotalamount=" + disposalAssetsTotalamount +
                ", differentiaComplementAssetsSum=" + differentiaComplementAssetsSum +
                ", differentiaComplementAssetsTotalamount=" + differentiaComplementAssetsTotalamount +
                ", compensatoryAssetsSum=" + compensatoryAssetsSum +
                ", compensatoryAssetsTotalamount=" + compensatoryAssetsTotalamount +
                ", dataExtractionDay=" + dataExtractionDay +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}