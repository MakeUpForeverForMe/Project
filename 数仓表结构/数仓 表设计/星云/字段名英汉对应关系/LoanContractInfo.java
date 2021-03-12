package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 借款合同信息  File01
 **/
public class LoanContractInfo extends BaseEntity {

    /**
     * 项目编号
     * 表字段 : t_loan_contract_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_loan_contract_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_loan_contract_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 资产类型-枚举
     * 预定义字段：
     * 个人消费贷款
     * 汽车抵押贷款
     * 住房抵押贷款
     * <p>
     * 表字段 : t_loan_contract_info.ASSET_TYPE
     */
    @SerializedName("资产类型")
    private String assetType;

    /**
     * 贷款合同编号
     * 表字段 : t_loan_contract_info.CONTRACT_CODE
     */
    @SerializedName("贷款合同编号")
    private String contractCode;

    /**
     * 产品编号
     * 表字段 : t_loan_contract_info.PRODUCT_ID
     */
    @SerializedName("产品编号")
    private String productId;

    /**
     * 产品方案名称
     * 表字段 : t_loan_contract_info.PRODUCT_SCHEME_NAME
     */
    @SerializedName("产品方案名称")
    private String productSchemeName;

    /**
     * 贷款总金额(元)
     * 表字段 : t_loan_contract_info.CONTRACT_AMOUNT
     */
    @SerializedName("贷款总金额(元)")
    private BigDecimal contractAmount = BigDecimal.ZERO;

    /**
     * 利率类型-枚举
     * 固定利率
     * 浮动利率
     * <p>
     * 表字段 : t_loan_contract_info.INTEREST_RATE_TYPE
     */
    @SerializedName("利率类型")
    private String interestRateType = StringUtils.EMPTY;

    /**
     * 贷款年利率(%)
     * 表字段 : t_loan_contract_info.CONTRACT_INTEREST_RATE
     */
    @SerializedName("贷款年利率(%)")
    private BigDecimal contractInterestRate = BigDecimal.ZERO;

    /**
     * 贷款日利率(%)
     * 表字段 : t_loan_contract_info.CONTRACT_DAILY_RATE
     */
    @SerializedName("贷款日利率(%)")
    private BigDecimal contractDailyRate = BigDecimal.ZERO;

    /**
     * 贷款月利率(%)
     * 表字段 : t_loan_contract_info.CONTRACT_AN_RATE
     */
    @SerializedName("贷款月利率(%)")
    private BigDecimal contractAnRate = BigDecimal.ZERO;

    /**
     * 日利率计算基础
     * 表字段 : t_loan_contract_info.DAILY_CALCULATION_BASIS
     */
    @SerializedName("日利率计算基础")
    private String dailyCalculationBasis = StringUtils.EMPTY;

    /**
     * 还款方式-枚举
     * 预定义字段：
     * 等额本息，
     * 等额本金，
     * 等本等息，
     * 先息后本，
     * 一次性还本付息
     * 气球贷
     * 自定义还款计划
     * <p>
     * 表字段 : t_loan_contract_info.REPAYMENT_TYPE
     */
    @SerializedName("还款方式")
    private String repaymentType = StringUtils.EMPTY;

    /**
     * 还款频率-枚举
     * 预定义字段：
     * 月，
     * 季，
     * 半年，
     * 年，
     * 到期一次付清
     * 自定义还款频率
     * <p>
     * 表字段 : t_loan_contract_info.REPAYMENT_FREQUENCY
     */
    @SerializedName("还款频率")
    private String repaymentFrequency = StringUtils.EMPTY;

    /**
     * 总期数
     * 表字段 : t_loan_contract_info.PERIODS
     */
    @SerializedName("总期数")
    private Integer periods;

    /**
     * 总收益率(%)
     * 表字段 : t_loan_contract_info.PROFIT_TOL_RATE
     */
    @SerializedName("总收益率(%)")
    private BigDecimal profitTolRate = BigDecimal.ZERO;

    /**
     * 剩余本金(元)
     * 表字段 : t_loan_contract_info.REST_PRINCIPAL
     */
    @SerializedName("剩余本金(元)")
    private BigDecimal restPrincipal = BigDecimal.ZERO;

    /**
     * 剩余费用其他(元)
     * 表字段 : t_loan_contract_info.REST_OTHER_COST
     */
    @SerializedName("剩余其他费用(元)")
    private BigDecimal restOtherCost = BigDecimal.ZERO;

    /**
     * 剩余利息(元)
     * 表字段 : t_loan_contract_info.REST_INTEREST
     */
    @SerializedName("剩余利息(元)")
    private BigDecimal restInterest = BigDecimal.ZERO;

    /**
     * 首付款金额(元)
     * 表字段 : t_loan_contract_info.SHOUFU_AMOUNT
     */
    @SerializedName("首付款金额(元)")
    private BigDecimal shoufuAmount = BigDecimal.ZERO;

    /**
     * 首付比例(%)
     * 表字段 : t_loan_contract_info.SHOUFU_PROPORTION
     */
    @SerializedName("首付比例(%)")
    private BigDecimal shoufuProportion = BigDecimal.ZERO;

    /**
     * 尾付款金额(元)
     * 表字段 : t_loan_contract_info.TAIL_AMOUNT
     */
    @SerializedName("尾付款金额(元)")
    private BigDecimal tailAmount = BigDecimal.ZERO;

    /**
     * 尾付比例(%)
     * 表字段 : t_loan_contract_info.TAIL_AMOUNT_RATE
     */
    @SerializedName("尾付比例(%)")
    private BigDecimal tailAmountRate = BigDecimal.ZERO;

    /**
     * 手续费
     * 表字段 : t_loan_contract_info.POUNDAGE
     */
    @SerializedName("手续费")
    private BigDecimal poundage = BigDecimal.ZERO;

    /**
     * 手续费利率
     * 表字段 : t_loan_contract_info.POUNDAGE_RATE
     */
    @SerializedName("手续费利率")
    private BigDecimal poundageRate = BigDecimal.ZERO;

    /**
     * 手续费扣款方式-枚举
     * 预定义字段：
     * 一次性
     * 分期扣款
     * <p>
     * 表字段 : t_loan_contract_info.POUNDAGE_DEDUCTION_TYPE
     */
    @SerializedName("手续费扣款方式")
    private String poundageDeductionType = StringUtils.EMPTY;

    /**
     * 结算手续费率
     * 表字段 : t_loan_contract_info.SETTLEMENT_POUNDAGE_RATE
     */
    @SerializedName("结算手续费率")
    private BigDecimal settlementPoundageRate = BigDecimal.ZERO;

    /**
     * 补贴手续费
     * 表字段 : t_loan_contract_info.SUBSIDIE_POUNDAGE
     */
    @SerializedName("补贴手续费")
    private BigDecimal subsidiePoundage = BigDecimal.ZERO;

    /**
     * 贴息金额
     * 表字段 : t_loan_contract_info.DISCOUNT_AMOUNT
     */
    @SerializedName("贴息金额")
    private BigDecimal discountAmount = BigDecimal.ZERO;

    /**
     * 保证金比例
     * 表字段 : t_loan_contract_info.MARGIN_RATE
     */
    @SerializedName("保证金比例")
    private BigDecimal marginRate = BigDecimal.ZERO;

    /**
     * 保证金
     * 表字段 : t_loan_contract_info.MARGIN
     */
    @SerializedName("保证金")
    private BigDecimal margin = BigDecimal.ZERO;

    /**
     * 保证金冲抵方式-枚举
     * 预定义字段：
     * 不冲抵
     * 冲抵
     * <p>
     * 表字段 : t_loan_contract_info.MARGIN_OFFSET_TYPE
     */
    @SerializedName("保证金冲抵方式")
    private String marginOffsetType = StringUtils.EMPTY;

    /**
     * 帐户管理费
     * 表字段 : t_loan_contract_info.ACCOUNT_MANAGEMENT_EXPENSE
     */
    @SerializedName("帐户管理费")
    private BigDecimal accountManagementExpense = BigDecimal.ZERO;

    /**
     * 抵押率(%)
     * 表字段 : t_loan_contract_info.MORTGAGE_RATE
     */
    @SerializedName("抵押率(%)")
    private BigDecimal mortgageRate = BigDecimal.ZERO;

    /**
     * 合同开始时间
     * 表字段 : t_loan_contract_info.LOAN_ISSUE_DATE
     */
    @SerializedName("合同开始时间")
    private Date loanIssueDate;

    /**
     * 合同结束时间
     * 表字段 : t_loan_contract_info.LOAN_EXPIRY_DATE
     */
    @SerializedName("合同结束时间")
    private Date loanExpiryDate;

    /**
     * 实际放款时间
     * 表字段 : t_loan_contract_info.ACTUAL_LOAN_DATE
     */
    @SerializedName("实际放款时间")
    private Date actualLoanDate;

    /**
     * 首次还款时间
     * 表字段 : t_loan_contract_info.FRIST_REPAYMENT_DATE
     */
    @SerializedName("首次还款时间")
    private Date fristRepaymentDate;

    /**
     * 贷款还款日
     * 表字段 : t_loan_contract_info.LOAN_REPAY_DATE
     */
    @SerializedName("贷款还款日")
    private String loanRepayDate = StringUtils.EMPTY;

    /**
     * 最后一次预计扣款时间
     * 表字段 : t_loan_contract_info.LAST_ESTIMATED_DEDUCTION_DATE
     */
    @SerializedName("最后一次预计扣款时间")
    private Date lastEstimatedDeductionDate;

    /**
     * 月还款额
     * 表字段 : t_loan_contract_info.MONTH_REPAY_AMOUNT
     */
    @SerializedName("月还款额")
    private BigDecimal monthRepayAmount = BigDecimal.ZERO;

    /**
     * 贷款用途-枚举
     * 预定义字段：
     * 购房，
     * 购车，
     * 车抵押,
     * 装修，
     * 旅游，
     * 3C，
     * 教育，
     * 医美，
     * 经营类
     * 其他
     * 农业，
     * 个人综合消费
     * <p>
     * 表字段 : t_loan_contract_info.LOAN_USE
     */
    @SerializedName("贷款用途")
    private String loanUse = StringUtils.EMPTY;

    /**
     * 担保方式-枚举
     * 预定义字段：
     * 质押担保，
     * 信用担保，
     * 保证担保，
     * 抵押担保
     * <p>
     * 表字段 : t_loan_contract_info.GUARANTEE_TYPE
     */
    @SerializedName("担保方式")
    private String guaranteeType = StringUtils.EMPTY;

    /**
     * 累计逾期期数
     * 表字段 : t_loan_contract_info.TOTAL_OVERDUE_DAYNUM
     */
    @SerializedName("累计逾期期数")
    private Integer totalOverdueDaynum = 0;

    /**
     * 历史最高逾期天数
     * 表字段 : t_loan_contract_info.HISTORY_MOST_OVERDUE_DAYNUM
     */
    @SerializedName("历史最高逾期天数")
    private Integer historyMostOverdueDaynum = 0;

    /**
     * 历史累计逾期天数
     * 表字段 : t_loan_contract_info.HISTORY_TOTAL_OVERDUE_DAYNUM
     */
    @SerializedName("历史累计逾期天数")
    private Integer historyTotalOverdueDaynum = 0;

    /**
     * 当前逾期天数
     * 表字段 : t_loan_contract_info.CURRENT_OVERDUE_DAYNUM
     */
    @SerializedName("当前逾期天数")
    private Integer currentOverdueDaynum = 0;

    /**
     * 合同状态-枚举
     * 预定义字段：
     * 生效
     * 不生效
     * <p>
     * 表字段 : t_loan_contract_info.CONTRACT_STATUS
     */
    @SerializedName("合同状态")
    private String contractStatus = StringUtils.EMPTY;

    /**
     * 申请状态代码-枚举
     * "预定义字段：
     * 已放款
     * 放款失败"
     * <p>
     * 表字段 : t_loan_contract_info.APPLY_STATUS_CODE
     */
    @SerializedName("申请状态代码")
    private String applyStatusCode = StringUtils.EMPTY;

    /**
     * 申请渠道
     * 表字段 : t_loan_contract_info.APPLY_CHANNEL
     */
    @SerializedName("申请渠道")
    private String applyChannel = StringUtils.EMPTY;

    /**
     * 申请地点
     * 表字段 : t_loan_contract_info.APPLY_PLACE
     */
    @SerializedName("申请地点")
    private String applyPlace = StringUtils.EMPTY;

    /**
     * 借款人状态-
     * "1-首次申请
     * 2-内部续贷
     * 3-其他机构转单
     * 9-其他"
     * <p>
     * 表字段 : t_loan_contract_info.BORROWERP_STATUS
     */
    @SerializedName("借款人状态")
    private String borrowerpStatus = StringUtils.EMPTY;

    /**
     * 经销商名称
     * 表字段 : t_loan_contract_info.DEALER_NAME
     */
    @SerializedName("经销商名称")
    private String dealerName = StringUtils.EMPTY;

    /**
     * 经销商卖场地址
     * 表字段 : t_loan_contract_info.DEALER_SALE_ADDRESS
     */
    @SerializedName("经销商卖场地址")
    private String dealerSaleAddress = StringUtils.EMPTY;

    /**
     * 店面省份
     * 表字段 : t_loan_contract_info.STORE_PROVINCES
     */
    @SerializedName("店面省份")
    private String storeProvinces = StringUtils.EMPTY;

    /**
     * 店面城市
     * 表字段 : t_loan_contract_info.STORE_CITIES
     */
    @SerializedName("店面城市")
    private String storeCities = StringUtils.EMPTY;

    /**
     * 申请用途
     * 表字段 : t_loan_contract_info.LOAN_APPLICATION
     */
    @SerializedName("申请用途")
    private String loanApplication = StringUtils.EMPTY;

    /**
     * 合同期限(月)
     * 表字段 : t_loan_contract_info.CONTRACT_TERM
     */
    @SerializedName("合同期限(月)")
    private BigDecimal contractTerm = BigDecimal.ZERO;

    /**
     * 数据提取日
     * 表字段 : t_loan_contract_info.DATA_EXTRACTION_DAY
     */
    @SerializedName("数据提取日")
    private Date dataExtractionDay;

    /**
     * 提取日剩余期限(月)
     * 表字段 : t_loan_contract_info.REMAINING_LIFE_EXTRACTION
     */
    @SerializedName("提取日剩余期限(月)")
    private BigDecimal remainingLifeExtraction = BigDecimal.ZERO;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_loan_contract_info.PROJECT_ID
     *
     * @return t_loan_contract_info.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_loan_contract_info.PROJECT_ID
     *
     * @param projectId t_loan_contract_info.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_loan_contract_info.AGENCY_ID
     *
     * @return t_loan_contract_info.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_loan_contract_info.AGENCY_ID
     *
     * @param agencyId t_loan_contract_info.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_loan_contract_info.SERIAL_NUMBER
     *
     * @return t_loan_contract_info.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_loan_contract_info.SERIAL_NUMBER
     *
     * @param serialNumber t_loan_contract_info.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 资产类型-枚举
     * 预定义字段：
     * 个人消费贷款
     * 汽车抵押贷款
     * 住房抵押贷款
     * 字段:t_loan_contract_info.ASSET_TYPE
     *
     * @return t_loan_contract_info.ASSET_TYPE, 资产类型-枚举
     * 预定义字段：
     * 个人消费贷款
     * 汽车抵押贷款
     * 住房抵押贷款
     */
    public String getAssetType() {
        return assetType;
    }

    /**
     * 设置 资产类型-枚举
     * 预定义字段：
     * 个人消费贷款
     * 汽车抵押贷款
     * 住房抵押贷款
     * 字段:t_loan_contract_info.ASSET_TYPE
     *
     * @param assetType t_loan_contract_info.ASSET_TYPE, 资产类型-枚举
     *                  预定义字段：
     *                  个人消费贷款
     *                  汽车抵押贷款
     *                  住房抵押贷款
     */
    public void setAssetType(String assetType) {
        this.assetType = assetType == null ? null : assetType.trim();
    }

    /**
     * 获取 贷款合同编号 字段:t_loan_contract_info.CONTRACT_CODE
     *
     * @return t_loan_contract_info.CONTRACT_CODE, 贷款合同编号
     */
    public String getContractCode() {
        return contractCode;
    }

    /**
     * 设置 贷款合同编号 字段:t_loan_contract_info.CONTRACT_CODE
     *
     * @param contractCode t_loan_contract_info.CONTRACT_CODE, 贷款合同编号
     */
    public void setContractCode(String contractCode) {
        this.contractCode = contractCode == null ? null : contractCode.trim();
    }

    /**
     * 获取 产品编号 字段:t_loan_contract_info.PRODUCT_ID
     *
     * @return t_loan_contract_info.PRODUCT_ID, 产品编号
     */
    public String getProductId() {
        return productId;
    }

    /**
     * 设置 产品编号 字段:t_loan_contract_info.PRODUCT_ID
     *
     * @param productId t_loan_contract_info.PRODUCT_ID, 产品编号
     */
    public void setProductId(String productId) {
        this.productId = productId == null ? null : productId.trim();
    }

    /**
     * 获取 产品方案名称 字段:t_loan_contract_info.PRODUCT_SCHEME_NAME
     *
     * @return t_loan_contract_info.PRODUCT_SCHEME_NAME, 产品方案名称
     */
    public String getProductSchemeName() {
        return productSchemeName;
    }

    /**
     * 设置 产品方案名称 字段:t_loan_contract_info.PRODUCT_SCHEME_NAME
     *
     * @param productSchemeName t_loan_contract_info.PRODUCT_SCHEME_NAME, 产品方案名称
     */
    public void setProductSchemeName(String productSchemeName) {
        this.productSchemeName = productSchemeName == null ? null : productSchemeName.trim();
    }

    /**
     * 获取 贷款总金额(元) 字段:t_loan_contract_info.CONTRACT_AMOUNT
     *
     * @return t_loan_contract_info.CONTRACT_AMOUNT, 贷款总金额(元)
     */
    public BigDecimal getContractAmount() {
        return contractAmount;
    }

    /**
     * 设置 贷款总金额(元) 字段:t_loan_contract_info.CONTRACT_AMOUNT
     *
     * @param contractAmount t_loan_contract_info.CONTRACT_AMOUNT, 贷款总金额(元)
     */
    public void setContractAmount(BigDecimal contractAmount) {
        this.contractAmount = contractAmount;
    }

    /**
     * 获取 利率类型-枚举
     * 固定利率
     * 浮动利率
     * 字段:t_loan_contract_info.INTEREST_RATE_TYPE
     *
     * @return t_loan_contract_info.INTEREST_RATE_TYPE, 利率类型-枚举
     * 固定利率
     * 浮动利率
     */
    public String getInterestRateType() {
        return interestRateType;
    }

    /**
     * 设置 利率类型-枚举
     * 固定利率
     * 浮动利率
     * 字段:t_loan_contract_info.INTEREST_RATE_TYPE
     *
     * @param interestRateType t_loan_contract_info.INTEREST_RATE_TYPE, 利率类型-枚举
     *                         固定利率
     *                         浮动利率
     */
    public void setInterestRateType(String interestRateType) {
        this.interestRateType = interestRateType == null ? null : interestRateType.trim();
    }

    /**
     * 获取 贷款年利率(%) 字段:t_loan_contract_info.CONTRACT_INTEREST_RATE
     *
     * @return t_loan_contract_info.CONTRACT_INTEREST_RATE, 贷款年利率(%)
     */
    public BigDecimal getContractInterestRate() {
        return contractInterestRate;
    }

    /**
     * 设置 贷款年利率(%) 字段:t_loan_contract_info.CONTRACT_INTEREST_RATE
     *
     * @param contractInterestRate t_loan_contract_info.CONTRACT_INTEREST_RATE, 贷款年利率(%)
     */
    public void setContractInterestRate(BigDecimal contractInterestRate) {
        this.contractInterestRate = contractInterestRate;
    }

    /**
     * 获取 还款方式-枚举
     * 预定义字段：
     * 等额本息，
     * 等额本金，
     * 等本等息，
     * 先息后本，
     * 一次性还本付息
     * 气球贷
     * 自定义还款计划
     * 字段:t_loan_contract_info.REPAYMENT_TYPE
     *
     * @return t_loan_contract_info.REPAYMENT_TYPE, 还款方式-枚举
     * 预定义字段：
     * 等额本息，
     * 等额本金，
     * 等本等息，
     * 先息后本，
     * 一次性还本付息
     * 气球贷
     * 自定义还款计划
     */
    public String getRepaymentType() {
        return repaymentType;
    }

    /**
     * 设置 还款方式-枚举
     * 预定义字段：
     * 等额本息，
     * 等额本金，
     * 等本等息，
     * 先息后本，
     * 一次性还本付息
     * 气球贷
     * 自定义还款计划
     * 字段:t_loan_contract_info.REPAYMENT_TYPE
     *
     * @param repaymentType t_loan_contract_info.REPAYMENT_TYPE, 还款方式-枚举
     *                      预定义字段：
     *                      等额本息，
     *                      等额本金，
     *                      等本等息，
     *                      先息后本，
     *                      一次性还本付息
     *                      气球贷
     *                      自定义还款计划
     */
    public void setRepaymentType(String repaymentType) {
        this.repaymentType = repaymentType == null ? null : repaymentType.trim();
    }

    /**
     * 获取 还款频率-枚举
     * 预定义字段：
     * 月，
     * 季，
     * 半年，
     * 年，
     * 到期一次付清
     * 自定义还款频率
     * 字段:t_loan_contract_info.REPAYMENT_FREQUENCY
     *
     * @return t_loan_contract_info.REPAYMENT_FREQUENCY, 还款频率-枚举
     * 预定义字段：
     * 月，
     * 季，
     * 半年，
     * 年，
     * 到期一次付清
     * 自定义还款频率
     */
    public String getRepaymentFrequency() {
        return repaymentFrequency;
    }

    /**
     * 设置 还款频率-枚举
     * 预定义字段：
     * 月，
     * 季，
     * 半年，
     * 年，
     * 到期一次付清
     * 自定义还款频率
     * 字段:t_loan_contract_info.REPAYMENT_FREQUENCY
     *
     * @param repaymentFrequency t_loan_contract_info.REPAYMENT_FREQUENCY, 还款频率-枚举
     *                           预定义字段：
     *                           月，
     *                           季，
     *                           半年，
     *                           年，
     *                           到期一次付清
     *                           自定义还款频率
     */
    public void setRepaymentFrequency(String repaymentFrequency) {
        this.repaymentFrequency = repaymentFrequency == null ? null : repaymentFrequency.trim();
    }

    /**
     * 获取 总期数 字段:t_loan_contract_info.PERIODS
     *
     * @return t_loan_contract_info.PERIODS, 总期数
     */
    public Integer getPeriods() {
        return periods;
    }

    /**
     * 设置 总期数 字段:t_loan_contract_info.PERIODS
     *
     * @param periods t_loan_contract_info.PERIODS, 总期数
     */
    public void setPeriods(Integer periods) {
        this.periods = periods;
    }

    /**
     * 获取 总收益率(%) 字段:t_loan_contract_info.PROFIT_TOL_RATE
     *
     * @return t_loan_contract_info.PROFIT_TOL_RATE, 总收益率(%)
     */
    public BigDecimal getProfitTolRate() {
        return profitTolRate;
    }

    /**
     * 设置 总收益率(%) 字段:t_loan_contract_info.PROFIT_TOL_RATE
     *
     * @param profitTolRate t_loan_contract_info.PROFIT_TOL_RATE, 总收益率(%)
     */
    public void setProfitTolRate(BigDecimal profitTolRate) {
        this.profitTolRate = profitTolRate;
    }

    /**
     * 获取 剩余本金(元) 字段:t_loan_contract_info.REST_PRINCIPAL
     *
     * @return t_loan_contract_info.REST_PRINCIPAL, 剩余本金(元)
     */
    public BigDecimal getRestPrincipal() {
        return restPrincipal;
    }

    /**
     * 设置 剩余本金(元) 字段:t_loan_contract_info.REST_PRINCIPAL
     *
     * @param restPrincipal t_loan_contract_info.REST_PRINCIPAL, 剩余本金(元)
     */
    public void setRestPrincipal(BigDecimal restPrincipal) {
        this.restPrincipal = restPrincipal;
    }

    /**
     * 获取 剩余费用其他(元) 字段:t_loan_contract_info.REST_OTHER_COST
     *
     * @return t_loan_contract_info.REST_OTHER_COST, 剩余费用其他(元)
     */
    public BigDecimal getRestOtherCost() {
        return restOtherCost;
    }

    /**
     * 设置 剩余费用其他(元) 字段:t_loan_contract_info.REST_OTHER_COST
     *
     * @param restOtherCost t_loan_contract_info.REST_OTHER_COST, 剩余费用其他(元)
     */
    public void setRestOtherCost(BigDecimal restOtherCost) {
        this.restOtherCost = restOtherCost;
    }

    /**
     * 获取 剩余利息(元) 字段:t_loan_contract_info.REST_INTEREST
     *
     * @return t_loan_contract_info.REST_INTEREST, 剩余利息(元)
     */
    public BigDecimal getRestInterest() {
        return restInterest;
    }

    /**
     * 设置 剩余利息(元) 字段:t_loan_contract_info.REST_INTEREST
     *
     * @param restInterest t_loan_contract_info.REST_INTEREST, 剩余利息(元)
     */
    public void setRestInterest(BigDecimal restInterest) {
        this.restInterest = restInterest;
    }

    /**
     * 获取 首付款金额(元) 字段:t_loan_contract_info.SHOUFU_AMOUNT
     *
     * @return t_loan_contract_info.SHOUFU_AMOUNT, 首付款金额(元)
     */
    public BigDecimal getShoufuAmount() {
        return shoufuAmount;
    }

    /**
     * 设置 首付款金额(元) 字段:t_loan_contract_info.SHOUFU_AMOUNT
     *
     * @param shoufuAmount t_loan_contract_info.SHOUFU_AMOUNT, 首付款金额(元)
     */
    public void setShoufuAmount(BigDecimal shoufuAmount) {
        this.shoufuAmount = shoufuAmount;
    }

    /**
     * 获取 首付比例(%) 字段:t_loan_contract_info.SHOUFU_PROPORTION
     *
     * @return t_loan_contract_info.SHOUFU_PROPORTION, 首付比例(%)
     */
    public BigDecimal getShoufuProportion() {
        return shoufuProportion;
    }

    /**
     * 设置 首付比例(%) 字段:t_loan_contract_info.SHOUFU_PROPORTION
     *
     * @param shoufuProportion t_loan_contract_info.SHOUFU_PROPORTION, 首付比例(%)
     */
    public void setShoufuProportion(BigDecimal shoufuProportion) {
        this.shoufuProportion = shoufuProportion;
    }

    /**
     * 获取 尾付款金额(元 字段:t_loan_contract_info.TAIL_AMOUNT
     *
     * @return t_loan_contract_info.TAIL_AMOUNT, 尾付款金额(元
     */
    public BigDecimal getTailAmount() {
        return tailAmount;
    }

    /**
     * 设置 尾付款金额(元 字段:t_loan_contract_info.TAIL_AMOUNT
     *
     * @param tailAmount t_loan_contract_info.TAIL_AMOUNT, 尾付款金额(元
     */
    public void setTailAmount(BigDecimal tailAmount) {
        this.tailAmount = tailAmount;
    }

    /**
     * 获取 尾付比例(%) 字段:t_loan_contract_info.TAIL_AMOUNT_RATE
     *
     * @return t_loan_contract_info.TAIL_AMOUNT_RATE, 尾付比例(%)
     */
    public BigDecimal getTailAmountRate() {
        return tailAmountRate;
    }

    /**
     * 设置 尾付比例(%) 字段:t_loan_contract_info.TAIL_AMOUNT_RATE
     *
     * @param tailAmountRate t_loan_contract_info.TAIL_AMOUNT_RATE, 尾付比例(%)
     */
    public void setTailAmountRate(BigDecimal tailAmountRate) {
        this.tailAmountRate = tailAmountRate;
    }

    /**
     * 获取 手续费 字段:t_loan_contract_info.POUNDAGE
     *
     * @return t_loan_contract_info.POUNDAGE, 手续费
     */
    public BigDecimal getPoundage() {
        return poundage;
    }

    /**
     * 设置 手续费 字段:t_loan_contract_info.POUNDAGE
     *
     * @param poundage t_loan_contract_info.POUNDAGE, 手续费
     */
    public void setPoundage(BigDecimal poundage) {
        this.poundage = poundage;
    }

    /**
     * 获取 手续费利率 字段:t_loan_contract_info.POUNDAGE_RATE
     *
     * @return t_loan_contract_info.POUNDAGE_RATE, 手续费利率
     */
    public BigDecimal getPoundageRate() {
        return poundageRate;
    }

    /**
     * 设置 手续费利率 字段:t_loan_contract_info.POUNDAGE_RATE
     *
     * @param poundageRate t_loan_contract_info.POUNDAGE_RATE, 手续费利率
     */
    public void setPoundageRate(BigDecimal poundageRate) {
        this.poundageRate = poundageRate;
    }

    /**
     * 获取 手续费扣款方式-枚举
     * 预定义字段：
     * 一次性
     * 分期扣款
     * 字段:t_loan_contract_info.POUNDAGE_DEDUCTION_TYPE
     *
     * @return t_loan_contract_info.POUNDAGE_DEDUCTION_TYPE, 手续费扣款方式-枚举
     * 预定义字段：
     * 一次性
     * 分期扣款
     */
    public String getPoundageDeductionType() {
        return poundageDeductionType;
    }

    /**
     * 设置 手续费扣款方式-枚举
     * 预定义字段：
     * 一次性
     * 分期扣款
     * 字段:t_loan_contract_info.POUNDAGE_DEDUCTION_TYPE
     *
     * @param poundageDeductionType t_loan_contract_info.POUNDAGE_DEDUCTION_TYPE, 手续费扣款方式-枚举
     *                              预定义字段：
     *                              一次性
     *                              分期扣款
     */
    public void setPoundageDeductionType(String poundageDeductionType) {
        this.poundageDeductionType = poundageDeductionType == null ? null : poundageDeductionType.trim();
    }

    /**
     * 获取 结算手续费率 字段:t_loan_contract_info.SETTLEMENT_POUNDAGE_RATE
     *
     * @return t_loan_contract_info.SETTLEMENT_POUNDAGE_RATE, 结算手续费率
     */
    public BigDecimal getSettlementPoundageRate() {
        return settlementPoundageRate;
    }

    /**
     * 设置 结算手续费率 字段:t_loan_contract_info.SETTLEMENT_POUNDAGE_RATE
     *
     * @param settlementPoundageRate t_loan_contract_info.SETTLEMENT_POUNDAGE_RATE, 结算手续费率
     */
    public void setSettlementPoundageRate(BigDecimal settlementPoundageRate) {
        this.settlementPoundageRate = settlementPoundageRate;
    }

    /**
     * 获取 补贴手续费 字段:t_loan_contract_info.SUBSIDIE_POUNDAGE
     *
     * @return t_loan_contract_info.SUBSIDIE_POUNDAGE, 补贴手续费
     */
    public BigDecimal getSubsidiePoundage() {
        return subsidiePoundage;
    }

    /**
     * 设置 补贴手续费 字段:t_loan_contract_info.SUBSIDIE_POUNDAGE
     *
     * @param subsidiePoundage t_loan_contract_info.SUBSIDIE_POUNDAGE, 补贴手续费
     */
    public void setSubsidiePoundage(BigDecimal subsidiePoundage) {
        this.subsidiePoundage = subsidiePoundage;
    }

    /**
     * 获取 贴息金额 字段:t_loan_contract_info.DISCOUNT_AMOUNT
     *
     * @return t_loan_contract_info.DISCOUNT_AMOUNT, 贴息金额
     */
    public BigDecimal getDiscountAmount() {
        return discountAmount;
    }

    /**
     * 设置 贴息金额 字段:t_loan_contract_info.DISCOUNT_AMOUNT
     *
     * @param discountAmount t_loan_contract_info.DISCOUNT_AMOUNT, 贴息金额
     */
    public void setDiscountAmount(BigDecimal discountAmount) {
        this.discountAmount = discountAmount;
    }

    /**
     * 获取 保证金比例 字段:t_loan_contract_info.MARGIN_RATE
     *
     * @return t_loan_contract_info.MARGIN_RATE, 保证金比例
     */
    public BigDecimal getMarginRate() {
        return marginRate;
    }

    /**
     * 设置 保证金比例 字段:t_loan_contract_info.MARGIN_RATE
     *
     * @param marginRate t_loan_contract_info.MARGIN_RATE, 保证金比例
     */
    public void setMarginRate(BigDecimal marginRate) {
        this.marginRate = marginRate;
    }

    /**
     * 获取 保证金 字段:t_loan_contract_info.MARGIN
     *
     * @return t_loan_contract_info.MARGIN, 保证金
     */
    public BigDecimal getMargin() {
        return margin;
    }

    /**
     * 设置 保证金 字段:t_loan_contract_info.MARGIN
     *
     * @param margin t_loan_contract_info.MARGIN, 保证金
     */
    public void setMargin(BigDecimal margin) {
        this.margin = margin;
    }

    /**
     * 获取 保证金冲抵方式-枚举
     * 预定义字段：
     * 不冲抵
     * 冲抵
     * 字段:t_loan_contract_info.MARGIN_OFFSET_TYPE
     *
     * @return t_loan_contract_info.MARGIN_OFFSET_TYPE, 保证金冲抵方式-枚举
     * 预定义字段：
     * 不冲抵
     * 冲抵
     */
    public String getMarginOffsetType() {
        return marginOffsetType;
    }

    /**
     * 设置 保证金冲抵方式-枚举
     * 预定义字段：
     * 不冲抵
     * 冲抵
     * 字段:t_loan_contract_info.MARGIN_OFFSET_TYPE
     *
     * @param marginOffsetType t_loan_contract_info.MARGIN_OFFSET_TYPE, 保证金冲抵方式-枚举
     *                         预定义字段：
     *                         不冲抵
     *                         冲抵
     */
    public void setMarginOffsetType(String marginOffsetType) {
        this.marginOffsetType = marginOffsetType == null ? null : marginOffsetType.trim();
    }

    /**
     * 获取 帐户管理费 字段:t_loan_contract_info.ACCOUNT_MANAGEMENT_EXPENSE
     *
     * @return t_loan_contract_info.ACCOUNT_MANAGEMENT_EXPENSE, 帐户管理费
     */
    public BigDecimal getAccountManagementExpense() {
        return accountManagementExpense;
    }

    /**
     * 设置 帐户管理费 字段:t_loan_contract_info.ACCOUNT_MANAGEMENT_EXPENSE
     *
     * @param accountManagementExpense t_loan_contract_info.ACCOUNT_MANAGEMENT_EXPENSE, 帐户管理费
     */
    public void setAccountManagementExpense(BigDecimal accountManagementExpense) {
        this.accountManagementExpense = accountManagementExpense;
    }

    /**
     * 获取 抵押率(%) 字段:t_loan_contract_info.MORTGAGE_RATE
     *
     * @return t_loan_contract_info.MORTGAGE_RATE, 抵押率(%)
     */
    public BigDecimal getMortgageRate() {
        return mortgageRate;
    }

    /**
     * 设置 抵押率(%) 字段:t_loan_contract_info.MORTGAGE_RATE
     *
     * @param mortgageRate t_loan_contract_info.MORTGAGE_RATE, 抵押率(%)
     */
    public void setMortgageRate(BigDecimal mortgageRate) {
        this.mortgageRate = mortgageRate;
    }

    /**
     * 获取 合同开始时间 字段:t_loan_contract_info.LOAN_ISSUE_DATE
     *
     * @return t_loan_contract_info.LOAN_ISSUE_DATE, 合同开始时间
     */
    public Date getLoanIssueDate() {
        return loanIssueDate;
    }

    /**
     * 设置 合同开始时间 字段:t_loan_contract_info.LOAN_ISSUE_DATE
     *
     * @param loanIssueDate t_loan_contract_info.LOAN_ISSUE_DATE, 合同开始时间
     */
    public void setLoanIssueDate(Date loanIssueDate) {
        this.loanIssueDate = loanIssueDate == null ? null : loanIssueDate;
    }

    /**
     * 获取 合同结束时间 字段:t_loan_contract_info.LOAN_EXPIRY_DATE
     *
     * @return t_loan_contract_info.LOAN_EXPIRY_DATE, 合同结束时间
     */
    public Date getLoanExpiryDate() {
        return loanExpiryDate;
    }

    /**
     * 设置 合同结束时间 字段:t_loan_contract_info.LOAN_EXPIRY_DATE
     *
     * @param loanExpiryDate t_loan_contract_info.LOAN_EXPIRY_DATE, 合同结束时间
     */
    public void setLoanExpiryDate(Date loanExpiryDate) {
        this.loanExpiryDate = loanExpiryDate == null ? null : loanExpiryDate;
    }

    /**
     * 获取 实际放款时间 字段:t_loan_contract_info.ACTUAL_LOAN_DATE
     *
     * @return t_loan_contract_info.ACTUAL_LOAN_DATE, 实际放款时间
     */
    public Date getActualLoanDate() {
        return actualLoanDate;
    }

    /**
     * 设置 实际放款时间 字段:t_loan_contract_info.ACTUAL_LOAN_DATE
     *
     * @param actualLoanDate t_loan_contract_info.ACTUAL_LOAN_DATE, 实际放款时间
     */
    public void setActualLoanDate(Date actualLoanDate) {
        this.actualLoanDate = actualLoanDate == null ? null : actualLoanDate;
    }

    /**
     * 获取 首次还款时间 字段:t_loan_contract_info.FRIST_REPAYMENT_DATE
     *
     * @return t_loan_contract_info.FRIST_REPAYMENT_DATE, 首次还款时间
     */
    public Date getFristRepaymentDate() {
        return fristRepaymentDate;
    }

    /**
     * 设置 首次还款时间 字段:t_loan_contract_info.FRIST_REPAYMENT_DATE
     *
     * @param fristRepaymentDate t_loan_contract_info.FRIST_REPAYMENT_DATE, 首次还款时间
     */
    public void setFristRepaymentDate(Date fristRepaymentDate) {
        this.fristRepaymentDate = fristRepaymentDate == null ? null : fristRepaymentDate;
    }

    /**
     * 获取 贷款还款日 字段:t_loan_contract_info.LOAN_REPAY_DATE
     *
     * @return t_loan_contract_info.LOAN_REPAY_DATE, 贷款还款日
     */
    public String getLoanRepayDate() {
        return loanRepayDate;
    }

    /**
     * 设置 贷款还款日 字段:t_loan_contract_info.LOAN_REPAY_DATE
     *
     * @param loanRepayDate t_loan_contract_info.LOAN_REPAY_DATE, 贷款还款日
     */
    public void setLoanRepayDate(String loanRepayDate) {
        this.loanRepayDate = loanRepayDate;
    }

    /**
     * 获取 最后一次预计扣款时间 字段:t_loan_contract_info.LAST_ESTIMATED_DEDUCTION_DATE
     *
     * @return t_loan_contract_info.LAST_ESTIMATED_DEDUCTION_DATE, 最后一次预计扣款时间
     */
    public Date getLastEstimatedDeductionDate() {
        return lastEstimatedDeductionDate;
    }

    /**
     * 设置 最后一次预计扣款时间 字段:t_loan_contract_info.LAST_ESTIMATED_DEDUCTION_DATE
     *
     * @param lastEstimatedDeductionDate t_loan_contract_info.LAST_ESTIMATED_DEDUCTION_DATE, 最后一次预计扣款时间
     */
    public void setLastEstimatedDeductionDate(Date lastEstimatedDeductionDate) {
        this.lastEstimatedDeductionDate = lastEstimatedDeductionDate == null ? null : lastEstimatedDeductionDate;
    }

    /**
     * 获取 月还款额 字段:t_loan_contract_info.MONTH_REPAY_AMOUNT
     *
     * @return t_loan_contract_info.MONTH_REPAY_AMOUNT, 月还款额
     */
    public BigDecimal getMonthRepayAmount() {
        return monthRepayAmount;
    }

    /**
     * 设置 月还款额 字段:t_loan_contract_info.MONTH_REPAY_AMOUNT
     *
     * @param monthRepayAmount t_loan_contract_info.MONTH_REPAY_AMOUNT, 月还款额
     */
    public void setMonthRepayAmount(BigDecimal monthRepayAmount) {
        this.monthRepayAmount = monthRepayAmount;
    }

    /**
     * 获取 贷款用途-枚举
     * 预定义字段：
     * 购房，
     * 购车，
     * 车抵押,
     * 装修，
     * 旅游，
     * 3C，
     * 教育，
     * 医美，
     * 经营类
     * 其他
     * 农业，
     * 个人综合消费
     * 字段:t_loan_contract_info.LOAN_USE
     *
     * @return t_loan_contract_info.LOAN_USE, 贷款用途-枚举
     * 预定义字段：
     * 购房，
     * 购车，
     * 车抵押,
     * 装修，
     * 旅游，
     * 3C，
     * 教育，
     * 医美，
     * 经营类
     * 其他
     * 农业，
     * 个人综合消费
     */
    public String getLoanUse() {
        return loanUse;
    }

    /**
     * 设置 贷款用途-枚举
     * 预定义字段：
     * 购房，
     * 购车，
     * 车抵押,
     * 装修，
     * 旅游，
     * 3C，
     * 教育，
     * 医美，
     * 经营类
     * 其他
     * 农业，
     * 个人综合消费
     * 字段:t_loan_contract_info.LOAN_USE
     *
     * @param loanUse t_loan_contract_info.LOAN_USE, 贷款用途-枚举
     *                预定义字段：
     *                购房，
     *                购车，
     *                车抵押,
     *                装修，
     *                旅游，
     *                3C，
     *                教育，
     *                医美，
     *                经营类
     *                其他
     *                农业，
     *                个人综合消费
     */
    public void setLoanUse(String loanUse) {
        this.loanUse = loanUse == null ? null : loanUse.trim();
    }

    /**
     * 获取 担保方式-枚举
     * 预定义字段：
     * 质押担保，
     * 信用担保，
     * 保证担保，
     * 抵押担保
     * 字段:t_loan_contract_info.GUARANTEE_TYPE
     *
     * @return t_loan_contract_info.GUARANTEE_TYPE, 担保方式-枚举
     * 预定义字段：
     * 质押担保，
     * 信用担保，
     * 保证担保，
     * 抵押担保
     */
    public String getGuaranteeType() {
        return guaranteeType;
    }

    /**
     * 设置 担保方式-枚举
     * 预定义字段：
     * 质押担保，
     * 信用担保，
     * 保证担保，
     * 抵押担保
     * 字段:t_loan_contract_info.GUARANTEE_TYPE
     *
     * @param guaranteeType t_loan_contract_info.GUARANTEE_TYPE, 担保方式-枚举
     *                      预定义字段：
     *                      质押担保，
     *                      信用担保，
     *                      保证担保，
     *                      抵押担保
     */
    public void setGuaranteeType(String guaranteeType) {
        this.guaranteeType = guaranteeType == null ? null : guaranteeType.trim();
    }

    /**
     * 获取 累计逾期期数 字段:t_loan_contract_info.TOTAL_OVERDUE_DAYNUM
     *
     * @return t_loan_contract_info.TOTAL_OVERDUE_DAYNUM, 累计逾期期数
     */
    public Integer getTotalOverdueDaynum() {
        return totalOverdueDaynum;
    }

    /**
     * 设置 累计逾期期数 字段:t_loan_contract_info.TOTAL_OVERDUE_DAYNUM
     *
     * @param totalOverdueDaynum t_loan_contract_info.TOTAL_OVERDUE_DAYNUM, 累计逾期期数
     */
    public void setTotalOverdueDaynum(Integer totalOverdueDaynum) {
        this.totalOverdueDaynum = totalOverdueDaynum;
    }

    /**
     * 获取 历史最高逾期天数 字段:t_loan_contract_info.HISTORY_MOST_OVERDUE_DAYNUM
     *
     * @return t_loan_contract_info.HISTORY_MOST_OVERDUE_DAYNUM, 历史最高逾期天数
     */
    public Integer getHistoryMostOverdueDaynum() {
        return historyMostOverdueDaynum;
    }

    /**
     * 设置 历史最高逾期天数 字段:t_loan_contract_info.HISTORY_MOST_OVERDUE_DAYNUM
     *
     * @param historyMostOverdueDaynum t_loan_contract_info.HISTORY_MOST_OVERDUE_DAYNUM, 历史最高逾期天数
     */
    public void setHistoryMostOverdueDaynum(Integer historyMostOverdueDaynum) {
        this.historyMostOverdueDaynum = historyMostOverdueDaynum;
    }

    /**
     * 获取 历史累计逾期天数 字段:t_loan_contract_info.HISTORY_TOTAL_OVERDUE_DAYNUM
     *
     * @return t_loan_contract_info.HISTORY_TOTAL_OVERDUE_DAYNUM, 历史累计逾期天数
     */
    public Integer getHistoryTotalOverdueDaynum() {
        return historyTotalOverdueDaynum;
    }

    /**
     * 设置 历史累计逾期天数 字段:t_loan_contract_info.HISTORY_TOTAL_OVERDUE_DAYNUM
     *
     * @param historyTotalOverdueDaynum t_loan_contract_info.HISTORY_TOTAL_OVERDUE_DAYNUM, 历史累计逾期天数
     */
    public void setHistoryTotalOverdueDaynum(Integer historyTotalOverdueDaynum) {
        this.historyTotalOverdueDaynum = historyTotalOverdueDaynum;
    }

    /**
     * 获取 当前逾期天数 字段:t_loan_contract_info.CURRENT_OVERDUE_DAYNUM
     *
     * @return t_loan_contract_info.CURRENT_OVERDUE_DAYNUM, 当前逾期天数
     */
    public Integer getCurrentOverdueDaynum() {
        return currentOverdueDaynum;
    }

    /**
     * 设置 当前逾期天数 字段:t_loan_contract_info.CURRENT_OVERDUE_DAYNUM
     *
     * @param currentOverdueDaynum t_loan_contract_info.CURRENT_OVERDUE_DAYNUM, 当前逾期天数
     */
    public void setCurrentOverdueDaynum(Integer currentOverdueDaynum) {
        this.currentOverdueDaynum = currentOverdueDaynum;
    }

    /**
     * 获取 合同状态-枚举
     * 预定义字段：
     * 生效
     * 不生效
     * 字段:t_loan_contract_info.CONTRACT_STATUS
     *
     * @return t_loan_contract_info.CONTRACT_STATUS, 合同状态-枚举
     * 预定义字段：
     * 生效
     * 不生效
     */
    public String getContractStatus() {
        return contractStatus;
    }

    /**
     * 设置 合同状态-枚举
     * 预定义字段：
     * 生效
     * 不生效
     * 字段:t_loan_contract_info.CONTRACT_STATUS
     *
     * @param contractStatus t_loan_contract_info.CONTRACT_STATUS, 合同状态-枚举
     *                       预定义字段：
     *                       生效
     *                       不生效
     */
    public void setContractStatus(String contractStatus) {
        this.contractStatus = contractStatus == null ? null : contractStatus.trim();
    }

    /**
     * 获取 申请状态代码-枚举
     * "预定义字段：
     * 已放款
     * 放款失败"
     * 字段:t_loan_contract_info.APPLY_STATUS_CODE
     *
     * @return t_loan_contract_info.APPLY_STATUS_CODE, 申请状态代码-枚举
     * "预定义字段：
     * 已放款
     * 放款失败"
     */
    public String getApplyStatusCode() {
        return applyStatusCode;
    }

    /**
     * 设置 申请状态代码-枚举
     * "预定义字段：
     * 已放款
     * 放款失败"
     * 字段:t_loan_contract_info.APPLY_STATUS_CODE
     *
     * @param applyStatusCode t_loan_contract_info.APPLY_STATUS_CODE, 申请状态代码-枚举
     *                        "预定义字段：
     *                        已放款
     *                        放款失败"
     */
    public void setApplyStatusCode(String applyStatusCode) {
        this.applyStatusCode = applyStatusCode == null ? null : applyStatusCode.trim();
    }

    /**
     * 获取 申请渠道 字段:t_loan_contract_info.APPLY_CHANNEL
     *
     * @return t_loan_contract_info.APPLY_CHANNEL, 申请渠道
     */
    public String getApplyChannel() {
        return applyChannel;
    }

    /**
     * 设置 申请渠道 字段:t_loan_contract_info.APPLY_CHANNEL
     *
     * @param applyChannel t_loan_contract_info.APPLY_CHANNEL, 申请渠道
     */
    public void setApplyChannel(String applyChannel) {
        this.applyChannel = applyChannel == null ? null : applyChannel.trim();
    }

    /**
     * 获取 申请地点 字段:t_loan_contract_info.APPLY_PLACE
     *
     * @return t_loan_contract_info.APPLY_PLACE, 申请地点
     */
    public String getApplyPlace() {
        return applyPlace;
    }

    /**
     * 设置 申请地点 字段:t_loan_contract_info.APPLY_PLACE
     *
     * @param applyPlace t_loan_contract_info.APPLY_PLACE, 申请地点
     */
    public void setApplyPlace(String applyPlace) {
        this.applyPlace = applyPlace == null ? null : applyPlace.trim();
    }

    /**
     * 获取 借款人状态-
     * "1-首次申请
     * 2-内部续贷
     * 3-其他机构转单
     * 9-其他"
     * 字段:t_loan_contract_info.BORROWERP_STATUS
     *
     * @return t_loan_contract_info.BORROWERP_STATUS, 借款人状态-
     * "1-首次申请
     * 2-内部续贷
     * 3-其他机构转单
     * 9-其他"
     */
    public String getBorrowerpStatus() {
        return borrowerpStatus;
    }

    /**
     * 设置 借款人状态-
     * "1-首次申请
     * 2-内部续贷
     * 3-其他机构转单
     * 9-其他"
     * 字段:t_loan_contract_info.BORROWERP_STATUS
     *
     * @param borrowerpStatus t_loan_contract_info.BORROWERP_STATUS, 借款人状态-
     *                        "1-首次申请
     *                        2-内部续贷
     *                        3-其他机构转单
     *                        9-其他"
     */
    public void setBorrowerpStatus(String borrowerpStatus) {
        this.borrowerpStatus = borrowerpStatus == null ? null : borrowerpStatus.trim();
    }

    /**
     * 获取 经销商名称 字段:t_loan_contract_info.DEALER_NAME
     *
     * @return t_loan_contract_info.DEALER_NAME, 经销商名称
     */
    public String getDealerName() {
        return dealerName;
    }

    /**
     * 设置 经销商名称 字段:t_loan_contract_info.DEALER_NAME
     *
     * @param dealerName t_loan_contract_info.DEALER_NAME, 经销商名称
     */
    public void setDealerName(String dealerName) {
        this.dealerName = dealerName == null ? null : dealerName.trim();
    }

    /**
     * 获取 经销商卖场地址 字段:t_loan_contract_info.DEALER_SALE_ADDRESS
     *
     * @return t_loan_contract_info.DEALER_SALE_ADDRESS, 经销商卖场地址
     */
    public String getDealerSaleAddress() {
        return dealerSaleAddress;
    }

    /**
     * 设置 经销商卖场地址 字段:t_loan_contract_info.DEALER_SALE_ADDRESS
     *
     * @param dealerSaleAddress t_loan_contract_info.DEALER_SALE_ADDRESS, 经销商卖场地址
     */
    public void setDealerSaleAddress(String dealerSaleAddress) {
        this.dealerSaleAddress = dealerSaleAddress == null ? null : dealerSaleAddress.trim();
    }

    /**
     * 获取 店面省份 字段:t_loan_contract_info.STORE_PROVINCES
     *
     * @return t_loan_contract_info.STORE_PROVINCES, 店面省份
     */
    public String getStoreProvinces() {
        return storeProvinces;
    }

    /**
     * 设置 店面省份 字段:t_loan_contract_info.STORE_PROVINCES
     *
     * @param storeProvinces t_loan_contract_info.STORE_PROVINCES, 店面省份
     */
    public void setStoreProvinces(String storeProvinces) {
        this.storeProvinces = storeProvinces == null ? null : storeProvinces.trim();
    }

    /**
     * 获取 店面城市 字段:t_loan_contract_info.STORE_CITIES
     *
     * @return t_loan_contract_info.STORE_CITIES, 店面城市
     */
    public String getStoreCities() {
        return storeCities;
    }

    /**
     * 设置 店面城市 字段:t_loan_contract_info.STORE_CITIES
     *
     * @param storeCities t_loan_contract_info.STORE_CITIES, 店面城市
     */
    public void setStoreCities(String storeCities) {
        this.storeCities = storeCities == null ? null : storeCities.trim();
    }

    /**
     * 获取 数据提取日 字段:t_loan_contract_info.DATA_EXTRACTION_DAY
     *
     * @return t_loan_contract_info.DATA_EXTRACTION_DAY, 数据提取日
     */
    public Date getDataExtractionDay() {
        return dataExtractionDay;
    }

    /**
     * 设置 数据提取日 字段:t_loan_contract_info.DATA_EXTRACTION_DAY
     *
     * @param dataExtractionDay t_loan_contract_info.DATA_EXTRACTION_DAY, 数据提取日
     */
    public void setDataExtractionDay(Date dataExtractionDay) {
        this.dataExtractionDay = dataExtractionDay == null ? null : dataExtractionDay;
    }

    public BigDecimal getContractDailyRate() {
        return contractDailyRate;
    }

    public void setContractDailyRate(BigDecimal contractDailyRate) {
        this.contractDailyRate = contractDailyRate;
    }

    public BigDecimal getContractAnRate() {
        return contractAnRate;
    }

    public void setContractAnRate(BigDecimal contractAnRate) {
        this.contractAnRate = contractAnRate;
    }

    public String getDailyCalculationBasis() {
        return dailyCalculationBasis;
    }

    public void setDailyCalculationBasis(String dailyCalculationBasis) {
        this.dailyCalculationBasis = dailyCalculationBasis;
    }

    public String getLoanApplication() {
        return loanApplication;
    }

    public void setLoanApplication(String loanApplication) {
        this.loanApplication = loanApplication;
    }

    public BigDecimal getContractTerm() {
        return contractTerm;
    }

    public void setContractTerm(BigDecimal contractTerm) {
        this.contractTerm = contractTerm;
    }

    public BigDecimal getRemainingLifeExtraction() {
        return remainingLifeExtraction;
    }

    public void setRemainingLifeExtraction(BigDecimal remainingLifeExtraction) {
        this.remainingLifeExtraction = remainingLifeExtraction;
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
        return "LoanContractInfo{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", assetType='" + assetType + '\'' +
                ", contractCode='" + contractCode + '\'' +
                ", productId='" + productId + '\'' +
                ", productSchemeName='" + productSchemeName + '\'' +
                ", contractAmount=" + contractAmount +
                ", interestRateType='" + interestRateType + '\'' +
                ", contractInterestRate=" + contractInterestRate +
                ", contractDailyRate=" + contractDailyRate +
                ", contractAnRate=" + contractAnRate +
                ", dailyCalculationBasis='" + dailyCalculationBasis + '\'' +
                ", repaymentType='" + repaymentType + '\'' +
                ", repaymentFrequency='" + repaymentFrequency + '\'' +
                ", periods=" + periods +
                ", profitTolRate=" + profitTolRate +
                ", restPrincipal=" + restPrincipal +
                ", restOtherCost=" + restOtherCost +
                ", restInterest=" + restInterest +
                ", shoufuAmount=" + shoufuAmount +
                ", shoufuProportion=" + shoufuProportion +
                ", tailAmount=" + tailAmount +
                ", tailAmountRate=" + tailAmountRate +
                ", poundage=" + poundage +
                ", poundageRate=" + poundageRate +
                ", poundageDeductionType='" + poundageDeductionType + '\'' +
                ", settlementPoundageRate=" + settlementPoundageRate +
                ", subsidiePoundage=" + subsidiePoundage +
                ", discountAmount=" + discountAmount +
                ", marginRate=" + marginRate +
                ", margin=" + margin +
                ", marginOffsetType='" + marginOffsetType + '\'' +
                ", accountManagementExpense=" + accountManagementExpense +
                ", mortgageRate=" + mortgageRate +
                ", loanIssueDate=" + loanIssueDate +
                ", loanExpiryDate=" + loanExpiryDate +
                ", actualLoanDate=" + actualLoanDate +
                ", fristRepaymentDate=" + fristRepaymentDate +
                ", loanRepayDate='" + loanRepayDate + '\'' +
                ", lastEstimatedDeductionDate=" + lastEstimatedDeductionDate +
                ", monthRepayAmount=" + monthRepayAmount +
                ", loanUse='" + loanUse + '\'' +
                ", guaranteeType='" + guaranteeType + '\'' +
                ", totalOverdueDaynum=" + totalOverdueDaynum +
                ", historyMostOverdueDaynum=" + historyMostOverdueDaynum +
                ", historyTotalOverdueDaynum=" + historyTotalOverdueDaynum +
                ", currentOverdueDaynum=" + currentOverdueDaynum +
                ", contractStatus='" + contractStatus + '\'' +
                ", applyStatusCode='" + applyStatusCode + '\'' +
                ", applyChannel='" + applyChannel + '\'' +
                ", applyPlace='" + applyPlace + '\'' +
                ", borrowerpStatus='" + borrowerpStatus + '\'' +
                ", dealerName='" + dealerName + '\'' +
                ", dealerSaleAddress='" + dealerSaleAddress + '\'' +
                ", storeProvinces='" + storeProvinces + '\'' +
                ", storeCities='" + storeCities + '\'' +
                ", loanApplication='" + loanApplication + '\'' +
                ", contractTerm=" + contractTerm +
                ", dataExtractionDay=" + dataExtractionDay +
                ", remainingLifeExtraction=" + remainingLifeExtraction +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}