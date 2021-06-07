package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.common.enums.asset.AssetCheckEnum;
import com.weshare.abscore.common.enums.asset.AssetCheckWhyEnum;
import com.weshare.abscore.common.excel.annotation.ExcelEntity;
import com.weshare.abscore.common.excel.annotation.ExcelProperty;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 资产对账信息 File10
 **/
@Data
@ExcelEntity
public class AssetAccountCheck extends BaseEntity {

    /**
     * excel中的行数
     **/
    private int location;

    /**
     * 合同编号
     **/
    @ExcelProperty(index = 1, value = "合同编号")
    private String contractCode;

    /**
     * 项目编号
     * 表字段 : t_asset_account_check.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_asset_account_check.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_asset_account_check.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    @ExcelProperty(index = 0, value = "借据号")
    private String serialNumber;

    /**
     * 是否变更还款计划（YES - 是，
     * NO - 否
     * ）
     * 表字段 : t_asset_account_check.IS_CHANGE_REPAY_SCHEDULE
     */
    @SerializedName("是否变更还款计划")
    private String isChangeRepaySchedule;

    /**
     * 变更时间
     * 表字段 : t_asset_account_check.CHANGE_TIME
     */
    @SerializedName("变更时间")
    private Date changeTime;

    /**
     * 贷款总金额
     * 表字段 : t_asset_account_check.TOTAL_LOAN_AMOUNT
     */
    @SerializedName("贷款总金额(元)")
    private BigDecimal totalLoanAmount;

    /**
     * 贷款年利率
     * 表字段 : t_asset_account_check.YEAR_LOAN_RATE
     */
    @SerializedName("贷款年利率(%)")
    private String yearLoanRate;

    /**
     * 总期数
     * 表字段 : t_asset_account_check.TOTAL_TERM
     */
    @SerializedName("总期数")
    private Integer totalTerm;

    /**
     * 已还期数
     * 表字段 : t_asset_account_check.ALREADY_REPAY_TERM
     */
    @SerializedName("已还期数")
    @ExcelProperty(index = 7, value = "已还期数", required = true, canBeNull = true, checkZeroPositiveInteger = true)
    private Integer alreadyRepayTerm;

    /**
     * 剩余期数
     * 表字段 : t_asset_account_check.REMAIN_TERM
     */
    @SerializedName("剩余期数")
    @ExcelProperty(index = 8, value = "剩余期数", required = true, canBeNull = true, checkZeroPositiveInteger = true)
    private Integer remainTerm;

    /**
     * 剩余本金
     * 表字段 : t_asset_account_check.REMAIN_PRINCIPAL
     */
    @SerializedName("剩余本金(元)")
    @ExcelProperty(index = 4, value = "剩余本金", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal remainPrincipal;

    /**
     * 剩余利息
     * 表字段 : t_asset_account_check.REMAIN_INTEREST
     */
    @SerializedName("剩余利息(元)")
    @ExcelProperty(index = 5, value = "剩余利息", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal remainInterest;

    /**
     * 剩余其他费用
     * 表字段 : t_asset_account_check.REMAIN_OTHER_FEE
     */
    @SerializedName("剩余其他费用(元)")
    @ExcelProperty(index = 6, value = "剩余费用", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal remainOtherFee;

    /**
     * 下一期应还款日
     * 表字段 : t_asset_account_check.NEXT_TERM_SHOULDREPAY_TIME
     */
    @SerializedName("下一期应还款日")
    private Date nextTermShouldrepayTime;

    /**
     * 资产状态
     * 表字段 : t_asset_account_check.ASSET_STATUS
     */
    @SerializedName("资产状态")
    @ExcelProperty(index = 2, value = "资产状态", required = true, checkInEnum = true, enumClz = AssetCheckEnum.class)
    private String assetStatus;

    /**
     * 结清原因
     * 表字段 : t_asset_account_check.CLOSEOFF_REASON
     */
    @SerializedName("结清原因")
    @ExcelProperty(index = 3, value = "结清原因", required = true, checkString = true, checkInEnum = true, enumClz = AssetCheckWhyEnum.class)
    private String closeoffReason;

    /**
     * 当前逾期本金
     * 表字段 : t_asset_account_check.CURRENT_OVERDUE_PRINCIPAL
     */
    @SerializedName("当前逾期本金")
    @ExcelProperty(index = 9, value = "当前逾期本金", required = true, checkBigDecimal = true)
    private BigDecimal currentOverduePrincipal;

    /**
     * 当前逾期利息
     * 表字段 : t_asset_account_check.CURRENT_OVERDUE_INTEREST
     */
    @SerializedName("当前逾期利息")
    @ExcelProperty(index = 11, value = "当前逾期利息", required = true, checkBigDecimal = true)
    private BigDecimal currentOverdueInterest;

    /**
     * 当前逾期费用
     * 表字段 : t_asset_account_check.CURRENT_OVERDUE_FEE
     */
    @SerializedName("当前逾期费用")
    @ExcelProperty(index = 12, value = "当前逾期费用", required = true, checkBigDecimal = true)
    private BigDecimal currentOverdueFee;

    /**
     * 当前逾期天数
     * 表字段 : t_asset_account_check.CURRENT_OVERDUE_DAYNUM
     */
    @SerializedName("当前逾期天数(天)")
    @ExcelProperty(index = 13, value = "当前逾期天数", required = true, canBeNull = true, checkZeroPositiveInteger = true)
    private Integer currentOverdueDaynum;

    /**
     * 累计逾期天数
     * 表字段 : t_asset_account_check.TOTAL_OVERDUE_DAYNUM
     */
    @SerializedName("累计逾期天数")
    private Integer totalOverdueDaynum;

    /**
     * 历史最高逾期天数
     * 表字段 : t_asset_account_check.HISTORY_MOST_OVERDUE_DAYNUM
     */
    @SerializedName("历史单次最长逾期天数(天)")
    @ExcelProperty(index = 10, value = "最长单期逾期天数", required = true, checkInteger = true)
    private Integer historyMostOverdueDaynum;

    /**
     * 历史累计逾期天数
     * 表字段 : t_asset_account_check.HISTORY_TOTAL_OVERDUE_DAYNUM
     */
    @SerializedName("历史累计逾期天数")
    private Integer historyTotalOverdueDaynum;

    /**
     * 当前逾期期数
     * 表字段 : t_asset_account_check.CURRENT_OVERDUE_TERMNUM
     */
    @SerializedName("当前逾期期数")
    private Integer currentOverdueTermnum;

    /**
     * 累计逾期期数
     * 表字段 : t_asset_account_check.TOTAL_OVERDUE_TERMNUM
     */
    @SerializedName("累计逾期期数")
    private Integer totalOverdueTermnum;

    /**
     * 历史单次最长逾期期数
     * 表字段 : t_asset_account_check.HISTORY_LONGEST_OVERDUE_TERM
     */
    @SerializedName("历史单次最长逾期期数")
    private Integer historyLongestOverdueTerm;

    /**
     * 历史最大逾期本金
     * 表字段 : t_asset_account_check.HISTORY_TOP_OVERDUE_PRINCIPAL
     */
    @SerializedName("历史最大逾期本金")
    private BigDecimal historyTopOverduePrincipal;

    /**
     * 数据提取日
     * 表字段 : t_asset_account_check.DATA_EXTRACT_TIME
     */
    @SerializedName("数据提取日")
    @ExcelProperty(index = 14, value = "数据提取日", required = true, canBeNull = true)
    private Date dataExtractTime;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport,3:systemgenerated
     **/
    private Integer dataSource;

    /**
     * 提前结清实还本金（元）
     */
    //@ExcelProperty(index = 11, value = "提前结清实还本金", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal principalSettledAdvance;

    /**
     * 提前结清实还利息（元）
     */
    //@ExcelProperty(index = 12, value = "提前结清实还利息", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal earlySettlementInterest;

    /**
     * 提前结清实还费用（元）
     */
    //@ExcelProperty(index = 13, value = "提前结清实还费用", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal settlePaymentAdvance;


}