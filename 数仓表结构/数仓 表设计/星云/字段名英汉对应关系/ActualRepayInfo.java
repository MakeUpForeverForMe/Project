package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.common.enums.asset.ActualRepayEnum;
import com.weshare.abscore.common.excel.annotation.ExcelEntity;
import com.weshare.abscore.common.excel.annotation.ExcelProperty;
import com.weshare.abscore.db.po.asset.base.BaseEntity;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

/**
 * 实际还款信息  File07
 **/
@Data
@ExcelEntity
public class ActualRepayInfo extends BaseEntity {

    /**
     * excel中的行数
     **/
    private int location;

    /**
     * 项目编号
     * 表字段 : t_actual_repay_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_actual_repay_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_actual_repay_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    @ExcelProperty(index = 0, value = "借据号", required = true)
    private String serialNumber;

    /**
     * 合同编号
     **/
    @ExcelProperty(index = 1, value = "合同编号", required = true)
    private String contractCode;

    /**
     * 期次
     * 表字段 : t_actual_repay_info.TERM
     */
    @SerializedName("期次")
    @ExcelProperty(index = 2, value = "还款期次", required = true, canBeNull = true, checkZeroPositiveInteger = true)
    private Integer term;

    /**
     * 应还款日
     * 表字段 : t_actual_repay_info.SHOUD_REPAY_DATE
     */
    @SerializedName("应还款日")
    private Date shoudRepayDate;

    /**
     * 应还本金（元）
     * 表字段 : t_actual_repay_info.SHOUD_REPAY_PRINCIPAL
     */
    @SerializedName("应还本金(元)")
    private BigDecimal shoudRepayPrincipal;

    /**
     * 应还利息（元）
     * 表字段 : t_actual_repay_info.SHOUD_REPAY_INTEREST
     */
    @SerializedName("应还利息(元)")
    private BigDecimal shoudRepayInterest;

    /**
     * 应还费用（元）
     * 表字段 : t_actual_repay_info.SHOUD_REPAY_FEE
     */
    @SerializedName("应还费用(元)")
    private BigDecimal shoudRepayFee;

    /**
     * 还款类型
     * 1，提前还款
     * 2，正常还款
     * 3，部分还款
     * 4，逾期还款
     * 表字段 : t_actual_repay_info.REPAY_TYPE
     */
    @SerializedName("还款类型")
    @ExcelProperty(index = 4, value = "回收类型", required = true, checkInEnum = true, enumName = "Name", enumClz = ActualRepayEnum.class)
    private String repayType;

    /**
     * 实际还款执行利率
     * 表字段 : t_actual_repay_info.ACTUAL_WORK_INTEREST_RATE
     */
    @SerializedName("实还执行利率")
    private BigDecimal actualWorkInterestRate;

    /**
     * 实际还款金额
     * 非表字段
     */
    private BigDecimal actualRepayAmount;
    /**
     * 实还本金（元）
     * 表字段 : t_actual_repay_info.ACTUAL_REPAY_PRINCIPAL
     */
    @SerializedName("实还本金(元)")
    @ExcelProperty(index = 5, value = "还本金额(元)", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal actualRepayPrincipal;

    /**
     * 实还利息（元）
     * 表字段 : t_actual_repay_info.ACTUAL_REPAY_INTEREST
     */
    @SerializedName("实还利息(元)")
    @ExcelProperty(index = 6, value = "利息金额(元)", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal actualRepayInterest;

    /**
     * 实还费用（元）
     * 表字段 : t_actual_repay_info.ACTUAL_REPAY_FEE
     */
    @SerializedName("实还费用(元)")
    @ExcelProperty(index = 7, value = "费用金额(元)", required = true, canBeNull = true, checkPositiveNumber = true)
    private BigDecimal actualRepayFee;

    /**
     * 实际还清日期 : ( 若没有还清传还款日期，若还清就传实际还清的日期  )
     * 表字段 : t_actual_repay_info.ACTUAL_REPAY_TIME
     */
    @SerializedName("实际还清日期")
    @ExcelProperty(index = 3, value = "还款日期", required = true, canBeNull = true)
    private Date actualRepayTime;

    /**
     * 当期贷款余额  （各期还款日（T+1）更新该字段，即截至当期还款日资产的剩余（未偿还）贷款本金余额）
     * 表字段 : t_actual_repay_info.CURRENT_PERIOD_LOAN_BALANCE
     */
    @SerializedName("当期贷款余额")
    private BigDecimal currentPeriodLoanBalance;

    /**
     * 当期账户状态： （各期还款日（T+1）更新该字段，
     * 正常：在还款日前该期应还已还清
     * 提前还清（早偿）：贷款在该期还款日前提前全部还清
     * 逾期：贷款在该期还款日时，实还金额小于应还金额。
     * ）
     * 表字段 : t_actual_repay_info.CURRENT_ACCOUNT_STATUS
     */
    @SerializedName("当期账户状态")
    private String currentAccountStatus;

    /**
     * 违约金
     * 表字段 : t_actual_repay_info.PENALBOND
     */
    @SerializedName("违约金")
    private BigDecimal penalbond;

    /**
     * 罚息
     * 表字段 : t_actual_repay_info.PENALTY_INTEREST
     */
    @SerializedName("罚息")
    private BigDecimal penaltyInterest;

    /**
     * 赔偿金（提前还款/逾期所产生的赔偿金）
     * 表字段 : t_actual_repay_info.COMPENSATION
     */
    @SerializedName("赔偿金")
    private BigDecimal compensation;

    /**
     * 提前还款手续费
     * 表字段 : t_actual_repay_info.ADVANCED_COMMISSION_CHARGE
     */
    @SerializedName("提前还款手续费")
    private BigDecimal advancedCommissionCharge;

    /**
     * 其他相关费用 （违约金、罚款、赔偿金和提前还款手续费以外的费用）
     * 表字段 : t_actual_repay_info.OTHER_FEE
     */
    @SerializedName("其它相关费用")
    private BigDecimal otherFee;

    /**
     * 是否借款人本人还款
     * 预定义字段
     * Y
     * N
     * <p>
     * 表字段 : t_actual_repay_info.IS_BORROWERS_ONESELF_REPAYMENT
     */
    @SerializedName("是否借款人本人还款")
    private String isBorrowersOneselfRepayment;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;
}