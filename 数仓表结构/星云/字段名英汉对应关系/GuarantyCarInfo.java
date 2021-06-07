package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.math.BigDecimal;
import java.util.Date;

/**
 *  抵押物(车)信息  File04
 **/
public class GuarantyCarInfo extends BaseEntity {

    /**
     * 项目编号
     * 表字段 : t_guaranty_car_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_guaranty_car_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_guaranty_car_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 抵押物编号
     * 表字段 : t_guaranty_car_info.GUARANTY_CODE
     */
    @SerializedName("抵押物编号")
    private String guarantyCode;

    /**
     * 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理

     * 表字段 : t_guaranty_car_info.GUARANTY_HANDLING_STATUS
     */
    @SerializedName("抵押办理状态")
    private String guarantyHandlingStatus;

    /**
     * 抵押顺位 
1-第一顺位
2-第二顺位
9-其他
     * 表字段 : t_guaranty_car_info.GUARANTY_ALIGNMENT
     */
    @SerializedName("抵押顺位")
    private String guarantyAlignment;

    /**
     * 车辆性质 
预定义字段：
非融资车分期
非融资车抵贷
融资租赁车分期
融资租赁车抵贷

     * 表字段 : t_guaranty_car_info.CAR_PROPERTY
     */
    @SerializedName("车辆性质")
    private String carProperty;

    /**
     * 融资方式 
预定义字段：
正租
反租

     * 表字段 : t_guaranty_car_info.FINANCING_TYPE
     */
    @SerializedName("融资方式")
    private String financingType;

    /**
     * 担保方式 
预定义字段：
质押担保，
信用担保，
保证担保，
抵押担保

     * 表字段 : t_guaranty_car_info.GUARANTEE_TYPE
     */
    @SerializedName("担保方式")
    private String guaranteeType;

    /**
     * 评估价格(元)
     * 表字段 : t_guaranty_car_info.PAWN_VALUE
     */
    @SerializedName("评估价格(元)")
    private BigDecimal pawnValue;

    /**
     * 车辆销售价格(元)
     * 表字段 : t_guaranty_car_info.CAR_SALES_PRICE
     */
    @SerializedName("车辆销售价格(元)")
    private BigDecimal carSalesPrice;

    /**
     * 新车指导价(元)
     * 表字段 : t_guaranty_car_info.CAR_NEW_PRICE
     */
    @SerializedName("新车指导价(元)")
    private BigDecimal carNewPrice;

    /**
     * 投资总额(元)
     * 表字段 : t_guaranty_car_info.TOTAL_INVESTMENT
     */
    @SerializedName("投资总额(元)")
    private BigDecimal totalInvestment;

    /**
     * 购置税金额(元)
     * 表字段 : t_guaranty_car_info.PURCHASE_TAX_AMOUTS
     */
    @SerializedName("购置税金额(元)")
    private BigDecimal purchaseTaxAmouts;

    /**
     * 保险种类 
交强险
第三者责任险
盗抢险
车损险
不计免赔
其他

     * 表字段 : t_guaranty_car_info.INSURANCE_TYPE
     */
    @SerializedName("保险种类")
    private String insuranceType;

    /**
     * 汽车保险总费用
     * 表字段 : t_guaranty_car_info.CAR_INSURANCE_PREMIUM
     */
    @SerializedName("汽车保险总费用")
    private BigDecimal carInsurancePremium;

    /**
     * 手续总费用(元)
     * 表字段 : t_guaranty_car_info.TOTAL_POUNDAGE
     */
    @SerializedName("手续总费用(元)")
    private BigDecimal totalPoundage;

    /**
     * 累计车辆过户次数
     * 表字段 : t_guaranty_car_info.CUMULATIVE_CAR_TRANSFER_NUMBER
     */
    @SerializedName("累计车辆过户次数")
    private Integer cumulativeCarTransferNumber;

    /**
     * 一年内车辆过户次数
     * 表字段 : t_guaranty_car_info.ONE_YEAR_CAR_TRANSFER_NUMBER
     */
    @SerializedName("一年内车辆过户次数")
    private Integer oneYearCarTransferNumber;

    /**
     * 责信保费用1
     * 表字段 : t_guaranty_car_info.LIABILITY_INSURANCE_COST1
     */
    @SerializedName("责信保费用1")
    private BigDecimal liabilityInsuranceCost1;

    /**
     * 责信保费用2
     * 表字段 : t_guaranty_car_info.LIABILITY_INSURANCE_COST2
     */
    @SerializedName("责信保费用2")
    private BigDecimal liabilityInsuranceCost2;

    /**
     * 车类型 
预定义字段：
新车
二手车

     * 表字段 : t_guaranty_car_info.CAR_TYPE
     */
    @SerializedName("车类型")
    private String carType;

    /**
     * 车架号
     * 表字段 : t_guaranty_car_info.FRAME_NUM
     */
    @SerializedName("车架号")
    private String frameNum;

    /**
     * 发动机号
     * 表字段 : t_guaranty_car_info.ENGINE_NUM
     */
    @SerializedName("发动机号")
    private String engineNum;

    /**
     * GPS编号
     * 表字段 : t_guaranty_car_info.GPS_CODE
     */
    @SerializedName("GPS编号")
    private String gpsCode;

    /**
     * GPS费用
     * 表字段 : t_guaranty_car_info.GPS_COST
     */
    @SerializedName("GPS费用")
    private BigDecimal gpsCost;

    /**
     * 车牌号码
     * 表字段 : t_guaranty_car_info.LICENSE_NUM
     */
    @SerializedName("车牌号码")
    private String licenseNum;

    /**
     * 车辆品牌
     * 表字段 : t_guaranty_car_info.CAR_BRAND
     */
    @SerializedName("车辆品牌")
    private String carBrand;

    /**
     * 车系
     * 表字段 : t_guaranty_car_info.CAR_SYSTEM
     */
    @SerializedName("车系")
    private String carSystem;

    /**
     * 车型
     * 表字段 : t_guaranty_car_info.CAR_MODEL
     */
    @SerializedName("车型")
    private String carModel;

    /**
     * 车龄
     * 表字段 : t_guaranty_car_info.CAR_AGE
     */
    @SerializedName("车龄")
    private BigDecimal carAge;

    /**
     * 车辆能源类型 
预定义字段：
混合动力
纯电
非新能源车

     * 表字段 : t_guaranty_car_info.CAR_ENERGY_TYPE
     */
    @SerializedName("车辆能源类型")
    private String carEnergyType;

    /**
     * 生产日期
     * 表字段 : t_guaranty_car_info.PRODUCTION_DATE
     */
    @SerializedName("生产日期")
    private String productionDate;

    /**
     * 里程数
     * 表字段 : t_guaranty_car_info.MILEAGE
     */
    @SerializedName("里程数")
    private BigDecimal mileage;

    /**
     * 注册日期
     * 表字段 : t_guaranty_car_info.REGISTER_DATE
     */
    @SerializedName("注册日期")
    private Date registerDate;

    /**
     * 车辆购买地
     * 表字段 : t_guaranty_car_info.BUY_CAR_ADDRESS
     */
    @SerializedName("车辆购买地")
    private String buyCarAddress;

    /**
     * 车辆颜色
     * 表字段 : t_guaranty_car_info.CAR_COLOUR
     */
    @SerializedName("车辆颜色")
    private String carColour;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_guaranty_car_info.PROJECT_ID
     *
     * @return t_guaranty_car_info.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_guaranty_car_info.PROJECT_ID
     *
     * @param projectId t_guaranty_car_info.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_guaranty_car_info.AGENCY_ID
     *
     * @return t_guaranty_car_info.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_guaranty_car_info.AGENCY_ID
     *
     * @param agencyId t_guaranty_car_info.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_guaranty_car_info.SERIAL_NUMBER
     *
     * @return t_guaranty_car_info.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_guaranty_car_info.SERIAL_NUMBER
     *
     * @param serialNumber t_guaranty_car_info.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 抵押物编号 字段:t_guaranty_car_info.GUARANTY_CODE
     *
     * @return t_guaranty_car_info.GUARANTY_CODE, 抵押物编号
     */
    public String getGuarantyCode() {
        return guarantyCode;
    }

    /**
     * 设置 抵押物编号 字段:t_guaranty_car_info.GUARANTY_CODE
     *
     * @param guarantyCode t_guaranty_car_info.GUARANTY_CODE, 抵押物编号
     */
    public void setGuarantyCode(String guarantyCode) {
        this.guarantyCode = guarantyCode == null ? null : guarantyCode.trim();
    }

    /**
     * 获取 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理
 字段:t_guaranty_car_info.GUARANTY_HANDLING_STATUS
     *
     * @return t_guaranty_car_info.GUARANTY_HANDLING_STATUS, 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理

     */
    public String getGuarantyHandlingStatus() {
        return guarantyHandlingStatus;
    }

    /**
     * 设置 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理
 字段:t_guaranty_car_info.GUARANTY_HANDLING_STATUS
     *
     * @param guarantyHandlingStatus t_guaranty_car_info.GUARANTY_HANDLING_STATUS, 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理

     */
    public void setGuarantyHandlingStatus(String guarantyHandlingStatus) {
        this.guarantyHandlingStatus = guarantyHandlingStatus == null ? null : guarantyHandlingStatus.trim();
    }

    /**
     * 获取 抵押顺位 
1-第一顺位
2-第二顺位
9-其他 字段:t_guaranty_car_info.GUARANTY_ALIGNMENT
     *
     * @return t_guaranty_car_info.GUARANTY_ALIGNMENT, 抵押顺位 
1-第一顺位
2-第二顺位
9-其他
     */
    public String getGuarantyAlignment() {
        return guarantyAlignment;
    }

    /**
     * 设置 抵押顺位 
1-第一顺位
2-第二顺位
9-其他 字段:t_guaranty_car_info.GUARANTY_ALIGNMENT
     *
     * @param guarantyAlignment t_guaranty_car_info.GUARANTY_ALIGNMENT, 抵押顺位 
1-第一顺位
2-第二顺位
9-其他
     */
    public void setGuarantyAlignment(String guarantyAlignment) {
        this.guarantyAlignment = guarantyAlignment == null ? null : guarantyAlignment.trim();
    }

    /**
     * 获取 车辆性质 
预定义字段：
非融资车分期
非融资车抵贷
融资租赁车分期
融资租赁车抵贷
 字段:t_guaranty_car_info.CAR_PROPERTY
     *
     * @return t_guaranty_car_info.CAR_PROPERTY, 车辆性质 
预定义字段：
非融资车分期
非融资车抵贷
融资租赁车分期
融资租赁车抵贷

     */
    public String getCarProperty() {
        return carProperty;
    }

    /**
     * 设置 车辆性质 
预定义字段：
非融资车分期
非融资车抵贷
融资租赁车分期
融资租赁车抵贷
 字段:t_guaranty_car_info.CAR_PROPERTY
     *
     * @param carProperty t_guaranty_car_info.CAR_PROPERTY, 车辆性质 
预定义字段：
非融资车分期
非融资车抵贷
融资租赁车分期
融资租赁车抵贷

     */
    public void setCarProperty(String carProperty) {
        this.carProperty = carProperty == null ? null : carProperty.trim();
    }

    /**
     * 获取 融资方式 
预定义字段：
正租
反租
 字段:t_guaranty_car_info.FINANCING_TYPE
     *
     * @return t_guaranty_car_info.FINANCING_TYPE, 融资方式 
预定义字段：
正租
反租

     */
    public String getFinancingType() {
        return financingType;
    }

    /**
     * 设置 融资方式 
预定义字段：
正租
反租
 字段:t_guaranty_car_info.FINANCING_TYPE
     *
     * @param financingType t_guaranty_car_info.FINANCING_TYPE, 融资方式 
预定义字段：
正租
反租

     */
    public void setFinancingType(String financingType) {
        this.financingType = financingType == null ? null : financingType.trim();
    }

    /**
     * 获取 担保方式 
预定义字段：
质押担保，
信用担保，
保证担保，
抵押担保
 字段:t_guaranty_car_info.GUARANTEE_TYPE
     *
     * @return t_guaranty_car_info.GUARANTEE_TYPE, 担保方式 
预定义字段：
质押担保，
信用担保，
保证担保，
抵押担保

     */
    public String getGuaranteeType() {
        return guaranteeType;
    }

    /**
     * 设置 担保方式 
预定义字段：
质押担保，
信用担保，
保证担保，
抵押担保
 字段:t_guaranty_car_info.GUARANTEE_TYPE
     *
     * @param guaranteeType t_guaranty_car_info.GUARANTEE_TYPE, 担保方式 
预定义字段：
质押担保，
信用担保，
保证担保，
抵押担保

     */
    public void setGuaranteeType(String guaranteeType) {
        this.guaranteeType = guaranteeType == null ? null : guaranteeType.trim();
    }

    /**
     * 获取 评估价格(元) 字段:t_guaranty_car_info.PAWN_VALUE
     *
     * @return t_guaranty_car_info.PAWN_VALUE, 评估价格(元)
     */
    public BigDecimal getPawnValue() {
        return pawnValue;
    }

    /**
     * 设置 评估价格(元) 字段:t_guaranty_car_info.PAWN_VALUE
     *
     * @param pawnValue t_guaranty_car_info.PAWN_VALUE, 评估价格(元)
     */
    public void setPawnValue(BigDecimal pawnValue) {
        this.pawnValue = pawnValue;
    }

    /**
     * 获取 车辆销售价格(元) 字段:t_guaranty_car_info.CAR_SALES_PRICE
     *
     * @return t_guaranty_car_info.CAR_SALES_PRICE, 车辆销售价格(元)
     */
    public BigDecimal getCarSalesPrice() {
        return carSalesPrice;
    }

    /**
     * 设置 车辆销售价格(元) 字段:t_guaranty_car_info.CAR_SALES_PRICE
     *
     * @param carSalesPrice t_guaranty_car_info.CAR_SALES_PRICE, 车辆销售价格(元)
     */
    public void setCarSalesPrice(BigDecimal carSalesPrice) {
        this.carSalesPrice = carSalesPrice;
    }

    /**
     * 获取 新车指导价(元) 字段:t_guaranty_car_info.CAR_NEW_PRICE
     *
     * @return t_guaranty_car_info.CAR_NEW_PRICE, 新车指导价(元)
     */
    public BigDecimal getCarNewPrice() {
        return carNewPrice;
    }

    /**
     * 设置 新车指导价(元) 字段:t_guaranty_car_info.CAR_NEW_PRICE
     *
     * @param carNewPrice t_guaranty_car_info.CAR_NEW_PRICE, 新车指导价(元)
     */
    public void setCarNewPrice(BigDecimal carNewPrice) {
        this.carNewPrice = carNewPrice;
    }

    /**
     * 获取 投资总额(元) 字段:t_guaranty_car_info.TOTAL_INVESTMENT
     *
     * @return t_guaranty_car_info.TOTAL_INVESTMENT, 投资总额(元)
     */
    public BigDecimal getTotalInvestment() {
        return totalInvestment;
    }

    /**
     * 设置 投资总额(元) 字段:t_guaranty_car_info.TOTAL_INVESTMENT
     *
     * @param totalInvestment t_guaranty_car_info.TOTAL_INVESTMENT, 投资总额(元)
     */
    public void setTotalInvestment(BigDecimal totalInvestment) {
        this.totalInvestment = totalInvestment;
    }

    /**
     * 获取 购置税金额(元) 字段:t_guaranty_car_info.PURCHASE_TAX_AMOUTS
     *
     * @return t_guaranty_car_info.PURCHASE_TAX_AMOUTS, 购置税金额(元)
     */
    public BigDecimal getPurchaseTaxAmouts() {
        return purchaseTaxAmouts;
    }

    /**
     * 设置 购置税金额(元) 字段:t_guaranty_car_info.PURCHASE_TAX_AMOUTS
     *
     * @param purchaseTaxAmouts t_guaranty_car_info.PURCHASE_TAX_AMOUTS, 购置税金额(元)
     */
    public void setPurchaseTaxAmouts(BigDecimal purchaseTaxAmouts) {
        this.purchaseTaxAmouts = purchaseTaxAmouts;
    }

    /**
     * 获取 保险种类 
交强险
第三者责任险
盗抢险
车损险
不计免赔
其他
 字段:t_guaranty_car_info.INSURANCE_TYPE
     *
     * @return t_guaranty_car_info.INSURANCE_TYPE, 保险种类 
交强险
第三者责任险
盗抢险
车损险
不计免赔
其他

     */
    public String getInsuranceType() {
        return insuranceType;
    }

    /**
     * 设置 保险种类 
交强险
第三者责任险
盗抢险
车损险
不计免赔
其他
 字段:t_guaranty_car_info.INSURANCE_TYPE
     *
     * @param insuranceType t_guaranty_car_info.INSURANCE_TYPE, 保险种类 
交强险
第三者责任险
盗抢险
车损险
不计免赔
其他

     */
    public void setInsuranceType(String insuranceType) {
        this.insuranceType = insuranceType == null ? null : insuranceType.trim();
    }

    /**
     * 获取 汽车保险总费用 字段:t_guaranty_car_info.CAR_INSURANCE_PREMIUM
     *
     * @return t_guaranty_car_info.CAR_INSURANCE_PREMIUM, 汽车保险总费用
     */
    public BigDecimal getCarInsurancePremium() {
        return carInsurancePremium;
    }

    /**
     * 设置 汽车保险总费用 字段:t_guaranty_car_info.CAR_INSURANCE_PREMIUM
     *
     * @param carInsurancePremium t_guaranty_car_info.CAR_INSURANCE_PREMIUM, 汽车保险总费用
     */
    public void setCarInsurancePremium(BigDecimal carInsurancePremium) {
        this.carInsurancePremium = carInsurancePremium;
    }

    /**
     * 获取 手续总费用(元) 字段:t_guaranty_car_info.TOTAL_POUNDAGE
     *
     * @return t_guaranty_car_info.TOTAL_POUNDAGE, 手续总费用(元)
     */
    public BigDecimal getTotalPoundage() {
        return totalPoundage;
    }

    /**
     * 设置 手续总费用(元) 字段:t_guaranty_car_info.TOTAL_POUNDAGE
     *
     * @param totalPoundage t_guaranty_car_info.TOTAL_POUNDAGE, 手续总费用(元)
     */
    public void setTotalPoundage(BigDecimal totalPoundage) {
        this.totalPoundage = totalPoundage;
    }

    /**
     * 获取 累计车辆过户次数 字段:t_guaranty_car_info.CUMULATIVE_CAR_TRANSFER_NUMBER
     *
     * @return t_guaranty_car_info.CUMULATIVE_CAR_TRANSFER_NUMBER, 累计车辆过户次数
     */
    public Integer getCumulativeCarTransferNumber() {
        return cumulativeCarTransferNumber;
    }

    /**
     * 设置 累计车辆过户次数 字段:t_guaranty_car_info.CUMULATIVE_CAR_TRANSFER_NUMBER
     *
     * @param cumulativeCarTransferNumber t_guaranty_car_info.CUMULATIVE_CAR_TRANSFER_NUMBER, 累计车辆过户次数
     */
    public void setCumulativeCarTransferNumber(Integer cumulativeCarTransferNumber) {
        this.cumulativeCarTransferNumber = cumulativeCarTransferNumber;
    }

    /**
     * 获取 一年内车辆过户次数 字段:t_guaranty_car_info.ONE_YEAR_CAR_TRANSFER_NUMBER
     *
     * @return t_guaranty_car_info.ONE_YEAR_CAR_TRANSFER_NUMBER, 一年内车辆过户次数
     */
    public Integer getOneYearCarTransferNumber() {
        return oneYearCarTransferNumber;
    }

    /**
     * 设置 一年内车辆过户次数 字段:t_guaranty_car_info.ONE_YEAR_CAR_TRANSFER_NUMBER
     *
     * @param oneYearCarTransferNumber t_guaranty_car_info.ONE_YEAR_CAR_TRANSFER_NUMBER, 一年内车辆过户次数
     */
    public void setOneYearCarTransferNumber(Integer oneYearCarTransferNumber) {
        this.oneYearCarTransferNumber = oneYearCarTransferNumber;
    }

    /**
     * 获取 责信保费用1 字段:t_guaranty_car_info.LIABILITY_INSURANCE_COST1
     *
     * @return t_guaranty_car_info.LIABILITY_INSURANCE_COST1, 责信保费用1
     */
    public BigDecimal getLiabilityInsuranceCost1() {
        return liabilityInsuranceCost1;
    }

    /**
     * 设置 责信保费用1 字段:t_guaranty_car_info.LIABILITY_INSURANCE_COST1
     *
     * @param liabilityInsuranceCost1 t_guaranty_car_info.LIABILITY_INSURANCE_COST1, 责信保费用1
     */
    public void setLiabilityInsuranceCost1(BigDecimal liabilityInsuranceCost1) {
        this.liabilityInsuranceCost1 = liabilityInsuranceCost1;
    }

    /**
     * 获取 责信保费用2 字段:t_guaranty_car_info.LIABILITY_INSURANCE_COST2
     *
     * @return t_guaranty_car_info.LIABILITY_INSURANCE_COST2, 责信保费用2
     */
    public BigDecimal getLiabilityInsuranceCost2() {
        return liabilityInsuranceCost2;
    }

    /**
     * 设置 责信保费用2 字段:t_guaranty_car_info.LIABILITY_INSURANCE_COST2
     *
     * @param liabilityInsuranceCost2 t_guaranty_car_info.LIABILITY_INSURANCE_COST2, 责信保费用2
     */
    public void setLiabilityInsuranceCost2(BigDecimal liabilityInsuranceCost2) {
        this.liabilityInsuranceCost2 = liabilityInsuranceCost2;
    }

    /**
     * 获取 车类型 
预定义字段：
新车
二手车
 字段:t_guaranty_car_info.CAR_TYPE
     *
     * @return t_guaranty_car_info.CAR_TYPE, 车类型 
预定义字段：
新车
二手车

     */
    public String getCarType() {
        return carType;
    }

    /**
     * 设置 车类型 
预定义字段：
新车
二手车
 字段:t_guaranty_car_info.CAR_TYPE
     *
     * @param carType t_guaranty_car_info.CAR_TYPE, 车类型 
预定义字段：
新车
二手车

     */
    public void setCarType(String carType) {
        this.carType = carType == null ? null : carType.trim();
    }

    /**
     * 获取 车架号 字段:t_guaranty_car_info.FRAME_NUM
     *
     * @return t_guaranty_car_info.FRAME_NUM, 车架号
     */
    public String getFrameNum() {
        return frameNum;
    }

    /**
     * 设置 车架号 字段:t_guaranty_car_info.FRAME_NUM
     *
     * @param frameNum t_guaranty_car_info.FRAME_NUM, 车架号
     */
    public void setFrameNum(String frameNum) {
        this.frameNum = frameNum == null ? null : frameNum.trim();
    }

    /**
     * 获取 发动机号 字段:t_guaranty_car_info.ENGINE_NUM
     *
     * @return t_guaranty_car_info.ENGINE_NUM, 发动机号
     */
    public String getEngineNum() {
        return engineNum;
    }

    /**
     * 设置 发动机号 字段:t_guaranty_car_info.ENGINE_NUM
     *
     * @param engineNum t_guaranty_car_info.ENGINE_NUM, 发动机号
     */
    public void setEngineNum(String engineNum) {
        this.engineNum = engineNum == null ? null : engineNum.trim();
    }

    /**
     * 获取 GPS编号 字段:t_guaranty_car_info.GPS_CODE
     *
     * @return t_guaranty_car_info.GPS_CODE, GPS编号
     */
    public String getGpsCode() {
        return gpsCode;
    }

    /**
     * 设置 GPS编号 字段:t_guaranty_car_info.GPS_CODE
     *
     * @param gpsCode t_guaranty_car_info.GPS_CODE, GPS编号
     */
    public void setGpsCode(String gpsCode) {
        this.gpsCode = gpsCode == null ? null : gpsCode.trim();
    }

    /**
     * 获取 GPS费用 字段:t_guaranty_car_info.GPS_COST
     *
     * @return t_guaranty_car_info.GPS_COST, GPS费用
     */
    public BigDecimal getGpsCost() {
        return gpsCost;
    }

    /**
     * 设置 GPS费用 字段:t_guaranty_car_info.GPS_COST
     *
     * @param gpsCost t_guaranty_car_info.GPS_COST, GPS费用
     */
    public void setGpsCost(BigDecimal gpsCost) {
        this.gpsCost = gpsCost;
    }

    /**
     * 获取 车牌号码 字段:t_guaranty_car_info.LICENSE_NUM
     *
     * @return t_guaranty_car_info.LICENSE_NUM, 车牌号码
     */
    public String getLicenseNum() {
        return licenseNum;
    }

    /**
     * 设置 车牌号码 字段:t_guaranty_car_info.LICENSE_NUM
     *
     * @param licenseNum t_guaranty_car_info.LICENSE_NUM, 车牌号码
     */
    public void setLicenseNum(String licenseNum) {
        this.licenseNum = licenseNum == null ? null : licenseNum.trim();
    }

    /**
     * 获取 车辆品牌 字段:t_guaranty_car_info.CAR_BRAND
     *
     * @return t_guaranty_car_info.CAR_BRAND, 车辆品牌
     */
    public String getCarBrand() {
        return carBrand;
    }

    /**
     * 设置 车辆品牌 字段:t_guaranty_car_info.CAR_BRAND
     *
     * @param carBrand t_guaranty_car_info.CAR_BRAND, 车辆品牌
     */
    public void setCarBrand(String carBrand) {
        this.carBrand = carBrand == null ? null : carBrand.trim();
    }

    /**
     * 获取 车系 字段:t_guaranty_car_info.CAR_SYSTEM
     *
     * @return t_guaranty_car_info.CAR_SYSTEM, 车系
     */
    public String getCarSystem() {
        return carSystem;
    }

    /**
     * 设置 车系 字段:t_guaranty_car_info.CAR_SYSTEM
     *
     * @param carSystem t_guaranty_car_info.CAR_SYSTEM, 车系
     */
    public void setCarSystem(String carSystem) {
        this.carSystem = carSystem == null ? null : carSystem.trim();
    }

    /**
     * 获取 车型 字段:t_guaranty_car_info.CAR_MODEL
     *
     * @return t_guaranty_car_info.CAR_MODEL, 车型
     */
    public String getCarModel() {
        return carModel;
    }

    /**
     * 设置 车型 字段:t_guaranty_car_info.CAR_MODEL
     *
     * @param carModel t_guaranty_car_info.CAR_MODEL, 车型
     */
    public void setCarModel(String carModel) {
        this.carModel = carModel == null ? null : carModel.trim();
    }

    /**
     * 获取 车龄 字段:t_guaranty_car_info.CAR_AGE
     *
     * @return t_guaranty_car_info.CAR_AGE, 车龄
     */
    public BigDecimal getCarAge() {
        return carAge;
    }

    /**
     * 设置 车龄 字段:t_guaranty_car_info.CAR_AGE
     *
     * @param carAge t_guaranty_car_info.CAR_AGE, 车龄
     */
    public void setCarAge(BigDecimal carAge) {
        this.carAge = carAge;
    }

    /**
     * 获取 车辆能源类型 
预定义字段：
混合动力
纯电
非新能源车
 字段:t_guaranty_car_info.CAR_ENERGY_TYPE
     *
     * @return t_guaranty_car_info.CAR_ENERGY_TYPE, 车辆能源类型 
预定义字段：
混合动力
纯电
非新能源车

     */
    public String getCarEnergyType() {
        return carEnergyType;
    }

    /**
     * 设置 车辆能源类型 
预定义字段：
混合动力
纯电
非新能源车
 字段:t_guaranty_car_info.CAR_ENERGY_TYPE
     *
     * @param carEnergyType t_guaranty_car_info.CAR_ENERGY_TYPE, 车辆能源类型 
预定义字段：
混合动力
纯电
非新能源车

     */
    public void setCarEnergyType(String carEnergyType) {
        this.carEnergyType = carEnergyType == null ? null : carEnergyType.trim();
    }

    /**
     * 获取 生产日期 字段:t_guaranty_car_info.PRODUCTION_DATE
     *
     * @return t_guaranty_car_info.PRODUCTION_DATE, 生产日期
     */
    public String getProductionDate() {
        return productionDate;
    }

    /**
     * 设置 生产日期 字段:t_guaranty_car_info.PRODUCTION_DATE
     *
     * @param productionDate t_guaranty_car_info.PRODUCTION_DATE, 生产日期
     */
    public void setProductionDate(String productionDate) {
        this.productionDate = productionDate == null ? null : productionDate.trim();
    }

    /**
     * 获取 里程数 字段:t_guaranty_car_info.MILEAGE
     *
     * @return t_guaranty_car_info.MILEAGE, 里程数
     */
    public BigDecimal getMileage() {
        return mileage;
    }

    /**
     * 设置 里程数 字段:t_guaranty_car_info.MILEAGE
     *
     * @param mileage t_guaranty_car_info.MILEAGE, 里程数
     */
    public void setMileage(BigDecimal mileage) {
        this.mileage = mileage;
    }

    /**
     * 获取 注册日期 字段:t_guaranty_car_info.REGISTER_DATE
     *
     * @return t_guaranty_car_info.REGISTER_DATE, 注册日期
     */
    public Date getRegisterDate() {
        return registerDate;
    }

    /**
     * 设置 注册日期 字段:t_guaranty_car_info.REGISTER_DATE
     *
     * @param registerDate t_guaranty_car_info.REGISTER_DATE, 注册日期
     */
    public void setRegisterDate(Date registerDate) {
        this.registerDate = registerDate == null ? null : registerDate;
    }

    /**
     * 获取 车辆购买地 字段:t_guaranty_car_info.BUY_CAR_ADDRESS
     *
     * @return t_guaranty_car_info.BUY_CAR_ADDRESS, 车辆购买地
     */
    public String getBuyCarAddress() {
        return buyCarAddress;
    }

    /**
     * 设置 车辆购买地 字段:t_guaranty_car_info.BUY_CAR_ADDRESS
     *
     * @param buyCarAddress t_guaranty_car_info.BUY_CAR_ADDRESS, 车辆购买地
     */
    public void setBuyCarAddress(String buyCarAddress) {
        this.buyCarAddress = buyCarAddress == null ? null : buyCarAddress.trim();
    }

    /**
     * 获取 车辆颜色 字段:t_guaranty_car_info.CAR_COLOUR
     *
     * @return t_guaranty_car_info.CAR_COLOUR, 车辆颜色
     */
    public String getCarColour() {
        return carColour;
    }

    /**
     * 设置 车辆颜色 字段:t_guaranty_car_info.CAR_COLOUR
     *
     * @param carColour t_guaranty_car_info.CAR_COLOUR, 车辆颜色
     */
    public void setCarColour(String carColour) {
        this.carColour = carColour == null ? null : carColour.trim();
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
        return "GuarantyCarInfo{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", guarantyCode='" + guarantyCode + '\'' +
                ", guarantyHandlingStatus='" + guarantyHandlingStatus + '\'' +
                ", guarantyAlignment='" + guarantyAlignment + '\'' +
                ", carProperty='" + carProperty + '\'' +
                ", financingType='" + financingType + '\'' +
                ", guaranteeType='" + guaranteeType + '\'' +
                ", pawnValue=" + pawnValue +
                ", carSalesPrice=" + carSalesPrice +
                ", carNewPrice=" + carNewPrice +
                ", totalInvestment=" + totalInvestment +
                ", purchaseTaxAmouts=" + purchaseTaxAmouts +
                ", insuranceType='" + insuranceType + '\'' +
                ", carInsurancePremium=" + carInsurancePremium +
                ", totalPoundage=" + totalPoundage +
                ", cumulativeCarTransferNumber=" + cumulativeCarTransferNumber +
                ", oneYearCarTransferNumber=" + oneYearCarTransferNumber +
                ", liabilityInsuranceCost1=" + liabilityInsuranceCost1 +
                ", liabilityInsuranceCost2=" + liabilityInsuranceCost2 +
                ", carType='" + carType + '\'' +
                ", frameNum='" + frameNum + '\'' +
                ", engineNum='" + engineNum + '\'' +
                ", gpsCode='" + gpsCode + '\'' +
                ", gpsCost=" + gpsCost +
                ", licenseNum='" + licenseNum + '\'' +
                ", carBrand='" + carBrand + '\'' +
                ", carSystem='" + carSystem + '\'' +
                ", carModel='" + carModel + '\'' +
                ", carAge=" + carAge +
                ", carEnergyType='" + carEnergyType + '\'' +
                ", productionDate='" + productionDate + '\'' +
                ", mileage=" + mileage +
                ", registerDate=" + registerDate +
                ", buyCarAddress='" + buyCarAddress + '\'' +
                ", carColour='" + carColour + '\'' +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}