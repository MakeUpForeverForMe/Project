package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.math.BigDecimal;

/**
 *  房抵押物信息  File13
 **/
public class GuarantyHouseInfo extends BaseEntity {

    /**
     * 项目编号
     * 表字段 : t_guaranty_house_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_guaranty_house_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_guaranty_house_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 抵押物编号
     * 表字段 : t_guaranty_house_info.GUARANTY_NUMBER
     */
    @SerializedName("抵押物编号")
    private String guarantyNumber;

    /**
     * 抵押物名称
     * 表字段 : t_guaranty_house_info.GUARANTY_NAME
     */
    @SerializedName("抵押物名称")
    private String guarantyName;

    /**
     * 抵押物描述
     * 表字段 : t_guaranty_house_info.GUARANTY_DESCRIBE
     */
    @SerializedName("抵押物描述")
    private String guarantyDescribe;

    /**
     * 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理

     * 表字段 : t_guaranty_house_info.GUARANTY_HANDLE_STATUS
     */
    @SerializedName("抵押办理状态")
    private String guarantyHandleStatus;

    /**
     * 抵押顺位 
第一顺位
第二顺位
其他
     * 表字段 : t_guaranty_house_info.GUARANTY_ALIGNMENT
     */
    @SerializedName("抵押顺位")
    private String guarantyAlignment;

    /**
     * 前手抵押余额
     * 表字段 : t_guaranty_house_info.GUARANTY_FRONT_HAND_BALANCE
     */
    @SerializedName("前手抵押余额")
    private BigDecimal guarantyFrontHandBalance;

    /**
     * 抵押类型
     * 表字段 : t_guaranty_house_info.GUARANTY_TYPE
     */
    @SerializedName("抵押类型")
    private String guarantyType;

    /**
     * 所有权人姓名
     * 表字段 : t_guaranty_house_info.OWNERSHIP_NAME
     */
    @SerializedName("所有权人姓名")
    private String ownershipName;

    /**
     * 所有权人证件类型 预定义字段：
身份证
护照
户口本
外国人护照
     * 表字段 : t_guaranty_house_info.OWNERSHIP_DOCUMENT_TYPE
     */
    @SerializedName("所有权人证件类型")
    private String ownershipDocumentType;

    /**
     * 所有权人证件号码
     * 表字段 : t_guaranty_house_info.OWNERSHIP_DOCUMENT_NUMBER
     */
    @SerializedName("所有权人证件号码")
    private String ownershipDocumentNumber;

    /**
     * 所有人职业 0
1
3
4
5
6
X
Y
Z
     * 表字段 : t_guaranty_house_info.OWNERSHIP_JOB
     */
    @SerializedName("所有人职业")
    private String ownershipJob;

    /**
     * 押品是否为所有权人/借款人名下唯一住所 
1
2
3
9
     * 表字段 : t_guaranty_house_info.IS_GUARANTY_OWNERSHIP_ONLY_DOMICILE
     */
    @SerializedName("押品是否为所有权人/借款人名下唯一住所")
    private String isGuarantyOwnershipOnlyDomicile;

    /**
     * 房屋建筑面积 平米
     * 表字段 : t_guaranty_house_info.HOUSE_AREA
     */
    @SerializedName("房屋建筑面积")
    private BigDecimal houseArea;

    /**
     * 楼龄
     * 表字段 : t_guaranty_house_info.HOUSE_AGE
     */
    @SerializedName("楼龄")
    private BigDecimal houseAge;

    /**
     * 房屋所在省
     * 表字段 : t_guaranty_house_info.HOUSE_LOCATION_PROVINCE
     */
    @SerializedName("房屋所在省")
    private String houseLocationProvince;

    /**
     * 房屋所在城市
     * 表字段 : t_guaranty_house_info.HOUSE_LOCATION_CITY
     */
    @SerializedName("房屋所在城市")
    private String houseLocationCity;

    /**
     * 房屋所在区县
     * 表字段 : t_guaranty_house_info.HOUSE_LOCATION_DISTRICT_COUNTY
     */
    @SerializedName("房屋所在区县")
    private String houseLocationDistrictCounty;

    /**
     * 房屋地址
     * 表字段 : t_guaranty_house_info.HOUSE_ADDRESS
     */
    @SerializedName("房屋地址")
    private String houseAddress;

    /**
     * 产权年限
     * 表字段 : t_guaranty_house_info.PROPERTY_YEARS
     */
    @SerializedName("产权年限")
    private BigDecimal propertyYears;

    /**
     * 购房合同编号
     * 表字段 : t_guaranty_house_info.PURCHASE_CONTRACT_NUMBER
     */
    @SerializedName("购房合同编号")
    private String purchaseContractNumber;

    /**
     * 权证类型 房产证
房屋他项权证
     * 表字段 : t_guaranty_house_info.WARRANT_TYPE
     */
    @SerializedName("权证类型")
    private String warrantType;

    /**
     * 房产证编号
     * 表字段 : t_guaranty_house_info.PROPERTY_CERTIFICATE_NUMBER
     */
    @SerializedName("房产证编号")
    private String propertyCertificateNumber;

    /**
     * 房屋他项权证编号
     * 表字段 : t_guaranty_house_info.HOUSE_WARRANT_NUMBER
     */
    @SerializedName("房屋他项权证编号")
    private String houseWarrantNumber;

    /**
     * 房屋类别 01
02
03
04
05
06
07
08
09
00
     * 表字段 : t_guaranty_house_info.HOUSE_TYPE
     */
    @SerializedName("房屋类别")
    private String houseType;

    /**
     * 是否有产权共有人 
     * 表字段 : t_guaranty_house_info.IS_PROPERTY_RIGHT_CO_OWNER
     */
    @SerializedName("是否有产权共有人")
    private String isPropertyRightCoOwner;

    /**
     * 产权共有人知情情况 1
2
3
9
     * 表字段 : t_guaranty_house_info.PROPERTY_CO_OWNER_INFORMED_SITUATION
     */
    @SerializedName("产权共有人知情情况")
    private String propertyCoOwnerInformedSituation;

    /**
     * 抵押登记办理 已完成
已递交申请并取得回执
尚未办理
     * 表字段 : t_guaranty_house_info.GUARANTY_REGISTRATION
     */
    @SerializedName("抵押登记办理")
    private String guarantyRegistration;

    /**
     * 强制执行公证 已完成
已递交申请并取得回执
尚未办理
     * 表字段 : t_guaranty_house_info.ENFORCEMENT_NOTARIZATION
     */
    @SerializedName("强制执行公证")
    private String enforcementNotarization;

    /**
     * 网络仲裁办仲裁证明
     * 表字段 : t_guaranty_house_info.IS_ARBITRATION_PROVE
     */
    @SerializedName("网络仲裁办仲裁证明")
    private String isArbitrationProve;

    /**
     * 评估价格-评估公司(元)
     * 表字段 : t_guaranty_house_info.ASSESSMENT_PRICE_EVALUATION_COMPANY
     */
    @SerializedName("评估价格-评估公司(元)")
    private BigDecimal assessmentPriceEvaluationCompany;

    /**
     * 评估价格-房屋中介(元)
     * 表字段 : t_guaranty_house_info.ASSESSMENT_PRICE_LETTING_AGENT
     */
    @SerializedName("评估价格-房屋中介(元)")
    private BigDecimal assessmentPriceLettingAgent;

    /**
     * 评估价格-原始权益日内部评估(元)
     * 表字段 : t_guaranty_house_info.ASSESSMENT_PRICE_ORIGINAL_RIGHTS_DAY
     */
    @SerializedName("评估价格-原始权益日内部评估(元)")
    private BigDecimal assessmentPriceOriginalRightsDay;

    /**
     * 房屋销售价格(元)
     * 表字段 : t_guaranty_house_info.HOUSE_SELLING_PRICE
     */
    @SerializedName("房屋销售价格(元)")
    private BigDecimal houseSellingPrice;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_guaranty_house_info.PROJECT_ID
     *
     * @return t_guaranty_house_info.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_guaranty_house_info.PROJECT_ID
     *
     * @param projectId t_guaranty_house_info.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_guaranty_house_info.AGENCY_ID
     *
     * @return t_guaranty_house_info.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_guaranty_house_info.AGENCY_ID
     *
     * @param agencyId t_guaranty_house_info.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_guaranty_house_info.SERIAL_NUMBER
     *
     * @return t_guaranty_house_info.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_guaranty_house_info.SERIAL_NUMBER
     *
     * @param serialNumber t_guaranty_house_info.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 抵押物编号 字段:t_guaranty_house_info.GUARANTY_NUMBER
     *
     * @return t_guaranty_house_info.GUARANTY_NUMBER, 抵押物编号
     */
    public String getGuarantyNumber() {
        return guarantyNumber;
    }

    /**
     * 设置 抵押物编号 字段:t_guaranty_house_info.GUARANTY_NUMBER
     *
     * @param guarantyNumber t_guaranty_house_info.GUARANTY_NUMBER, 抵押物编号
     */
    public void setGuarantyNumber(String guarantyNumber) {
        this.guarantyNumber = guarantyNumber == null ? null : guarantyNumber.trim();
    }

    /**
     * 获取 抵押物名称 字段:t_guaranty_house_info.GUARANTY_NAME
     *
     * @return t_guaranty_house_info.GUARANTY_NAME, 抵押物名称
     */
    public String getGuarantyName() {
        return guarantyName;
    }

    /**
     * 设置 抵押物名称 字段:t_guaranty_house_info.GUARANTY_NAME
     *
     * @param guarantyName t_guaranty_house_info.GUARANTY_NAME, 抵押物名称
     */
    public void setGuarantyName(String guarantyName) {
        this.guarantyName = guarantyName == null ? null : guarantyName.trim();
    }

    /**
     * 获取 抵押物描述 字段:t_guaranty_house_info.GUARANTY_DESCRIBE
     *
     * @return t_guaranty_house_info.GUARANTY_DESCRIBE, 抵押物描述
     */
    public String getGuarantyDescribe() {
        return guarantyDescribe;
    }

    /**
     * 设置 抵押物描述 字段:t_guaranty_house_info.GUARANTY_DESCRIBE
     *
     * @param guarantyDescribe t_guaranty_house_info.GUARANTY_DESCRIBE, 抵押物描述
     */
    public void setGuarantyDescribe(String guarantyDescribe) {
        this.guarantyDescribe = guarantyDescribe == null ? null : guarantyDescribe.trim();
    }

    /**
     * 获取 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理
 字段:t_guaranty_house_info.GUARANTY_HANDLE_STATUS
     *
     * @return t_guaranty_house_info.GUARANTY_HANDLE_STATUS, 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理

     */
    public String getGuarantyHandleStatus() {
        return guarantyHandleStatus;
    }

    /**
     * 设置 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理
 字段:t_guaranty_house_info.GUARANTY_HANDLE_STATUS
     *
     * @param guarantyHandleStatus t_guaranty_house_info.GUARANTY_HANDLE_STATUS, 抵押办理状态 
预定义字段：
办理中
办理完成
尚未办理

     */
    public void setGuarantyHandleStatus(String guarantyHandleStatus) {
        this.guarantyHandleStatus = guarantyHandleStatus == null ? null : guarantyHandleStatus.trim();
    }

    /**
     * 获取 抵押顺位 
第一顺位
第二顺位
其他 字段:t_guaranty_house_info.GUARANTY_ALIGNMENT
     *
     * @return t_guaranty_house_info.GUARANTY_ALIGNMENT, 抵押顺位 
第一顺位
第二顺位
其他
     */
    public String getGuarantyAlignment() {
        return guarantyAlignment;
    }

    /**
     * 设置 抵押顺位 
第一顺位
第二顺位
其他 字段:t_guaranty_house_info.GUARANTY_ALIGNMENT
     *
     * @param guarantyAlignment t_guaranty_house_info.GUARANTY_ALIGNMENT, 抵押顺位 
第一顺位
第二顺位
其他
     */
    public void setGuarantyAlignment(String guarantyAlignment) {
        this.guarantyAlignment = guarantyAlignment == null ? null : guarantyAlignment.trim();
    }

    /**
     * 获取 前手抵押余额 字段:t_guaranty_house_info.GUARANTY_FRONT_HAND_BALANCE
     *
     * @return t_guaranty_house_info.GUARANTY_FRONT_HAND_BALANCE, 前手抵押余额
     */
    public BigDecimal getGuarantyFrontHandBalance() {
        return guarantyFrontHandBalance;
    }

    /**
     * 设置 前手抵押余额 字段:t_guaranty_house_info.GUARANTY_FRONT_HAND_BALANCE
     *
     * @param guarantyFrontHandBalance t_guaranty_house_info.GUARANTY_FRONT_HAND_BALANCE, 前手抵押余额
     */
    public void setGuarantyFrontHandBalance(BigDecimal guarantyFrontHandBalance) {
        this.guarantyFrontHandBalance = guarantyFrontHandBalance;
    }

    /**
     * 获取 抵押类型 字段:t_guaranty_house_info.GUARANTY_TYPE
     *
     * @return t_guaranty_house_info.GUARANTY_TYPE, 抵押类型
     */
    public String getGuarantyType() {
        return guarantyType;
    }

    /**
     * 设置 抵押类型 字段:t_guaranty_house_info.GUARANTY_TYPE
     *
     * @param guarantyType t_guaranty_house_info.GUARANTY_TYPE, 抵押类型
     */
    public void setGuarantyType(String guarantyType) {
        this.guarantyType = guarantyType == null ? null : guarantyType.trim();
    }

    /**
     * 获取 所有权人姓名 字段:t_guaranty_house_info.OWNERSHIP_NAME
     *
     * @return t_guaranty_house_info.OWNERSHIP_NAME, 所有权人姓名
     */
    public String getOwnershipName() {
        return ownershipName;
    }

    /**
     * 设置 所有权人姓名 字段:t_guaranty_house_info.OWNERSHIP_NAME
     *
     * @param ownershipName t_guaranty_house_info.OWNERSHIP_NAME, 所有权人姓名
     */
    public void setOwnershipName(String ownershipName) {
        this.ownershipName = ownershipName == null ? null : ownershipName.trim();
    }

    /**
     * 获取 所有权人证件类型 预定义字段：
身份证
护照
户口本
外国人护照 字段:t_guaranty_house_info.OWNERSHIP_DOCUMENT_TYPE
     *
     * @return t_guaranty_house_info.OWNERSHIP_DOCUMENT_TYPE, 所有权人证件类型 预定义字段：
身份证
护照
户口本
外国人护照
     */
    public String getOwnershipDocumentType() {
        return ownershipDocumentType;
    }

    /**
     * 设置 所有权人证件类型 预定义字段：
身份证
护照
户口本
外国人护照 字段:t_guaranty_house_info.OWNERSHIP_DOCUMENT_TYPE
     *
     * @param ownershipDocumentType t_guaranty_house_info.OWNERSHIP_DOCUMENT_TYPE, 所有权人证件类型 预定义字段：
身份证
护照
户口本
外国人护照
     */
    public void setOwnershipDocumentType(String ownershipDocumentType) {
        this.ownershipDocumentType = ownershipDocumentType == null ? null : ownershipDocumentType.trim();
    }

    /**
     * 获取 所有权人证件号码 字段:t_guaranty_house_info.OWNERSHIP_DOCUMENT_NUMBER
     *
     * @return t_guaranty_house_info.OWNERSHIP_DOCUMENT_NUMBER, 所有权人证件号码
     */
    public String getOwnershipDocumentNumber() {
        return ownershipDocumentNumber;
    }

    /**
     * 设置 所有权人证件号码 字段:t_guaranty_house_info.OWNERSHIP_DOCUMENT_NUMBER
     *
     * @param ownershipDocumentNumber t_guaranty_house_info.OWNERSHIP_DOCUMENT_NUMBER, 所有权人证件号码
     */
    public void setOwnershipDocumentNumber(String ownershipDocumentNumber) {
        this.ownershipDocumentNumber = ownershipDocumentNumber == null ? null : ownershipDocumentNumber.trim();
    }

    /**
     * 获取 所有人职业 0
1
3
4
5
6
X
Y
Z 字段:t_guaranty_house_info.OWNERSHIP_JOB
     *
     * @return t_guaranty_house_info.OWNERSHIP_JOB, 所有人职业 0
1
3
4
5
6
X
Y
Z
     */
    public String getOwnershipJob() {
        return ownershipJob;
    }

    /**
     * 设置 所有人职业 0
1
3
4
5
6
X
Y
Z 字段:t_guaranty_house_info.OWNERSHIP_JOB
     *
     * @param ownershipJob t_guaranty_house_info.OWNERSHIP_JOB, 所有人职业 0
1
3
4
5
6
X
Y
Z
     */
    public void setOwnershipJob(String ownershipJob) {
        this.ownershipJob = ownershipJob == null ? null : ownershipJob.trim();
    }

    /**
     * 获取 押品是否为所有权人/借款人名下唯一住所 
1
2
3
9 字段:t_guaranty_house_info.IS_GUARANTY_OWNERSHIP_ONLY_DOMICILE
     *
     * @return t_guaranty_house_info.IS_GUARANTY_OWNERSHIP_ONLY_DOMICILE, 押品是否为所有权人/借款人名下唯一住所 
1
2
3
9
     */
    public String getIsGuarantyOwnershipOnlyDomicile() {
        return isGuarantyOwnershipOnlyDomicile;
    }

    /**
     * 设置 押品是否为所有权人/借款人名下唯一住所 
1
2
3
9 字段:t_guaranty_house_info.IS_GUARANTY_OWNERSHIP_ONLY_DOMICILE
     *
     * @param isGuarantyOwnershipOnlyDomicile t_guaranty_house_info.IS_GUARANTY_OWNERSHIP_ONLY_DOMICILE, 押品是否为所有权人/借款人名下唯一住所 
1
2
3
9
     */
    public void setIsGuarantyOwnershipOnlyDomicile(String isGuarantyOwnershipOnlyDomicile) {
        this.isGuarantyOwnershipOnlyDomicile = isGuarantyOwnershipOnlyDomicile == null ? null : isGuarantyOwnershipOnlyDomicile.trim();
    }

    /**
     * 获取 房屋建筑面积 平米 字段:t_guaranty_house_info.HOUSE_AREA
     *
     * @return t_guaranty_house_info.HOUSE_AREA, 房屋建筑面积 平米
     */
    public BigDecimal getHouseArea() {
        return houseArea;
    }

    /**
     * 设置 房屋建筑面积 平米 字段:t_guaranty_house_info.HOUSE_AREA
     *
     * @param houseArea t_guaranty_house_info.HOUSE_AREA, 房屋建筑面积 平米
     */
    public void setHouseArea(BigDecimal houseArea) {
        this.houseArea = houseArea;
    }

    /**
     * 获取 楼龄 字段:t_guaranty_house_info.HOUSE_AGE
     *
     * @return t_guaranty_house_info.HOUSE_AGE, 楼龄
     */
    public BigDecimal getHouseAge() {
        return houseAge;
    }

    /**
     * 设置 楼龄 字段:t_guaranty_house_info.HOUSE_AGE
     *
     * @param houseAge t_guaranty_house_info.HOUSE_AGE, 楼龄
     */
    public void setHouseAge(BigDecimal houseAge) {
        this.houseAge = houseAge;
    }

    /**
     * 获取 房屋所在省 字段:t_guaranty_house_info.HOUSE_LOCATION_PROVINCE
     *
     * @return t_guaranty_house_info.HOUSE_LOCATION_PROVINCE, 房屋所在省
     */
    public String getHouseLocationProvince() {
        return houseLocationProvince;
    }

    /**
     * 设置 房屋所在省 字段:t_guaranty_house_info.HOUSE_LOCATION_PROVINCE
     *
     * @param houseLocationProvince t_guaranty_house_info.HOUSE_LOCATION_PROVINCE, 房屋所在省
     */
    public void setHouseLocationProvince(String houseLocationProvince) {
        this.houseLocationProvince = houseLocationProvince == null ? null : houseLocationProvince.trim();
    }

    /**
     * 获取 房屋所在城市 字段:t_guaranty_house_info.HOUSE_LOCATION_CITY
     *
     * @return t_guaranty_house_info.HOUSE_LOCATION_CITY, 房屋所在城市
     */
    public String getHouseLocationCity() {
        return houseLocationCity;
    }

    /**
     * 设置 房屋所在城市 字段:t_guaranty_house_info.HOUSE_LOCATION_CITY
     *
     * @param houseLocationCity t_guaranty_house_info.HOUSE_LOCATION_CITY, 房屋所在城市
     */
    public void setHouseLocationCity(String houseLocationCity) {
        this.houseLocationCity = houseLocationCity == null ? null : houseLocationCity.trim();
    }

    /**
     * 获取 房屋所在区县 字段:t_guaranty_house_info.HOUSE_LOCATION_DISTRICT_COUNTY
     *
     * @return t_guaranty_house_info.HOUSE_LOCATION_DISTRICT_COUNTY, 房屋所在区县
     */
    public String getHouseLocationDistrictCounty() {
        return houseLocationDistrictCounty;
    }

    /**
     * 设置 房屋所在区县 字段:t_guaranty_house_info.HOUSE_LOCATION_DISTRICT_COUNTY
     *
     * @param houseLocationDistrictCounty t_guaranty_house_info.HOUSE_LOCATION_DISTRICT_COUNTY, 房屋所在区县
     */
    public void setHouseLocationDistrictCounty(String houseLocationDistrictCounty) {
        this.houseLocationDistrictCounty = houseLocationDistrictCounty == null ? null : houseLocationDistrictCounty.trim();
    }

    /**
     * 获取 房屋地址 字段:t_guaranty_house_info.HOUSE_ADDRESS
     *
     * @return t_guaranty_house_info.HOUSE_ADDRESS, 房屋地址
     */
    public String getHouseAddress() {
        return houseAddress;
    }

    /**
     * 设置 房屋地址 字段:t_guaranty_house_info.HOUSE_ADDRESS
     *
     * @param houseAddress t_guaranty_house_info.HOUSE_ADDRESS, 房屋地址
     */
    public void setHouseAddress(String houseAddress) {
        this.houseAddress = houseAddress == null ? null : houseAddress.trim();
    }

    /**
     * 获取 产权年限 字段:t_guaranty_house_info.PROPERTY_YEARS
     *
     * @return t_guaranty_house_info.PROPERTY_YEARS, 产权年限
     */
    public BigDecimal getPropertyYears() {
        return propertyYears;
    }

    /**
     * 设置 产权年限 字段:t_guaranty_house_info.PROPERTY_YEARS
     *
     * @param propertyYears t_guaranty_house_info.PROPERTY_YEARS, 产权年限
     */
    public void setPropertyYears(BigDecimal propertyYears) {
        this.propertyYears = propertyYears;
    }

    /**
     * 获取 购房合同编号 字段:t_guaranty_house_info.PURCHASE_CONTRACT_NUMBER
     *
     * @return t_guaranty_house_info.PURCHASE_CONTRACT_NUMBER, 购房合同编号
     */
    public String getPurchaseContractNumber() {
        return purchaseContractNumber;
    }

    /**
     * 设置 购房合同编号 字段:t_guaranty_house_info.PURCHASE_CONTRACT_NUMBER
     *
     * @param purchaseContractNumber t_guaranty_house_info.PURCHASE_CONTRACT_NUMBER, 购房合同编号
     */
    public void setPurchaseContractNumber(String purchaseContractNumber) {
        this.purchaseContractNumber = purchaseContractNumber == null ? null : purchaseContractNumber.trim();
    }

    /**
     * 获取 权证类型 房产证
房屋他项权证 字段:t_guaranty_house_info.WARRANT_TYPE
     *
     * @return t_guaranty_house_info.WARRANT_TYPE, 权证类型 房产证
房屋他项权证
     */
    public String getWarrantType() {
        return warrantType;
    }

    /**
     * 设置 权证类型 房产证
房屋他项权证 字段:t_guaranty_house_info.WARRANT_TYPE
     *
     * @param warrantType t_guaranty_house_info.WARRANT_TYPE, 权证类型 房产证
房屋他项权证
     */
    public void setWarrantType(String warrantType) {
        this.warrantType = warrantType == null ? null : warrantType.trim();
    }

    /**
     * 获取 房产证编号 字段:t_guaranty_house_info.PROPERTY_CERTIFICATE_NUMBER
     *
     * @return t_guaranty_house_info.PROPERTY_CERTIFICATE_NUMBER, 房产证编号
     */
    public String getPropertyCertificateNumber() {
        return propertyCertificateNumber;
    }

    /**
     * 设置 房产证编号 字段:t_guaranty_house_info.PROPERTY_CERTIFICATE_NUMBER
     *
     * @param propertyCertificateNumber t_guaranty_house_info.PROPERTY_CERTIFICATE_NUMBER, 房产证编号
     */
    public void setPropertyCertificateNumber(String propertyCertificateNumber) {
        this.propertyCertificateNumber = propertyCertificateNumber == null ? null : propertyCertificateNumber.trim();
    }

    /**
     * 获取 房屋他项权证编号 字段:t_guaranty_house_info.HOUSE_WARRANT_NUMBER
     *
     * @return t_guaranty_house_info.HOUSE_WARRANT_NUMBER, 房屋他项权证编号
     */
    public String getHouseWarrantNumber() {
        return houseWarrantNumber;
    }

    /**
     * 设置 房屋他项权证编号 字段:t_guaranty_house_info.HOUSE_WARRANT_NUMBER
     *
     * @param houseWarrantNumber t_guaranty_house_info.HOUSE_WARRANT_NUMBER, 房屋他项权证编号
     */
    public void setHouseWarrantNumber(String houseWarrantNumber) {
        this.houseWarrantNumber = houseWarrantNumber == null ? null : houseWarrantNumber.trim();
    }

    /**
     * 获取 房屋类别 01
02
03
04
05
06
07
08
09
00 字段:t_guaranty_house_info.HOUSE_TYPE
     *
     * @return t_guaranty_house_info.HOUSE_TYPE, 房屋类别 01
02
03
04
05
06
07
08
09
00
     */
    public String getHouseType() {
        return houseType;
    }

    /**
     * 设置 房屋类别 01
02
03
04
05
06
07
08
09
00 字段:t_guaranty_house_info.HOUSE_TYPE
     *
     * @param houseType t_guaranty_house_info.HOUSE_TYPE, 房屋类别 01
02
03
04
05
06
07
08
09
00
     */
    public void setHouseType(String houseType) {
        this.houseType = houseType == null ? null : houseType.trim();
    }

    /**
     * 获取 是否有产权共有人  字段:t_guaranty_house_info.IS_PROPERTY_RIGHT_CO_OWNER
     *
     * @return t_guaranty_house_info.IS_PROPERTY_RIGHT_CO_OWNER, 是否有产权共有人 
     */
    public String getIsPropertyRightCoOwner() {
        return isPropertyRightCoOwner;
    }

    /**
     * 设置 是否有产权共有人  字段:t_guaranty_house_info.IS_PROPERTY_RIGHT_CO_OWNER
     *
     * @param isPropertyRightCoOwner t_guaranty_house_info.IS_PROPERTY_RIGHT_CO_OWNER, 是否有产权共有人 
     */
    public void setIsPropertyRightCoOwner(String isPropertyRightCoOwner) {
        this.isPropertyRightCoOwner = isPropertyRightCoOwner == null ? null : isPropertyRightCoOwner.trim();
    }

    /**
     * 获取 产权共有人知情情况 1
2
3
9 字段:t_guaranty_house_info.PROPERTY_CO_OWNER_INFORMED_SITUATION
     *
     * @return t_guaranty_house_info.PROPERTY_CO_OWNER_INFORMED_SITUATION, 产权共有人知情情况 1
2
3
9
     */
    public String getPropertyCoOwnerInformedSituation() {
        return propertyCoOwnerInformedSituation;
    }

    /**
     * 设置 产权共有人知情情况 1
2
3
9 字段:t_guaranty_house_info.PROPERTY_CO_OWNER_INFORMED_SITUATION
     *
     * @param propertyCoOwnerInformedSituation t_guaranty_house_info.PROPERTY_CO_OWNER_INFORMED_SITUATION, 产权共有人知情情况 1
2
3
9
     */
    public void setPropertyCoOwnerInformedSituation(String propertyCoOwnerInformedSituation) {
        this.propertyCoOwnerInformedSituation = propertyCoOwnerInformedSituation == null ? null : propertyCoOwnerInformedSituation.trim();
    }

    /**
     * 获取 抵押登记办理 已完成
已递交申请并取得回执
尚未办理 字段:t_guaranty_house_info.GUARANTY_REGISTRATION
     *
     * @return t_guaranty_house_info.GUARANTY_REGISTRATION, 抵押登记办理 已完成
已递交申请并取得回执
尚未办理
     */
    public String getGuarantyRegistration() {
        return guarantyRegistration;
    }

    /**
     * 设置 抵押登记办理 已完成
已递交申请并取得回执
尚未办理 字段:t_guaranty_house_info.GUARANTY_REGISTRATION
     *
     * @param guarantyRegistration t_guaranty_house_info.GUARANTY_REGISTRATION, 抵押登记办理 已完成
已递交申请并取得回执
尚未办理
     */
    public void setGuarantyRegistration(String guarantyRegistration) {
        this.guarantyRegistration = guarantyRegistration == null ? null : guarantyRegistration.trim();
    }

    /**
     * 获取 强制执行公证 已完成
已递交申请并取得回执
尚未办理 字段:t_guaranty_house_info.ENFORCEMENT_NOTARIZATION
     *
     * @return t_guaranty_house_info.ENFORCEMENT_NOTARIZATION, 强制执行公证 已完成
已递交申请并取得回执
尚未办理
     */
    public String getEnforcementNotarization() {
        return enforcementNotarization;
    }

    /**
     * 设置 强制执行公证 已完成
已递交申请并取得回执
尚未办理 字段:t_guaranty_house_info.ENFORCEMENT_NOTARIZATION
     *
     * @param enforcementNotarization t_guaranty_house_info.ENFORCEMENT_NOTARIZATION, 强制执行公证 已完成
已递交申请并取得回执
尚未办理
     */
    public void setEnforcementNotarization(String enforcementNotarization) {
        this.enforcementNotarization = enforcementNotarization == null ? null : enforcementNotarization.trim();
    }

    /**
     * 获取 网络仲裁办仲裁证明 字段:t_guaranty_house_info.IS_ARBITRATION_PROVE
     *
     * @return t_guaranty_house_info.IS_ARBITRATION_PROVE, 网络仲裁办仲裁证明
     */
    public String getIsArbitrationProve() {
        return isArbitrationProve;
    }

    /**
     * 设置 网络仲裁办仲裁证明 字段:t_guaranty_house_info.IS_ARBITRATION_PROVE
     *
     * @param isArbitrationProve t_guaranty_house_info.IS_ARBITRATION_PROVE, 网络仲裁办仲裁证明
     */
    public void setIsArbitrationProve(String isArbitrationProve) {
        this.isArbitrationProve = isArbitrationProve == null ? null : isArbitrationProve.trim();
    }

    /**
     * 获取 评估价格-评估公司(元) 字段:t_guaranty_house_info.ASSESSMENT_PRICE_EVALUATION_COMPANY
     *
     * @return t_guaranty_house_info.ASSESSMENT_PRICE_EVALUATION_COMPANY, 评估价格-评估公司(元)
     */
    public BigDecimal getAssessmentPriceEvaluationCompany() {
        return assessmentPriceEvaluationCompany;
    }

    /**
     * 设置 评估价格-评估公司(元) 字段:t_guaranty_house_info.ASSESSMENT_PRICE_EVALUATION_COMPANY
     *
     * @param assessmentPriceEvaluationCompany t_guaranty_house_info.ASSESSMENT_PRICE_EVALUATION_COMPANY, 评估价格-评估公司(元)
     */
    public void setAssessmentPriceEvaluationCompany(BigDecimal assessmentPriceEvaluationCompany) {
        this.assessmentPriceEvaluationCompany = assessmentPriceEvaluationCompany;
    }

    /**
     * 获取 评估价格-房屋中介(元) 字段:t_guaranty_house_info.ASSESSMENT_PRICE_LETTING_AGENT
     *
     * @return t_guaranty_house_info.ASSESSMENT_PRICE_LETTING_AGENT, 评估价格-房屋中介(元)
     */
    public BigDecimal getAssessmentPriceLettingAgent() {
        return assessmentPriceLettingAgent;
    }

    /**
     * 设置 评估价格-房屋中介(元) 字段:t_guaranty_house_info.ASSESSMENT_PRICE_LETTING_AGENT
     *
     * @param assessmentPriceLettingAgent t_guaranty_house_info.ASSESSMENT_PRICE_LETTING_AGENT, 评估价格-房屋中介(元)
     */
    public void setAssessmentPriceLettingAgent(BigDecimal assessmentPriceLettingAgent) {
        this.assessmentPriceLettingAgent = assessmentPriceLettingAgent;
    }

    /**
     * 获取 评估价格-原始权益日内部评估(元) 字段:t_guaranty_house_info.ASSESSMENT_PRICE_ORIGINAL_RIGHTS_DAY
     *
     * @return t_guaranty_house_info.ASSESSMENT_PRICE_ORIGINAL_RIGHTS_DAY, 评估价格-原始权益日内部评估(元)
     */
    public BigDecimal getAssessmentPriceOriginalRightsDay() {
        return assessmentPriceOriginalRightsDay;
    }

    /**
     * 设置 评估价格-原始权益日内部评估(元) 字段:t_guaranty_house_info.ASSESSMENT_PRICE_ORIGINAL_RIGHTS_DAY
     *
     * @param assessmentPriceOriginalRightsDay t_guaranty_house_info.ASSESSMENT_PRICE_ORIGINAL_RIGHTS_DAY, 评估价格-原始权益日内部评估(元)
     */
    public void setAssessmentPriceOriginalRightsDay(BigDecimal assessmentPriceOriginalRightsDay) {
        this.assessmentPriceOriginalRightsDay = assessmentPriceOriginalRightsDay;
    }

    /**
     * 获取 房屋销售价格(元) 字段:t_guaranty_house_info.HOUSE_SELLING_PRICE
     *
     * @return t_guaranty_house_info.HOUSE_SELLING_PRICE, 房屋销售价格(元)
     */
    public BigDecimal getHouseSellingPrice() {
        return houseSellingPrice;
    }

    /**
     * 设置 房屋销售价格(元) 字段:t_guaranty_house_info.HOUSE_SELLING_PRICE
     *
     * @param houseSellingPrice t_guaranty_house_info.HOUSE_SELLING_PRICE, 房屋销售价格(元)
     */
    public void setHouseSellingPrice(BigDecimal houseSellingPrice) {
        this.houseSellingPrice = houseSellingPrice;
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
        return "GuarantyHouseInfo{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", guarantyNumber='" + guarantyNumber + '\'' +
                ", guarantyName='" + guarantyName + '\'' +
                ", guarantyDescribe='" + guarantyDescribe + '\'' +
                ", guarantyHandleStatus='" + guarantyHandleStatus + '\'' +
                ", guarantyAlignment='" + guarantyAlignment + '\'' +
                ", guarantyFrontHandBalance=" + guarantyFrontHandBalance +
                ", guarantyType='" + guarantyType + '\'' +
                ", ownershipName='" + ownershipName + '\'' +
                ", ownershipDocumentType='" + ownershipDocumentType + '\'' +
                ", ownershipDocumentNumber='" + ownershipDocumentNumber + '\'' +
                ", ownershipJob='" + ownershipJob + '\'' +
                ", isGuarantyOwnershipOnlyDomicile='" + isGuarantyOwnershipOnlyDomicile + '\'' +
                ", houseArea=" + houseArea +
                ", houseAge=" + houseAge +
                ", houseLocationProvince='" + houseLocationProvince + '\'' +
                ", houseLocationCity='" + houseLocationCity + '\'' +
                ", houseLocationDistrictCounty='" + houseLocationDistrictCounty + '\'' +
                ", houseAddress='" + houseAddress + '\'' +
                ", propertyYears=" + propertyYears +
                ", purchaseContractNumber='" + purchaseContractNumber + '\'' +
                ", warrantType='" + warrantType + '\'' +
                ", propertyCertificateNumber='" + propertyCertificateNumber + '\'' +
                ", houseWarrantNumber='" + houseWarrantNumber + '\'' +
                ", houseType='" + houseType + '\'' +
                ", isPropertyRightCoOwner='" + isPropertyRightCoOwner + '\'' +
                ", propertyCoOwnerInformedSituation='" + propertyCoOwnerInformedSituation + '\'' +
                ", guarantyRegistration='" + guarantyRegistration + '\'' +
                ", enforcementNotarization='" + enforcementNotarization + '\'' +
                ", isArbitrationProve='" + isArbitrationProve + '\'' +
                ", assessmentPriceEvaluationCompany=" + assessmentPriceEvaluationCompany +
                ", assessmentPriceLettingAgent=" + assessmentPriceLettingAgent +
                ", assessmentPriceOriginalRightsDay=" + assessmentPriceOriginalRightsDay +
                ", houseSellingPrice=" + houseSellingPrice +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}