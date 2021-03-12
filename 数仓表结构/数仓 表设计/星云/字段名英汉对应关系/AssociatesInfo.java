package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.math.BigDecimal;

/**
 *  关联人信息  File03
 **/
public class AssociatesInfo extends BaseEntity {
    /**
     * 项目编号
     * 表字段 : t_associates_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_associates_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_associates_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 合同角色-枚举
预定义字段:
共同借款人,
担保人,
无
     * 表字段 : t_associates_info.CONTRACT_ROLE
     */
    @SerializedName("合同角色")
    private String contractRole;

    /**
     * 客户姓名
     * 表字段 : t_associates_info.BORROWER_NAME
     */
    @SerializedName("客户姓名")
    private String borrowerName;

    /**
     * 证件类型-预定义字段：
身份证
护照
户口本
外国人护照
     * 表字段 : t_associates_info.CERTIFICATE_TYPE
     */
    @SerializedName("证件类型")
    private String certificateType;

    /**
     * 身份证号
     * 表字段 : t_associates_info.DOCUMENT_NUM
     */
    @SerializedName("身份证号")
    private String documentNum;

    /**
     * 手机号
     * 表字段 : t_associates_info.PHONE_NUM
     */
    @SerializedName("手机号")
    private String phoneNum;

    /**
     * 年龄
     * 表字段 : t_associates_info.AGE
     */
    @SerializedName("年龄")
    private Integer age;

    /**
     * 性别
预定义字段：
男，
女

     * 表字段 : t_associates_info.SEX
     */
    @SerializedName("性别")
    private String sex;

    /**
     * 与借款人关系
预定义字段：
配偶
父母
子女
亲戚
朋友
同事
     * 表字段 : t_associates_info.RELATIONSHIP_WITH_BORROWERS
     */
    @SerializedName("与主借款人关系")
    private String relationshipWithBorrowers;

    /**
     * 职业
     * 表字段 : t_associates_info.CAREER
     */
    @SerializedName("职业")
    private String career;

    /**
     * 工作状态
预定义字段：
在职
失业
     * 表字段 : t_associates_info.WORKING_STATE
     */
    @SerializedName("工作状态")
    private String workingState;

    /**
     * 年收入(元)
     * 表字段 : t_associates_info.ANNUAL_INCOME
     */
    @SerializedName("年收入(元)")
    private BigDecimal annualIncome;

    /**
     * 通讯地址
     * 表字段 : t_associates_info.MAILING_ADDRESS
     */
    @SerializedName("通讯地址")
    private String mailingAddress;

    /**
     * 单位详细地址
     * 表字段 : t_associates_info.UNIT_ADDRESS
     */
    @SerializedName("单位地址")
    private String unitAddress;

    /**
     * 单位联系方式
     * 表字段 : t_associates_info.UNIT_CONTACT_MODE
     */
    @SerializedName("单位联系方式")
    private String unitContactMode;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_associates_info.PROJECT_ID
     *
     * @return t_associates_info.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_associates_info.PROJECT_ID
     *
     * @param projectId t_associates_info.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_associates_info.AGENCY_ID
     *
     * @return t_associates_info.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_associates_info.AGENCY_ID
     *
     * @param agencyId t_associates_info.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_associates_info.SERIAL_NUMBER
     *
     * @return t_associates_info.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_associates_info.SERIAL_NUMBER
     *
     * @param serialNumber t_associates_info.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 合同角色-枚举
预定义字段:
共同借款人,
担保人,
无 字段:t_associates_info.CONTRACT_ROLE
     *
     * @return t_associates_info.CONTRACT_ROLE, 合同角色-枚举
预定义字段:
共同借款人,
担保人,
无
     */
    public String getContractRole() {
        return contractRole;
    }

    /**
     * 设置 合同角色-枚举
预定义字段:
共同借款人,
担保人,
无 字段:t_associates_info.CONTRACT_ROLE
     *
     * @param contractRole t_associates_info.CONTRACT_ROLE, 合同角色-枚举
预定义字段:
共同借款人,
担保人,
无
     */
    public void setContractRole(String contractRole) {
        this.contractRole = contractRole == null ? null : contractRole.trim();
    }

    /**
     * 获取 客户姓名 字段:t_associates_info.BORROWER_NAME
     *
     * @return t_associates_info.BORROWER_NAME, 客户姓名
     */
    public String getBorrowerName() {
        return borrowerName;
    }

    /**
     * 设置 客户姓名 字段:t_associates_info.BORROWER_NAME
     *
     * @param borrowerName t_associates_info.BORROWER_NAME, 客户姓名
     */
    public void setBorrowerName(String borrowerName) {
        this.borrowerName = borrowerName == null ? null : borrowerName.trim();
    }

    /**
     * 获取 证件类型-预定义字段：
身份证
护照
户口本
外国人护照 字段:t_associates_info.CERTIFICATE_TYPE
     *
     * @return t_associates_info.CERTIFICATE_TYPE, 证件类型-预定义字段：
身份证
护照
户口本
外国人护照
     */
    public String getCertificateType() {
        return certificateType;
    }

    /**
     * 设置 证件类型-预定义字段：
身份证
护照
户口本
外国人护照 字段:t_associates_info.CERTIFICATE_TYPE
     *
     * @param certificateType t_associates_info.CERTIFICATE_TYPE, 证件类型-预定义字段：
身份证
护照
户口本
外国人护照
     */
    public void setCertificateType(String certificateType) {
        this.certificateType = certificateType == null ? null : certificateType.trim();
    }

    /**
     * 获取 身份证号 字段:t_associates_info.DOCUMENT_NUM
     *
     * @return t_associates_info.DOCUMENT_NUM, 身份证号
     */
    public String getDocumentNum() {
        return documentNum;
    }

    /**
     * 设置 身份证号 字段:t_associates_info.DOCUMENT_NUM
     *
     * @param documentNum t_associates_info.DOCUMENT_NUM, 身份证号
     */
    public void setDocumentNum(String documentNum) {
        this.documentNum = documentNum == null ? null : documentNum.trim();
    }

    /**
     * 获取 手机号 字段:t_associates_info.PHONE_NUM
     *
     * @return t_associates_info.PHONE_NUM, 手机号
     */
    public String getPhoneNum() {
        return phoneNum;
    }

    /**
     * 设置 手机号 字段:t_associates_info.PHONE_NUM
     *
     * @param phoneNum t_associates_info.PHONE_NUM, 手机号
     */
    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum == null ? null : phoneNum.trim();
    }

    /**
     * 获取 年龄 字段:t_associates_info.AGE
     *
     * @return t_associates_info.AGE, 年龄
     */
    public Integer getAge() {
        return age;
    }

    /**
     * 设置 年龄 字段:t_associates_info.AGE
     *
     * @param age t_associates_info.AGE, 年龄
     */
    public void setAge(Integer age) {
        this.age = age;
    }

    /**
     * 获取 性别
预定义字段：
男，
女
 字段:t_associates_info.SEX
     *
     * @return t_associates_info.SEX, 性别
预定义字段：
男，
女

     */
    public String getSex() {
        return sex;
    }

    /**
     * 设置 性别
预定义字段：
男，
女
 字段:t_associates_info.SEX
     *
     * @param sex t_associates_info.SEX, 性别
预定义字段：
男，
女

     */
    public void setSex(String sex) {
        this.sex = sex == null ? null : sex.trim();
    }

    /**
     * 获取 与借款人关系
预定义字段：
配偶
父母
子女
亲戚
朋友
同事 字段:t_associates_info.RELATIONSHIP_WITH_BORROWERS
     *
     * @return t_associates_info.RELATIONSHIP_WITH_BORROWERS, 与借款人关系
预定义字段：
配偶
父母
子女
亲戚
朋友
同事
     */
    public String getRelationshipWithBorrowers() {
        return relationshipWithBorrowers;
    }

    /**
     * 设置 与借款人关系
预定义字段：
配偶
父母
子女
亲戚
朋友
同事 字段:t_associates_info.RELATIONSHIP_WITH_BORROWERS
     *
     * @param relationshipWithBorrowers t_associates_info.RELATIONSHIP_WITH_BORROWERS, 与借款人关系
预定义字段：
配偶
父母
子女
亲戚
朋友
同事
     */
    public void setRelationshipWithBorrowers(String relationshipWithBorrowers) {
        this.relationshipWithBorrowers = relationshipWithBorrowers == null ? null : relationshipWithBorrowers.trim();
    }

    /**
     * 获取 职业 字段:t_associates_info.CAREER
     *
     * @return t_associates_info.CAREER, 职业
     */
    public String getCareer() {
        return career;
    }

    /**
     * 设置 职业 字段:t_associates_info.CAREER
     *
     * @param career t_associates_info.CAREER, 职业
     */
    public void setCareer(String career) {
        this.career = career == null ? null : career.trim();
    }

    /**
     * 获取 工作状态
预定义字段：
在职
失业 字段:t_associates_info.WORKING_STATE
     *
     * @return t_associates_info.WORKING_STATE, 工作状态
预定义字段：
在职
失业
     */
    public String getWorkingState() {
        return workingState;
    }

    /**
     * 设置 工作状态
预定义字段：
在职
失业 字段:t_associates_info.WORKING_STATE
     *
     * @param workingState t_associates_info.WORKING_STATE, 工作状态
预定义字段：
在职
失业
     */
    public void setWorkingState(String workingState) {
        this.workingState = workingState == null ? null : workingState.trim();
    }

    /**
     * 获取 年收入(元) 字段:t_associates_info.ANNUAL_INCOME
     *
     * @return t_associates_info.ANNUAL_INCOME, 年收入(元)
     */
    public BigDecimal getAnnualIncome() {
        return annualIncome;
    }

    /**
     * 设置 年收入(元) 字段:t_associates_info.ANNUAL_INCOME
     *
     * @param annualIncome t_associates_info.ANNUAL_INCOME, 年收入(元)
     */
    public void setAnnualIncome(BigDecimal annualIncome) {
        this.annualIncome = annualIncome;
    }

    /**
     * 获取 通讯地址 字段:t_associates_info.MAILING_ADDRESS
     *
     * @return t_associates_info.MAILING_ADDRESS, 通讯地址
     */
    public String getMailingAddress() {
        return mailingAddress;
    }

    /**
     * 设置 通讯地址 字段:t_associates_info.MAILING_ADDRESS
     *
     * @param mailingAddress t_associates_info.MAILING_ADDRESS, 通讯地址
     */
    public void setMailingAddress(String mailingAddress) {
        this.mailingAddress = mailingAddress == null ? null : mailingAddress.trim();
    }

    /**
     * 获取 单位详细地址 字段:t_associates_info.UNIT_ADDRESS
     *
     * @return t_associates_info.UNIT_ADDRESS, 单位详细地址
     */
    public String getUnitAddress() {
        return unitAddress;
    }

    /**
     * 设置 单位详细地址 字段:t_associates_info.UNIT_ADDRESS
     *
     * @param unitAddress t_associates_info.UNIT_ADDRESS, 单位详细地址
     */
    public void setUnitAddress(String unitAddress) {
        this.unitAddress = unitAddress == null ? null : unitAddress.trim();
    }

    /**
     * 获取 单位联系方式 字段:t_associates_info.UNIT_CONTACT_MODE
     *
     * @return t_associates_info.UNIT_CONTACT_MODE, 单位联系方式
     */
    public String getUnitContactMode() {
        return unitContactMode;
    }

    /**
     * 设置 单位联系方式 字段:t_associates_info.UNIT_CONTACT_MODE
     *
     * @param unitContactMode t_associates_info.UNIT_CONTACT_MODE, 单位联系方式
     */
    public void setUnitContactMode(String unitContactMode) {
        this.unitContactMode = unitContactMode == null ? null : unitContactMode.trim();
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
        return "AssociatesInfo{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", contractRole='" + contractRole + '\'' +
                ", borrowerName='" + borrowerName + '\'' +
                ", certificateType='" + certificateType + '\'' +
                ", documentNum='" + documentNum + '\'' +
                ", phoneNum='" + phoneNum + '\'' +
                ", age=" + age +
                ", sex='" + sex + '\'' +
                ", relationshipWithBorrowers='" + relationshipWithBorrowers + '\'' +
                ", career='" + career + '\'' +
                ", workingState='" + workingState + '\'' +
                ", annualIncome=" + annualIncome +
                ", mailingAddress='" + mailingAddress + '\'' +
                ", unitAddress='" + unitAddress + '\'' +
                ", unitContactMode='" + unitContactMode + '\'' +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}