package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

/**
 *  企业名称信息  File12
 **/
public class EnterpriseInfo extends BaseEntity {

    /**
     * 项目编号
     * 表字段 : t_enterprise_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_enterprise_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_enterprise_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 合同角色 
预定义字段:
主借款企业
共同借款企业
担保企业
无
     * 表字段 : t_enterprise_info.CONTRACT_ROLE
     */
    @SerializedName("合同角色")
    private String contractRole;

    /**
     * 企业姓名
     * 表字段 : t_enterprise_info.ENTERPRISE_NAME
     */
    @SerializedName("企业姓名")
    private String enterpriseName;

    /**
     * 工商注册号
     * 表字段 : t_enterprise_info.REGISTRATION_NUMBER
     */
    @SerializedName("工商注册号")
    private String registrationNumber;

    /**
     * 组织机构代码
     * 表字段 : t_enterprise_info.ORGANIZATION_CODE
     */
    @SerializedName("组织机构代码")
    private String organizationCode;

    /**
     * 纳税人识别号
     * 表字段 : t_enterprise_info.TAXPAYER_IDENTIFICATION_NUMBER
     */
    @SerializedName("纳税人识别号")
    private String taxpayerIdentificationNumber;

    /**
     * 统一信用代码
     * 表字段 : t_enterprise_info.UNIFORM_CREDIT_CODE
     */
    @SerializedName("统一信用代码")
    private String uniformCreditCode;

    /**
     * 注册地址
     * 表字段 : t_enterprise_info.REGISTERED_ADDRESS
     */
    @SerializedName("注册地址")
    private String registeredAddress;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_enterprise_info.PROJECT_ID
     *
     * @return t_enterprise_info.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_enterprise_info.PROJECT_ID
     *
     * @param projectId t_enterprise_info.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_enterprise_info.AGENCY_ID
     *
     * @return t_enterprise_info.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_enterprise_info.AGENCY_ID
     *
     * @param agencyId t_enterprise_info.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_enterprise_info.SERIAL_NUMBER
     *
     * @return t_enterprise_info.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_enterprise_info.SERIAL_NUMBER
     *
     * @param serialNumber t_enterprise_info.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 合同角色 
预定义字段:
主借款企业
共同借款企业
担保企业
无 字段:t_enterprise_info.CONTRACT_ROLE
     *
     * @return t_enterprise_info.CONTRACT_ROLE, 合同角色 
预定义字段:
主借款企业
共同借款企业
担保企业
无
     */
    public String getContractRole() {
        return contractRole;
    }

    /**
     * 设置 合同角色 
预定义字段:
主借款企业
共同借款企业
担保企业
无 字段:t_enterprise_info.CONTRACT_ROLE
     *
     * @param contractRole t_enterprise_info.CONTRACT_ROLE, 合同角色 
预定义字段:
主借款企业
共同借款企业
担保企业
无
     */
    public void setContractRole(String contractRole) {
        this.contractRole = contractRole == null ? null : contractRole.trim();
    }

    /**
     * 获取 企业姓名 字段:t_enterprise_info.ENTERPRISE_NAME
     *
     * @return t_enterprise_info.ENTERPRISE_NAME, 企业姓名
     */
    public String getEnterpriseName() {
        return enterpriseName;
    }

    /**
     * 设置 企业姓名 字段:t_enterprise_info.ENTERPRISE_NAME
     *
     * @param enterpriseName t_enterprise_info.ENTERPRISE_NAME, 企业姓名
     */
    public void setEnterpriseName(String enterpriseName) {
        this.enterpriseName = enterpriseName == null ? null : enterpriseName.trim();
    }

    /**
     * 获取 工商注册号 字段:t_enterprise_info.REGISTRATION_NUMBER
     *
     * @return t_enterprise_info.REGISTRATION_NUMBER, 工商注册号
     */
    public String getRegistrationNumber() {
        return registrationNumber;
    }

    /**
     * 设置 工商注册号 字段:t_enterprise_info.REGISTRATION_NUMBER
     *
     * @param registrationNumber t_enterprise_info.REGISTRATION_NUMBER, 工商注册号
     */
    public void setRegistrationNumber(String registrationNumber) {
        this.registrationNumber = registrationNumber == null ? null : registrationNumber.trim();
    }

    /**
     * 获取 组织机构代码 字段:t_enterprise_info.ORGANIZATION_CODE
     *
     * @return t_enterprise_info.ORGANIZATION_CODE, 组织机构代码
     */
    public String getOrganizationCode() {
        return organizationCode;
    }

    /**
     * 设置 组织机构代码 字段:t_enterprise_info.ORGANIZATION_CODE
     *
     * @param organizationCode t_enterprise_info.ORGANIZATION_CODE, 组织机构代码
     */
    public void setOrganizationCode(String organizationCode) {
        this.organizationCode = organizationCode == null ? null : organizationCode.trim();
    }

    /**
     * 获取 纳税人识别号 字段:t_enterprise_info.TAXPAYER_IDENTIFICATION_NUMBER
     *
     * @return t_enterprise_info.TAXPAYER_IDENTIFICATION_NUMBER, 纳税人识别号
     */
    public String getTaxpayerIdentificationNumber() {
        return taxpayerIdentificationNumber;
    }

    /**
     * 设置 纳税人识别号 字段:t_enterprise_info.TAXPAYER_IDENTIFICATION_NUMBER
     *
     * @param taxpayerIdentificationNumber t_enterprise_info.TAXPAYER_IDENTIFICATION_NUMBER, 纳税人识别号
     */
    public void setTaxpayerIdentificationNumber(String taxpayerIdentificationNumber) {
        this.taxpayerIdentificationNumber = taxpayerIdentificationNumber == null ? null : taxpayerIdentificationNumber.trim();
    }

    /**
     * 获取 统一信用代码 字段:t_enterprise_info.UNIFORM_CREDIT_CODE
     *
     * @return t_enterprise_info.UNIFORM_CREDIT_CODE, 统一信用代码
     */
    public String getUniformCreditCode() {
        return uniformCreditCode;
    }

    /**
     * 设置 统一信用代码 字段:t_enterprise_info.UNIFORM_CREDIT_CODE
     *
     * @param uniformCreditCode t_enterprise_info.UNIFORM_CREDIT_CODE, 统一信用代码
     */
    public void setUniformCreditCode(String uniformCreditCode) {
        this.uniformCreditCode = uniformCreditCode == null ? null : uniformCreditCode.trim();
    }

    /**
     * 获取 注册地址 字段:t_enterprise_info.REGISTERED_ADDRESS
     *
     * @return t_enterprise_info.REGISTERED_ADDRESS, 注册地址
     */
    public String getRegisteredAddress() {
        return registeredAddress;
    }

    /**
     * 设置 注册地址 字段:t_enterprise_info.REGISTERED_ADDRESS
     *
     * @param registeredAddress t_enterprise_info.REGISTERED_ADDRESS, 注册地址
     */
    public void setRegisteredAddress(String registeredAddress) {
        this.registeredAddress = registeredAddress == null ? null : registeredAddress.trim();
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
        return "EnterpriseInfo{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", contractRole='" + contractRole + '\'' +
                ", enterpriseName='" + enterpriseName + '\'' +
                ", registrationNumber='" + registrationNumber + '\'' +
                ", organizationCode='" + organizationCode + '\'' +
                ", taxpayerIdentificationNumber='" + taxpayerIdentificationNumber + '\'' +
                ", uniformCreditCode='" + uniformCreditCode + '\'' +
                ", registeredAddress='" + registeredAddress + '\'' +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}