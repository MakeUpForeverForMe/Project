package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.math.BigDecimal;

/**
 *  主借款人信息  File02
 **/
public class BorrowerInfo extends BaseEntity {

    /**
     * 项目编号
     * 表字段 : t_borrower_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_borrower_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_borrower_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 客户姓名
     * 表字段 : t_borrower_info.BORROWER_NAME
     */
    @SerializedName("客户姓名")
    private String borrowerName;

    /**
     * 证件类型 
预定义字段：
身份证
护照
户口本
外国人护照
     * 表字段 : t_borrower_info.CERTIFICATE_TYPE
     */
    @SerializedName("证件类型")
    private String certificateType;

    /**
     * 身份证号
     * 表字段 : t_borrower_info.DOCUMENT_NUM
     */
    @SerializedName("身份证号")
    private String documentNum;

    /**
     * 手机号
     * 表字段 : t_borrower_info.PHONE_NUM
     */
    @SerializedName("手机号")
    private String phoneNum;

    /**
     * 年龄
     * 表字段 : t_borrower_info.AGE
     */
    @SerializedName("年龄")
    private Integer age;

    /**
     * 性别-枚举 
男，
女

     * 表字段 : t_borrower_info.SEX
     */
    @SerializedName("性别")
    private String sex;

    /**
     * 婚姻状况-枚举
预定义字段：
已婚，
未婚，
离异，
丧偶

     * 表字段 : t_borrower_info.MARITAL_STATUS
     */
    @SerializedName("婚姻状况")
    private String maritalStatus;

    /**
     * 子女数量
     * 表字段 : t_borrower_info.CHILDREN_NUMBER
     */
    @SerializedName("子女数量")
    private Integer childrenNumber;

    /**
     * 工作年限
     * 表字段 : t_borrower_info.WORK_YEARS
     */
    @SerializedName("工作年限")
    private String workYears;

    /**
     * 客户类型-枚举
"01-农户
02-工薪
03-个体工商户
04-学生
99-其他"

     * 表字段 : t_borrower_info.CUSTOMER_TYPE
     */
    @SerializedName("客户类型")
    private String customerType;

    /**
     * 学历-枚举
预定义字段：
小学，
初中，
高中/职高/技校，
大专，
本科,
硕士,
博士，
文盲和半文盲

     * 表字段 : t_borrower_info.EDUCATION_LEVEL
     */
    @SerializedName("学历")
    private String educationLevel;

    /**
     * 学位
     * 表字段 : t_borrower_info.DEGREE
     */
    @SerializedName("学位")
    private String degree;

    /**
     * 是否具有完全民事行为能力
"预定义字段：
是
否"

     * 表字段 : t_borrower_info.IS_CAPACITY_CIVIL_CONDUCT
     */
    @SerializedName("是否具有完全民事行为能力")
    private String isCapacityCivilConduct;

    /**
     * 居住状态
     * 表字段 : t_borrower_info.LIVING_STATE
     */
    @SerializedName("居住状态")
    private String livingState;

    /**
     * 客户所在省 
传输省份城市代码则提供对应关系
     * 表字段 : t_borrower_info.PROVINCE
     */
    @SerializedName("客户居住所在省")
    private String province;

    /**
     * 客户所在市 传输省份城市代码则提供对应关系
     * 表字段 : t_borrower_info.CITY
     */
    @SerializedName("客户居住所在市")
    private String city;

    /**
     * 客户居住地址
     * 表字段 : t_borrower_info.ADDRESS
     */
    @SerializedName("客户居住地址")
    private String address;

    /**
     * 客户户籍所在省
     * 表字段 : t_borrower_info.HOUSE_PROVINCE
     */
    @SerializedName("客户户籍所在省")
    private String houseProvince;

    /**
     * 客户户籍所在市
     * 表字段 : t_borrower_info.HOUSE_CITY
     */
    @SerializedName("客户户籍所在市")
    private String houseCity;

    /**
     * 客户户籍地址
     * 表字段 : t_borrower_info.HOUSE_ADDRESS
     */
    @SerializedName("客户户籍地址")
    private String houseAddress;

    /**
     * 通讯邮编
     * 表字段 : t_borrower_info.COMMUNICATIONS_ZIP_CODE
     */
    @SerializedName("通讯邮编")
    private String communicationsZipCode;

    /**
     * 客户通讯地址
     * 表字段 : t_borrower_info.MAILING_ADDRESS
     */
    @SerializedName("客户通讯地址")
    private String mailingAddress;

    /**
     * 客户职业
     * 表字段 : t_borrower_info.CAREER
     */
    @SerializedName("客户职业")
    private String career;

    /**
     * 工作状态
"预定义字段：
在职
，失业"

     * 表字段 : t_borrower_info.WORKING_STATE
     */
    @SerializedName("工作状态")
    private String workingState;

    /**
     * 职务
     * 表字段 : t_borrower_info.POSITION
     */
    @SerializedName("职务")
    private String position;

    /**
     * 职称
     * 表字段 : t_borrower_info.TITLE
     */
    @SerializedName("职称")
    private String title;

    /**
     * 借款人行业-枚举
借款人行业
NIL--空
A--农、林、牧、渔业
B--采矿业
C--制造业
D--电力、热力、燃气及水生产和供应业
E--建筑业
F--批发和零售业
G--交通运输、仓储和邮政业
H--住宿和餐饮业
I--信息传输、软件和信息技术服务业
J--金融业
K--房地产业
L--租赁和商务服务业
M--科学研究和技术服务业
N--水利、环境和公共设施管理业
O--居民服务、修理和其他服务业
P--教育
Q--卫生和社会工作
R--文化、体育和娱乐业
S--公共管理、社会保障和社会组织
T--国际组织
Z--其他

     * 表字段 : t_borrower_info.BORROWER_INDUSTRY
     */
    @SerializedName("借款人行业")
    private String borrowerIndustry;

    /**
     * 是否有车
"预定义字段:
是
否"

     * 表字段 : t_borrower_info.IS_CAR
     */
    @SerializedName("是否有车")
    private String isCar;

    /**
     * 是否有按揭车贷
"预定义字段:
是
否"

     * 表字段 : t_borrower_info.IS_MORTGAGE_FINANCING
     */
    @SerializedName("是否有按揭车贷")
    private String isMortgageFinancing;

    /**
     * 是否有房
"预定义字段:
是
否"

     * 表字段 : t_borrower_info.IS_HOUSE
     */
    @SerializedName("是否有房")
    private String isHouse;

    /**
     * 是否有按揭房贷
"预定义字段:
是
否"

     * 表字段 : t_borrower_info.IS_MORTGAGE_LOANS
     */
    @SerializedName("是否有按揭房贷")
    private String isMortgageLoans;

    /**
     * 是否有信用卡
"预定义字段:
是
否"

     * 表字段 : t_borrower_info.IS_CREDIT_CARD
     */
    @SerializedName("是否有信用卡")
    private String isCreditCard;

    /**
     * 信用卡额度
     * 表字段 : t_borrower_info.CREDIT_LIMIT
     */
    @SerializedName("信用卡额度")
    private BigDecimal creditLimit;

    /**
     * 年收入(元)
     * 表字段 : t_borrower_info.ANNUAL_INCOME
     */
    @SerializedName("年收入(元)")
    private BigDecimal annualIncome;

    /**
     * 内部信用等级
     * 表字段 : t_borrower_info.INTERNAL_CREDIT_RATING
     */
    @SerializedName("内部信用等级")
    private String internalCreditRating;

    /**
     * 黑名单等级
     * 表字段 : t_borrower_info.BLACKLIST_LEVEL
     */
    @SerializedName("黑名单等级")
    private String blacklistLevel;

    /**
     * 单位名称
     * 表字段 : t_borrower_info.UNIT_NAME
     */
    @SerializedName("单位名称")
    private String unitName;

    /**
     * 固定电话
     * 表字段 : t_borrower_info.FIXED_TELEPHONE
     */
    @SerializedName("固定电话")
    private String fixedTelephone;

    /**
     * 邮编
     * 表字段 : t_borrower_info.ZIP_CODE
     */
    @SerializedName("邮编")
    private String zipCode;

    /**
     * 单位详细地址
     * 表字段 : t_borrower_info.UNIT_ADDRESS
     */
    @SerializedName("单位详细地址")
    private String unitAddress;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_borrower_info.PROJECT_ID
     *
     * @return t_borrower_info.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_borrower_info.PROJECT_ID
     *
     * @param projectId t_borrower_info.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_borrower_info.AGENCY_ID
     *
     * @return t_borrower_info.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_borrower_info.AGENCY_ID
     *
     * @param agencyId t_borrower_info.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_borrower_info.SERIAL_NUMBER
     *
     * @return t_borrower_info.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_borrower_info.SERIAL_NUMBER
     *
     * @param serialNumber t_borrower_info.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 客户姓名 字段:t_borrower_info.BORROWER_NAME
     *
     * @return t_borrower_info.BORROWER_NAME, 客户姓名
     */
    public String getBorrowerName() {
        return borrowerName;
    }

    /**
     * 设置 客户姓名 字段:t_borrower_info.BORROWER_NAME
     *
     * @param borrowerName t_borrower_info.BORROWER_NAME, 客户姓名
     */
    public void setBorrowerName(String borrowerName) {
        this.borrowerName = borrowerName == null ? null : borrowerName.trim();
    }

    /**
     * 获取 证件类型 
预定义字段：
身份证
护照
户口本
外国人护照 字段:t_borrower_info.CERTIFICATE_TYPE
     *
     * @return t_borrower_info.CERTIFICATE_TYPE, 证件类型 
预定义字段：
身份证
护照
户口本
外国人护照
     */
    public String getCertificateType() {
        return certificateType;
    }

    /**
     * 设置 证件类型 
预定义字段：
身份证
护照
户口本
外国人护照 字段:t_borrower_info.CERTIFICATE_TYPE
     *
     * @param certificateType t_borrower_info.CERTIFICATE_TYPE, 证件类型 
预定义字段：
身份证
护照
户口本
外国人护照
     */
    public void setCertificateType(String certificateType) {
        this.certificateType = certificateType == null ? null : certificateType.trim();
    }

    /**
     * 获取 身份证号 字段:t_borrower_info.DOCUMENT_NUM
     *
     * @return t_borrower_info.DOCUMENT_NUM, 身份证号
     */
    public String getDocumentNum() {
        return documentNum;
    }

    /**
     * 设置 身份证号 字段:t_borrower_info.DOCUMENT_NUM
     *
     * @param documentNum t_borrower_info.DOCUMENT_NUM, 身份证号
     */
    public void setDocumentNum(String documentNum) {
        this.documentNum = documentNum == null ? null : documentNum.trim();
    }

    /**
     * 获取 手机号 字段:t_borrower_info.PHONE_NUM
     *
     * @return t_borrower_info.PHONE_NUM, 手机号
     */
    public String getPhoneNum() {
        return phoneNum;
    }

    /**
     * 设置 手机号 字段:t_borrower_info.PHONE_NUM
     *
     * @param phoneNum t_borrower_info.PHONE_NUM, 手机号
     */
    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum == null ? null : phoneNum.trim();
    }

    /**
     * 获取 年龄 字段:t_borrower_info.AGE
     *
     * @return t_borrower_info.AGE, 年龄
     */
    public Integer getAge() {
        return age;
    }

    /**
     * 设置 年龄 字段:t_borrower_info.AGE
     *
     * @param age t_borrower_info.AGE, 年龄
     */
    public void setAge(Integer age) {
        this.age = age;
    }

    /**
     * 获取 性别-枚举 
男，
女
 字段:t_borrower_info.SEX
     *
     * @return t_borrower_info.SEX, 性别-枚举 
男，
女

     */
    public String getSex() {
        return sex;
    }

    /**
     * 设置 性别-枚举 
男，
女
 字段:t_borrower_info.SEX
     *
     * @param sex t_borrower_info.SEX, 性别-枚举 
男，
女

     */
    public void setSex(String sex) {
        this.sex = sex == null ? null : sex.trim();
    }

    /**
     * 获取 婚姻状况-枚举
预定义字段：
已婚，
未婚，
离异，
丧偶
 字段:t_borrower_info.MARITAL_STATUS
     *
     * @return t_borrower_info.MARITAL_STATUS, 婚姻状况-枚举
预定义字段：
已婚，
未婚，
离异，
丧偶

     */
    public String getMaritalStatus() {
        return maritalStatus;
    }

    /**
     * 设置 婚姻状况-枚举
预定义字段：
已婚，
未婚，
离异，
丧偶
 字段:t_borrower_info.MARITAL_STATUS
     *
     * @param maritalStatus t_borrower_info.MARITAL_STATUS, 婚姻状况-枚举
预定义字段：
已婚，
未婚，
离异，
丧偶

     */
    public void setMaritalStatus(String maritalStatus) {
        this.maritalStatus = maritalStatus == null ? null : maritalStatus.trim();
    }

    /**
     * 获取 子女数量 字段:t_borrower_info.CHILDREN_NUMBER
     *
     * @return t_borrower_info.CHILDREN_NUMBER, 子女数量
     */
    public Integer getChildrenNumber() {
        return childrenNumber;
    }

    /**
     * 设置 子女数量 字段:t_borrower_info.CHILDREN_NUMBER
     *
     * @param childrenNumber t_borrower_info.CHILDREN_NUMBER, 子女数量
     */
    public void setChildrenNumber(Integer childrenNumber) {
        this.childrenNumber = childrenNumber;
    }

    /**
     * 获取 工作年限 字段:t_borrower_info.WORK_YEARS
     *
     * @return t_borrower_info.WORK_YEARS, 工作年限
     */
    public String getWorkYears() {
        return workYears;
    }

    /**
     * 设置 工作年限 字段:t_borrower_info.WORK_YEARS
     *
     * @param workYears t_borrower_info.WORK_YEARS, 工作年限
     */
    public void setWorkYears(String workYears) {
        this.workYears = workYears == null ? null : workYears.trim();
    }

    /**
     * 获取 客户类型-枚举
"01-农户
02-工薪
03-个体工商户
04-学生
99-其他"
 字段:t_borrower_info.CUSTOMER_TYPE
     *
     * @return t_borrower_info.CUSTOMER_TYPE, 客户类型-枚举
"01-农户
02-工薪
03-个体工商户
04-学生
99-其他"

     */
    public String getCustomerType() {
        return customerType;
    }

    /**
     * 设置 客户类型-枚举
"01-农户
02-工薪
03-个体工商户
04-学生
99-其他"
 字段:t_borrower_info.CUSTOMER_TYPE
     *
     * @param customerType t_borrower_info.CUSTOMER_TYPE, 客户类型-枚举
"01-农户
02-工薪
03-个体工商户
04-学生
99-其他"

     */
    public void setCustomerType(String customerType) {
        this.customerType = customerType == null ? null : customerType.trim();
    }

    /**
     * 获取 学历-枚举
预定义字段：
小学，
初中，
高中/职高/技校，
大专，
本科,
硕士,
博士，
文盲和半文盲
 字段:t_borrower_info.EDUCATION_LEVEL
     *
     * @return t_borrower_info.EDUCATION_LEVEL, 学历-枚举
预定义字段：
小学，
初中，
高中/职高/技校，
大专，
本科,
硕士,
博士，
文盲和半文盲

     */
    public String getEducationLevel() {
        return educationLevel;
    }

    /**
     * 设置 学历-枚举
预定义字段：
小学，
初中，
高中/职高/技校，
大专，
本科,
硕士,
博士，
文盲和半文盲
 字段:t_borrower_info.EDUCATION_LEVEL
     *
     * @param educationLevel t_borrower_info.EDUCATION_LEVEL, 学历-枚举
预定义字段：
小学，
初中，
高中/职高/技校，
大专，
本科,
硕士,
博士，
文盲和半文盲

     */
    public void setEducationLevel(String educationLevel) {
        this.educationLevel = educationLevel == null ? null : educationLevel.trim();
    }

    /**
     * 获取 学位 字段:t_borrower_info.DEGREE
     *
     * @return t_borrower_info.DEGREE, 学位
     */
    public String getDegree() {
        return degree;
    }

    /**
     * 设置 学位 字段:t_borrower_info.DEGREE
     *
     * @param degree t_borrower_info.DEGREE, 学位
     */
    public void setDegree(String degree) {
        this.degree = degree == null ? null : degree.trim();
    }

    /**
     * 获取 是否具有完全民事行为能力
"预定义字段：
是
否"
 字段:t_borrower_info.IS_CAPACITY_CIVIL_CONDUCT
     *
     * @return t_borrower_info.IS_CAPACITY_CIVIL_CONDUCT, 是否具有完全民事行为能力
"预定义字段：
是
否"

     */
    public String getIsCapacityCivilConduct() {
        return isCapacityCivilConduct;
    }

    /**
     * 设置 是否具有完全民事行为能力
"预定义字段：
是
否"
 字段:t_borrower_info.IS_CAPACITY_CIVIL_CONDUCT
     *
     * @param isCapacityCivilConduct t_borrower_info.IS_CAPACITY_CIVIL_CONDUCT, 是否具有完全民事行为能力
"预定义字段：
是
否"

     */
    public void setIsCapacityCivilConduct(String isCapacityCivilConduct) {
        this.isCapacityCivilConduct = isCapacityCivilConduct == null ? null : isCapacityCivilConduct.trim();
    }

    /**
     * 获取 居住状态 字段:t_borrower_info.LIVING_STATE
     *
     * @return t_borrower_info.LIVING_STATE, 居住状态
     */
    public String getLivingState() {
        return livingState;
    }

    /**
     * 设置 居住状态 字段:t_borrower_info.LIVING_STATE
     *
     * @param livingState t_borrower_info.LIVING_STATE, 居住状态
     */
    public void setLivingState(String livingState) {
        this.livingState = livingState == null ? null : livingState.trim();
    }

    /**
     * 获取 客户所在省 
传输省份城市代码则提供对应关系 字段:t_borrower_info.PROVINCE
     *
     * @return t_borrower_info.PROVINCE, 客户所在省 
传输省份城市代码则提供对应关系
     */
    public String getProvince() {
        return province;
    }

    /**
     * 设置 客户所在省 
传输省份城市代码则提供对应关系 字段:t_borrower_info.PROVINCE
     *
     * @param province t_borrower_info.PROVINCE, 客户所在省 
传输省份城市代码则提供对应关系
     */
    public void setProvince(String province) {
        this.province = province == null ? null : province.trim();
    }

    /**
     * 获取 客户所在市 传输省份城市代码则提供对应关系 字段:t_borrower_info.CITY
     *
     * @return t_borrower_info.CITY, 客户所在市 传输省份城市代码则提供对应关系
     */
    public String getCity() {
        return city;
    }

    /**
     * 设置 客户所在市 传输省份城市代码则提供对应关系 字段:t_borrower_info.CITY
     *
     * @param city t_borrower_info.CITY, 客户所在市 传输省份城市代码则提供对应关系
     */
    public void setCity(String city) {
        this.city = city == null ? null : city.trim();
    }

    /**
     * 获取 客户居住地址 字段:t_borrower_info.ADDRESS
     *
     * @return t_borrower_info.ADDRESS, 客户居住地址
     */
    public String getAddress() {
        return address;
    }

    /**
     * 设置 客户居住地址 字段:t_borrower_info.ADDRESS
     *
     * @param address t_borrower_info.ADDRESS, 客户居住地址
     */
    public void setAddress(String address) {
        this.address = address == null ? null : address.trim();
    }

    /**
     * 获取 客户户籍所在省 字段:t_borrower_info.HOUSE_PROVINCE
     *
     * @return t_borrower_info.HOUSE_PROVINCE, 客户户籍所在省
     */
    public String getHouseProvince() {
        return houseProvince;
    }

    /**
     * 设置 客户户籍所在省 字段:t_borrower_info.HOUSE_PROVINCE
     *
     * @param houseProvince t_borrower_info.HOUSE_PROVINCE, 客户户籍所在省
     */
    public void setHouseProvince(String houseProvince) {
        this.houseProvince = houseProvince == null ? null : houseProvince.trim();
    }

    /**
     * 获取 客户户籍所在市 字段:t_borrower_info.HOUSE_CITY
     *
     * @return t_borrower_info.HOUSE_CITY, 客户户籍所在市
     */
    public String getHouseCity() {
        return houseCity;
    }

    /**
     * 设置 客户户籍所在市 字段:t_borrower_info.HOUSE_CITY
     *
     * @param houseCity t_borrower_info.HOUSE_CITY, 客户户籍所在市
     */
    public void setHouseCity(String houseCity) {
        this.houseCity = houseCity == null ? null : houseCity.trim();
    }

    /**
     * 获取 客户户籍地址 字段:t_borrower_info.HOUSE_ADDRESS
     *
     * @return t_borrower_info.HOUSE_ADDRESS, 客户户籍地址
     */
    public String getHouseAddress() {
        return houseAddress;
    }

    /**
     * 设置 客户户籍地址 字段:t_borrower_info.HOUSE_ADDRESS
     *
     * @param houseAddress t_borrower_info.HOUSE_ADDRESS, 客户户籍地址
     */
    public void setHouseAddress(String houseAddress) {
        this.houseAddress = houseAddress == null ? null : houseAddress.trim();
    }

    /**
     * 获取 通讯邮编 字段:t_borrower_info.COMMUNICATIONS_ZIP_CODE
     *
     * @return t_borrower_info.COMMUNICATIONS_ZIP_CODE, 通讯邮编
     */
    public String getCommunicationsZipCode() {
        return communicationsZipCode;
    }

    /**
     * 设置 通讯邮编 字段:t_borrower_info.COMMUNICATIONS_ZIP_CODE
     *
     * @param communicationsZipCode t_borrower_info.COMMUNICATIONS_ZIP_CODE, 通讯邮编
     */
    public void setCommunicationsZipCode(String communicationsZipCode) {
        this.communicationsZipCode = communicationsZipCode == null ? null : communicationsZipCode.trim();
    }

    /**
     * 获取 客户通讯地址 字段:t_borrower_info.MAILING_ADDRESS
     *
     * @return t_borrower_info.MAILING_ADDRESS, 客户通讯地址
     */
    public String getMailingAddress() {
        return mailingAddress;
    }

    /**
     * 设置 客户通讯地址 字段:t_borrower_info.MAILING_ADDRESS
     *
     * @param mailingAddress t_borrower_info.MAILING_ADDRESS, 客户通讯地址
     */
    public void setMailingAddress(String mailingAddress) {
        this.mailingAddress = mailingAddress == null ? null : mailingAddress.trim();
    }

    /**
     * 获取 客户职业 字段:t_borrower_info.CAREER
     *
     * @return t_borrower_info.CAREER, 客户职业
     */
    public String getCareer() {
        return career;
    }

    /**
     * 设置 客户职业 字段:t_borrower_info.CAREER
     *
     * @param career t_borrower_info.CAREER, 客户职业
     */
    public void setCareer(String career) {
        this.career = career == null ? null : career.trim();
    }

    /**
     * 获取 工作状态
"预定义字段：
在职
，失业"
 字段:t_borrower_info.WORKING_STATE
     *
     * @return t_borrower_info.WORKING_STATE, 工作状态
"预定义字段：
在职
，失业"

     */
    public String getWorkingState() {
        return workingState;
    }

    /**
     * 设置 工作状态
"预定义字段：
在职
，失业"
 字段:t_borrower_info.WORKING_STATE
     *
     * @param workingState t_borrower_info.WORKING_STATE, 工作状态
"预定义字段：
在职
，失业"

     */
    public void setWorkingState(String workingState) {
        this.workingState = workingState == null ? null : workingState.trim();
    }

    /**
     * 获取 职务 字段:t_borrower_info.POSITION
     *
     * @return t_borrower_info.POSITION, 职务
     */
    public String getPosition() {
        return position;
    }

    /**
     * 设置 职务 字段:t_borrower_info.POSITION
     *
     * @param position t_borrower_info.POSITION, 职务
     */
    public void setPosition(String position) {
        this.position = position == null ? null : position.trim();
    }

    /**
     * 获取 职称 字段:t_borrower_info.TITLE
     *
     * @return t_borrower_info.TITLE, 职称
     */
    public String getTitle() {
        return title;
    }

    /**
     * 设置 职称 字段:t_borrower_info.TITLE
     *
     * @param title t_borrower_info.TITLE, 职称
     */
    public void setTitle(String title) {
        this.title = title == null ? null : title.trim();
    }

    /**
     * 获取 借款人行业-枚举
借款人行业
NIL--空
A--农、林、牧、渔业
B--采矿业
C--制造业
D--电力、热力、燃气及水生产和供应业
E--建筑业
F--批发和零售业
G--交通运输、仓储和邮政业
H--住宿和餐饮业
I--信息传输、软件和信息技术服务业
J--金融业
K--房地产业
L--租赁和商务服务业
M--科学研究和技术服务业
N--水利、环境和公共设施管理业
O--居民服务、修理和其他服务业
P--教育
Q--卫生和社会工作
R--文化、体育和娱乐业
S--公共管理、社会保障和社会组织
T--国际组织
Z--其他
 字段:t_borrower_info.BORROWER_INDUSTRY
     *
     * @return t_borrower_info.BORROWER_INDUSTRY, 借款人行业-枚举
借款人行业
NIL--空
A--农、林、牧、渔业
B--采矿业
C--制造业
D--电力、热力、燃气及水生产和供应业
E--建筑业
F--批发和零售业
G--交通运输、仓储和邮政业
H--住宿和餐饮业
I--信息传输、软件和信息技术服务业
J--金融业
K--房地产业
L--租赁和商务服务业
M--科学研究和技术服务业
N--水利、环境和公共设施管理业
O--居民服务、修理和其他服务业
P--教育
Q--卫生和社会工作
R--文化、体育和娱乐业
S--公共管理、社会保障和社会组织
T--国际组织
Z--其他

     */
    public String getBorrowerIndustry() {
        return borrowerIndustry;
    }

    /**
     * 设置 借款人行业-枚举
借款人行业
NIL--空
A--农、林、牧、渔业
B--采矿业
C--制造业
D--电力、热力、燃气及水生产和供应业
E--建筑业
F--批发和零售业
G--交通运输、仓储和邮政业
H--住宿和餐饮业
I--信息传输、软件和信息技术服务业
J--金融业
K--房地产业
L--租赁和商务服务业
M--科学研究和技术服务业
N--水利、环境和公共设施管理业
O--居民服务、修理和其他服务业
P--教育
Q--卫生和社会工作
R--文化、体育和娱乐业
S--公共管理、社会保障和社会组织
T--国际组织
Z--其他
 字段:t_borrower_info.BORROWER_INDUSTRY
     *
     * @param borrowerIndustry t_borrower_info.BORROWER_INDUSTRY, 借款人行业-枚举
借款人行业
NIL--空
A--农、林、牧、渔业
B--采矿业
C--制造业
D--电力、热力、燃气及水生产和供应业
E--建筑业
F--批发和零售业
G--交通运输、仓储和邮政业
H--住宿和餐饮业
I--信息传输、软件和信息技术服务业
J--金融业
K--房地产业
L--租赁和商务服务业
M--科学研究和技术服务业
N--水利、环境和公共设施管理业
O--居民服务、修理和其他服务业
P--教育
Q--卫生和社会工作
R--文化、体育和娱乐业
S--公共管理、社会保障和社会组织
T--国际组织
Z--其他

     */
    public void setBorrowerIndustry(String borrowerIndustry) {
        this.borrowerIndustry = borrowerIndustry == null ? null : borrowerIndustry.trim();
    }

    /**
     * 获取 是否有车
"预定义字段:
是
否"
 字段:t_borrower_info.IS_CAR
     *
     * @return t_borrower_info.IS_CAR, 是否有车
"预定义字段:
是
否"

     */
    public String getIsCar() {
        return isCar;
    }

    /**
     * 设置 是否有车
"预定义字段:
是
否"
 字段:t_borrower_info.IS_CAR
     *
     * @param isCar t_borrower_info.IS_CAR, 是否有车
"预定义字段:
是
否"

     */
    public void setIsCar(String isCar) {
        this.isCar = isCar == null ? null : isCar.trim();
    }

    /**
     * 获取 是否有按揭车贷
"预定义字段:
是
否"
 字段:t_borrower_info.IS_MORTGAGE_FINANCING
     *
     * @return t_borrower_info.IS_MORTGAGE_FINANCING, 是否有按揭车贷
"预定义字段:
是
否"

     */
    public String getIsMortgageFinancing() {
        return isMortgageFinancing;
    }

    /**
     * 设置 是否有按揭车贷
"预定义字段:
是
否"
 字段:t_borrower_info.IS_MORTGAGE_FINANCING
     *
     * @param isMortgageFinancing t_borrower_info.IS_MORTGAGE_FINANCING, 是否有按揭车贷
"预定义字段:
是
否"

     */
    public void setIsMortgageFinancing(String isMortgageFinancing) {
        this.isMortgageFinancing = isMortgageFinancing == null ? null : isMortgageFinancing.trim();
    }

    /**
     * 获取 是否有房
"预定义字段:
是
否"
 字段:t_borrower_info.IS_HOUSE
     *
     * @return t_borrower_info.IS_HOUSE, 是否有房
"预定义字段:
是
否"

     */
    public String getIsHouse() {
        return isHouse;
    }

    /**
     * 设置 是否有房
"预定义字段:
是
否"
 字段:t_borrower_info.IS_HOUSE
     *
     * @param isHouse t_borrower_info.IS_HOUSE, 是否有房
"预定义字段:
是
否"

     */
    public void setIsHouse(String isHouse) {
        this.isHouse = isHouse == null ? null : isHouse.trim();
    }

    /**
     * 获取 是否有按揭房贷
"预定义字段:
是
否"
 字段:t_borrower_info.IS_MORTGAGE_LOANS
     *
     * @return t_borrower_info.IS_MORTGAGE_LOANS, 是否有按揭房贷
"预定义字段:
是
否"

     */
    public String getIsMortgageLoans() {
        return isMortgageLoans;
    }

    /**
     * 设置 是否有按揭房贷
"预定义字段:
是
否"
 字段:t_borrower_info.IS_MORTGAGE_LOANS
     *
     * @param isMortgageLoans t_borrower_info.IS_MORTGAGE_LOANS, 是否有按揭房贷
"预定义字段:
是
否"

     */
    public void setIsMortgageLoans(String isMortgageLoans) {
        this.isMortgageLoans = isMortgageLoans == null ? null : isMortgageLoans.trim();
    }

    /**
     * 获取 是否有信用卡
"预定义字段:
是
否"
 字段:t_borrower_info.IS_CREDIT_CARD
     *
     * @return t_borrower_info.IS_CREDIT_CARD, 是否有信用卡
"预定义字段:
是
否"

     */
    public String getIsCreditCard() {
        return isCreditCard;
    }

    /**
     * 设置 是否有信用卡
"预定义字段:
是
否"
 字段:t_borrower_info.IS_CREDIT_CARD
     *
     * @param isCreditCard t_borrower_info.IS_CREDIT_CARD, 是否有信用卡
"预定义字段:
是
否"

     */
    public void setIsCreditCard(String isCreditCard) {
        this.isCreditCard = isCreditCard == null ? null : isCreditCard.trim();
    }

    /**
     * 获取 信用卡额度 字段:t_borrower_info.CREDIT_LIMIT
     *
     * @return t_borrower_info.CREDIT_LIMIT, 信用卡额度
     */
    public BigDecimal getCreditLimit() {
        return creditLimit;
    }

    /**
     * 设置 信用卡额度 字段:t_borrower_info.CREDIT_LIMIT
     *
     * @param creditLimit t_borrower_info.CREDIT_LIMIT, 信用卡额度
     */
    public void setCreditLimit(BigDecimal creditLimit) {
        this.creditLimit = creditLimit;
    }

    /**
     * 获取 年收入(元) 字段:t_borrower_info.ANNUAL_INCOME
     *
     * @return t_borrower_info.ANNUAL_INCOME, 年收入(元)
     */
    public BigDecimal getAnnualIncome() {
        return annualIncome;
    }

    /**
     * 设置 年收入(元) 字段:t_borrower_info.ANNUAL_INCOME
     *
     * @param annualIncome t_borrower_info.ANNUAL_INCOME, 年收入(元)
     */
    public void setAnnualIncome(BigDecimal annualIncome) {
        this.annualIncome = annualIncome;
    }

    /**
     * 获取 内部信用等级 字段:t_borrower_info.INTERNAL_CREDIT_RATING
     *
     * @return t_borrower_info.INTERNAL_CREDIT_RATING, 内部信用等级
     */
    public String getInternalCreditRating() {
        return internalCreditRating;
    }

    /**
     * 设置 内部信用等级 字段:t_borrower_info.INTERNAL_CREDIT_RATING
     *
     * @param internalCreditRating t_borrower_info.INTERNAL_CREDIT_RATING, 内部信用等级
     */
    public void setInternalCreditRating(String internalCreditRating) {
        this.internalCreditRating = internalCreditRating == null ? null : internalCreditRating.trim();
    }

    /**
     * 获取 黑名单等级 字段:t_borrower_info.BLACKLIST_LEVEL
     *
     * @return t_borrower_info.BLACKLIST_LEVEL, 黑名单等级
     */
    public String getBlacklistLevel() {
        return blacklistLevel;
    }

    /**
     * 设置 黑名单等级 字段:t_borrower_info.BLACKLIST_LEVEL
     *
     * @param blacklistLevel t_borrower_info.BLACKLIST_LEVEL, 黑名单等级
     */
    public void setBlacklistLevel(String blacklistLevel) {
        this.blacklistLevel = blacklistLevel == null ? null : blacklistLevel.trim();
    }

    /**
     * 获取 单位名称 字段:t_borrower_info.UNIT_NAME
     *
     * @return t_borrower_info.UNIT_NAME, 单位名称
     */
    public String getUnitName() {
        return unitName;
    }

    /**
     * 设置 单位名称 字段:t_borrower_info.UNIT_NAME
     *
     * @param unitName t_borrower_info.UNIT_NAME, 单位名称
     */
    public void setUnitName(String unitName) {
        this.unitName = unitName == null ? null : unitName.trim();
    }

    /**
     * 获取 固定电话 字段:t_borrower_info.FIXED_TELEPHONE
     *
     * @return t_borrower_info.FIXED_TELEPHONE, 固定电话
     */
    public String getFixedTelephone() {
        return fixedTelephone;
    }

    /**
     * 设置 固定电话 字段:t_borrower_info.FIXED_TELEPHONE
     *
     * @param fixedTelephone t_borrower_info.FIXED_TELEPHONE, 固定电话
     */
    public void setFixedTelephone(String fixedTelephone) {
        this.fixedTelephone = fixedTelephone == null ? null : fixedTelephone.trim();
    }

    /**
     * 获取 邮编 字段:t_borrower_info.ZIP_CODE
     *
     * @return t_borrower_info.ZIP_CODE, 邮编
     */
    public String getZipCode() {
        return zipCode;
    }

    /**
     * 设置 邮编 字段:t_borrower_info.ZIP_CODE
     *
     * @param zipCode t_borrower_info.ZIP_CODE, 邮编
     */
    public void setZipCode(String zipCode) {
        this.zipCode = zipCode == null ? null : zipCode.trim();
    }

    /**
     * 获取 单位详细地址 字段:t_borrower_info.UNIT_ADDRESS
     *
     * @return t_borrower_info.UNIT_ADDRESS, 单位详细地址
     */
    public String getUnitAddress() {
        return unitAddress;
    }

    /**
     * 设置 单位详细地址 字段:t_borrower_info.UNIT_ADDRESS
     *
     * @param unitAddress t_borrower_info.UNIT_ADDRESS, 单位详细地址
     */
    public void setUnitAddress(String unitAddress) {
        this.unitAddress = unitAddress == null ? null : unitAddress.trim();
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
        return "BorrowerInfo{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", borrowerName='" + borrowerName + '\'' +
                ", certificateType='" + certificateType + '\'' +
                ", documentNum='" + documentNum + '\'' +
                ", phoneNum='" + phoneNum + '\'' +
                ", age=" + age +
                ", sex='" + sex + '\'' +
                ", maritalStatus='" + maritalStatus + '\'' +
                ", childrenNumber=" + childrenNumber +
                ", workYears='" + workYears + '\'' +
                ", customerType='" + customerType + '\'' +
                ", educationLevel='" + educationLevel + '\'' +
                ", degree='" + degree + '\'' +
                ", isCapacityCivilConduct='" + isCapacityCivilConduct + '\'' +
                ", livingState='" + livingState + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", address='" + address + '\'' +
                ", houseProvince='" + houseProvince + '\'' +
                ", houseCity='" + houseCity + '\'' +
                ", houseAddress='" + houseAddress + '\'' +
                ", communicationsZipCode='" + communicationsZipCode + '\'' +
                ", mailingAddress='" + mailingAddress + '\'' +
                ", career='" + career + '\'' +
                ", workingState='" + workingState + '\'' +
                ", position='" + position + '\'' +
                ", title='" + title + '\'' +
                ", borrowerIndustry='" + borrowerIndustry + '\'' +
                ", isCar='" + isCar + '\'' +
                ", isMortgageFinancing='" + isMortgageFinancing + '\'' +
                ", isHouse='" + isHouse + '\'' +
                ", isMortgageLoans='" + isMortgageLoans + '\'' +
                ", isCreditCard='" + isCreditCard + '\'' +
                ", creditLimit=" + creditLimit +
                ", annualIncome=" + annualIncome +
                ", internalCreditRating='" + internalCreditRating + '\'' +
                ", blacklistLevel='" + blacklistLevel + '\'' +
                ", unitName='" + unitName + '\'' +
                ", fixedTelephone='" + fixedTelephone + '\'' +
                ", zipCode='" + zipCode + '\'' +
                ", unitAddress='" + unitAddress + '\'' +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}