package com.weshare.abscore.db.po.asset;

import com.google.gson.annotations.SerializedName;
import com.weshare.abscore.db.po.asset.base.BaseEntity;

import java.util.Date;


/**
 *  资产处置过程信息  File08
 **/
public class AssetDealProcessInfo extends BaseEntity {
    /**
     * 项目编号
     * 表字段 : t_asset_deal_process_info.PROJECT_ID
     */
    @SerializedName("项目编号")
    private String projectId;

    /**
     * 机构编号
     * 表字段 : t_asset_deal_process_info.AGENCY_ID
     */
    @SerializedName("机构编号")
    private String agencyId;

    /**
     * 借据号
     * 表字段 : t_asset_deal_process_info.SERIAL_NUMBER
     */
    @SerializedName("借据号")
    private String serialNumber;

    /**
     * 处置状态（处置中
     已处置
     ）
     * 表字段 : t_asset_deal_process_info.DEAL_STATUS
     */
    @SerializedName("处置状态")
    private String dealStatus;

    /**
     * 处置类型（SUSONG-诉讼
     FEISONGSU-非诉讼
     ）
     * 表字段 : t_asset_deal_process_info.DEAL_TYPE
     */
    @SerializedName("处置类型")
    private String dealType;

    /**
     * 诉讼节点 处置开始
     诉讼准备
     法院受理
     执行拍卖
     处置结束

     * 表字段 : t_asset_deal_process_info.LAWSUIT_NODE
     */
    @SerializedName("诉讼节点")
    private String lawsuitNode;

    /**
     * 诉讼节点时间
     * 表字段 : t_asset_deal_process_info.LAWSUIT_NODE_TIME
     */
    @SerializedName("诉讼节点时间")
    private Date lawsuitNodeTime;

    /**
     * 处置结果
     * 表字段 : t_asset_deal_process_info.DEAL_RESULT
     */
    @SerializedName("处置结果")
    private String dealResult;

    /**
     * 导入Id
     **/
    private Integer importId;

    /**
     * 数据来源1:startLink,2:excelImport
     **/
    private Integer dataSource;

    /**
     * 获取 项目编号 字段:t_asset_deal_process_info.PROJECT_ID
     *
     * @return t_asset_deal_process_info.PROJECT_ID, 项目编号
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * 设置 项目编号 字段:t_asset_deal_process_info.PROJECT_ID
     *
     * @param projectId t_asset_deal_process_info.PROJECT_ID, 项目编号
     */
    public void setProjectId(String projectId) {
        this.projectId = projectId == null ? null : projectId.trim();
    }

    /**
     * 获取 机构编号 字段:t_asset_deal_process_info.AGENCY_ID
     *
     * @return t_asset_deal_process_info.AGENCY_ID, 机构编号
     */
    public String getAgencyId() {
        return agencyId;
    }

    /**
     * 设置 机构编号 字段:t_asset_deal_process_info.AGENCY_ID
     *
     * @param agencyId t_asset_deal_process_info.AGENCY_ID, 机构编号
     */
    public void setAgencyId(String agencyId) {
        this.agencyId = agencyId == null ? null : agencyId.trim();
    }

    /**
     * 获取 借据号 字段:t_asset_deal_process_info.SERIAL_NUMBER
     *
     * @return t_asset_deal_process_info.SERIAL_NUMBER, 借据号
     */
    public String getSerialNumber() {
        return serialNumber;
    }

    /**
     * 设置 借据号 字段:t_asset_deal_process_info.SERIAL_NUMBER
     *
     * @param serialNumber t_asset_deal_process_info.SERIAL_NUMBER, 借据号
     */
    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber == null ? null : serialNumber.trim();
    }

    /**
     * 获取 处置状态（处置中
已处置
） 字段:t_asset_deal_process_info.DEAL_STATUS
     *
     * @return t_asset_deal_process_info.DEAL_STATUS, 处置状态（处置中
已处置
）
     */
    public String getDealStatus() {
        return dealStatus;
    }

    /**
     * 设置 处置状态（处置中
已处置
） 字段:t_asset_deal_process_info.DEAL_STATUS
     *
     * @param dealStatus t_asset_deal_process_info.DEAL_STATUS, 处置状态（处置中
已处置
）
     */
    public void setDealStatus(String dealStatus) {
        this.dealStatus = dealStatus == null ? null : dealStatus.trim();
    }

    /**
     * 获取 处置类型（SUSONG-诉讼
FEISONGSU-非诉讼
） 字段:t_asset_deal_process_info.DEAL_TYPE
     *
     * @return t_asset_deal_process_info.DEAL_TYPE, 处置类型（SUSONG-诉讼
FEISONGSU-非诉讼
）
     */
    public String getDealType() {
        return dealType;
    }

    /**
     * 设置 处置类型（SUSONG-诉讼
FEISONGSU-非诉讼
） 字段:t_asset_deal_process_info.DEAL_TYPE
     *
     * @param dealType t_asset_deal_process_info.DEAL_TYPE, 处置类型（SUSONG-诉讼
FEISONGSU-非诉讼
）
     */
    public void setDealType(String dealType) {
        this.dealType = dealType == null ? null : dealType.trim();
    }

    /**
     * 获取 诉讼节点 处置开始
诉讼准备
法院受理
执行拍卖
处置结束
 字段:t_asset_deal_process_info.LAWSUIT_NODE
     *
     * @return t_asset_deal_process_info.LAWSUIT_NODE, 诉讼节点 处置开始
诉讼准备
法院受理
执行拍卖
处置结束

     */
    public String getLawsuitNode() {
        return lawsuitNode;
    }

    /**
     * 设置 诉讼节点 处置开始
诉讼准备
法院受理
执行拍卖
处置结束
 字段:t_asset_deal_process_info.LAWSUIT_NODE
     *
     * @param lawsuitNode t_asset_deal_process_info.LAWSUIT_NODE, 诉讼节点 处置开始
诉讼准备
法院受理
执行拍卖
处置结束

     */
    public void setLawsuitNode(String lawsuitNode) {
        this.lawsuitNode = lawsuitNode == null ? null : lawsuitNode.trim();
    }

    /**
     * 获取 诉讼节点时间 字段:t_asset_deal_process_info.LAWSUIT_NODE_TIME
     *
     * @return t_asset_deal_process_info.LAWSUIT_NODE_TIME, 诉讼节点时间
     */
    public Date getLawsuitNodeTime() {
        return lawsuitNodeTime;
    }

    /**
     * 设置 诉讼节点时间 字段:t_asset_deal_process_info.LAWSUIT_NODE_TIME
     *
     * @param lawsuitNodeTime t_asset_deal_process_info.LAWSUIT_NODE_TIME, 诉讼节点时间
     */
    public void setLawsuitNodeTime(Date lawsuitNodeTime) {
        this.lawsuitNodeTime = lawsuitNodeTime == null ? null : lawsuitNodeTime;
    }

    /**
     * 获取 处置结果 字段:t_asset_deal_process_info.DEAL_RESULT
     *
     * @return t_asset_deal_process_info.DEAL_RESULT, 处置结果
     */
    public String getDealResult() {
        return dealResult;
    }

    /**
     * 设置 处置结果 字段:t_asset_deal_process_info.DEAL_RESULT
     *
     * @param dealResult t_asset_deal_process_info.DEAL_RESULT, 处置结果
     */
    public void setDealResult(String dealResult) {
        this.dealResult = dealResult == null ? null : dealResult.trim();
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
        return "AssetDealProcessInfo{" +
                "projectId='" + projectId + '\'' +
                ", agencyId='" + agencyId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", dealStatus='" + dealStatus + '\'' +
                ", dealType='" + dealType + '\'' +
                ", lawsuitNode='" + lawsuitNode + '\'' +
                ", lawsuitNodeTime=" + lawsuitNodeTime +
                ", dealResult='" + dealResult + '\'' +
                ", importId=" + importId +
                ", dataSource=" + dataSource +
                ", id=" + id +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}