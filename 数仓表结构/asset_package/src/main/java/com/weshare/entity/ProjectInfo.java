package com.weshare.entity;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-11-30 22:28
 */
public class ProjectInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String projectId;
    private String projectName;
    private String projectStage;
    private String assetSide;
    private String fundSide;
    private String year;
    private String term;
    private String projectFullName;
    private String assetType;
    private String projectType;
    private String mode;
    private String projectTime;
    private String projectBeginDate;
    private String projectEndDate;
    private String assetPoolType;
    private String publicOffer;
    private String dataSource;
    private String createUser;
    private String createTime;
    private String updateTime;
    private String remarks;

    public ProjectInfo() {
    }

    public ProjectInfo(String projectId, String projectName, String projectStage, String assetSide, String fundSide, String year, String term, String projectFullName, String assetType, String projectType, String mode, String projectTime, String projectBeginDate, String projectEndDate, String assetPoolType, String publicOffer, String dataSource, String createUser, String createTime, String updateTime, String remarks) {
        this.projectId = projectId;
        this.projectName = projectName;
        this.projectStage = projectStage;
        this.assetSide = assetSide;
        this.fundSide = fundSide;
        this.year = year;
        this.term = term;
        this.projectFullName = projectFullName;
        this.assetType = assetType;
        this.projectType = projectType;
        this.mode = mode;
        this.projectTime = projectTime;
        this.projectBeginDate = projectBeginDate;
        this.projectEndDate = projectEndDate;
        this.assetPoolType = assetPoolType;
        this.publicOffer = publicOffer;
        this.dataSource = dataSource;
        this.createUser = createUser;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.remarks = remarks;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getProjectStage() {
        return projectStage;
    }

    public void setProjectStage(String projectStage) {
        this.projectStage = projectStage;
    }

    public String getAssetSide() {
        return assetSide;
    }

    public void setAssetSide(String assetSide) {
        this.assetSide = assetSide;
    }

    public String getFundSide() {
        return fundSide;
    }

    public void setFundSide(String fundSide) {
        this.fundSide = fundSide;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public String getProjectFullName() {
        return projectFullName;
    }

    public void setProjectFullName(String projectFullName) {
        this.projectFullName = projectFullName;
    }

    public String getAssetType() {
        return assetType;
    }

    public void setAssetType(String assetType) {
        this.assetType = assetType;
    }

    public String getProjectType() {
        return projectType;
    }

    public void setProjectType(String projectType) {
        this.projectType = projectType;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getProjectTime() {
        return projectTime;
    }

    public void setProjectTime(String projectTime) {
        this.projectTime = projectTime;
    }

    public String getProjectBeginDate() {
        return projectBeginDate;
    }

    public void setProjectBeginDate(String projectBeginDate) {
        this.projectBeginDate = projectBeginDate;
    }

    public String getProjectEndDate() {
        return projectEndDate;
    }

    public void setProjectEndDate(String projectEndDate) {
        this.projectEndDate = projectEndDate;
    }

    public String getAssetPoolType() {
        return assetPoolType;
    }

    public void setAssetPoolType(String assetPoolType) {
        this.assetPoolType = assetPoolType;
    }

    public String getPublicOffer() {
        return publicOffer;
    }

    public void setPublicOffer(String publicOffer) {
        this.publicOffer = publicOffer;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getCreateUser() {
        return createUser;
    }

    public void setCreateUser(String createUser) {
        this.createUser = createUser;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }
}
