package com.weshare.bizconftable.bean;

import org.springframework.context.annotation.Bean;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;

/**
 * Created by mouzwang on 2021-01-20 11:27
 */
public class PageBizConf implements Serializable {
    private String id; //id:若为星云,则为cloud-project_id,看管为eagle-product_id
    private String bizName; //业务名称(中文)
    private String bizNameEn; //业务名称(英文)
    private String capitalId; //资金方编号
    private String capitalName; //资金方名称(中文)
    private String capitalNameEn; //资金方名称(英文)
    private String channelId; //渠道方编号
    private String channelName; //渠道方名称(中文)
    private String channelNameEn; //渠道方名称（英文)
    private String trustId; //信托计划编号
    private String trustName; //信托计划名称(中文)
    private String trustNameEn; //信托计划名称(英文)
    private String projectId; //项目编号
    private String projectName; //项目名称（中文）
    private String projectNameEn; //项目名称（英文）
    private String projectAmount; //项目初始金额
    private String productId; //产品编号
    private String productName; //产品名称（中文）
    private String productNameEn; //产品名称（英文）
    private String productIdVt; //产品编号（虚拟）
    private String productNameVt; //产品名称（中文、虚拟）
    private String productNameEnVt; //产品名称（英文、虚拟）

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getBizName() {
        return bizName;
    }

    public void setBizName(String bizName) {
        this.bizName = bizName;
    }

    public String getBizNameEn() {
        return bizNameEn;
    }

    public void setBizNameEn(String bizNameEn) {
        this.bizNameEn = bizNameEn;
    }

    public String getCapitalId() {
        return capitalId;
    }

    public void setCapitalId(String capitalId) {
        this.capitalId = capitalId;
    }

    public String getCapitalName() {
        return capitalName;
    }

    public void setCapitalName(String capitalName) {
        this.capitalName = capitalName;
    }

    public String getCapitalNameEn() {
        return capitalNameEn;
    }

    public void setCapitalNameEn(String capitalNameEn) {
        this.capitalNameEn = capitalNameEn;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public String getChannelNameEn() {
        return channelNameEn;
    }

    public void setChannelNameEn(String channelNameEn) {
        this.channelNameEn = channelNameEn;
    }

    public String getTrustId() {
        return trustId;
    }

    public void setTrustId(String trustId) {
        this.trustId = trustId;
    }

    public String getTrustName() {
        return trustName;
    }

    public void setTrustName(String trustName) {
        this.trustName = trustName;
    }

    public String getTrustNameEn() {
        return trustNameEn;
    }

    public void setTrustNameEn(String trustNameEn) {
        this.trustNameEn = trustNameEn;
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

    public String getProjectNameEn() {
        return projectNameEn;
    }

    public void setProjectNameEn(String projectNameEn) {
        this.projectNameEn = projectNameEn;
    }

    public String getProjectAmount() {
        return projectAmount;
    }

    public void setProjectAmount(String projectAmount) {
        this.projectAmount = projectAmount;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProductNameEn() {
        return productNameEn;
    }

    public void setProductNameEn(String productNameEn) {
        this.productNameEn = productNameEn;
    }

    public String getProductIdVt() {
        return productIdVt;
    }

    public void setProductIdVt(String productIdVt) {
        this.productIdVt = productIdVt;
    }

    public String getProductNameVt() {
        return productNameVt;
    }

    public void setProductNameVt(String productNameVt) {
        this.productNameVt = productNameVt;
    }

    public String getProductNameEnVt() {
        return productNameEnVt;
    }

    public void setProductNameEnVt(String productNameEnVt) {
        this.productNameEnVt = productNameEnVt;
    }
}
