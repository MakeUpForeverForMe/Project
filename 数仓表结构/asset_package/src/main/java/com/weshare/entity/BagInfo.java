package com.weshare.entity;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Created by mouzwang on 2020-12-01 14:52
 */
public class BagInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String projectId;
    private String bagId;
    private String bagName;
    private String bagStatus;
    private BigDecimal bagRemainPrincipal;
    private String bagDate;
    private String insertDate;

    public BagInfo() {
    }

    public BagInfo(String projectId, String bagId, String bagName, String bagStatus, BigDecimal bagRemainPrincipal, String bagDate) {
        this.projectId = projectId;
        this.bagId = bagId;
        this.bagName = bagName;
        this.bagStatus = bagStatus;
        this.bagRemainPrincipal = bagRemainPrincipal;
        this.bagDate = bagDate;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getInsertDate() {
        return insertDate;
    }

    public void setInsertDate(String insertDate) {
        this.insertDate = insertDate;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getBagId() {
        return bagId;
    }

    public void setBagId(String bagId) {
        this.bagId = bagId;
    }

    public String getBagName() {
        return bagName;
    }

    public void setBagName(String bagName) {
        this.bagName = bagName;
    }

    public String getBagStatus() {
        return bagStatus;
    }

    public void setBagStatus(String bagStatus) {
        this.bagStatus = bagStatus;
    }

    public BigDecimal getBagRemainPrincipal() {
        return bagRemainPrincipal;
    }

    public void setBagRemainPrincipal(BigDecimal bagRemainPrincipal) {
        this.bagRemainPrincipal = bagRemainPrincipal;
    }

    public String getBagDate() {
        return bagDate;
    }

    public void setBagDate(String bagDate) {
        this.bagDate = bagDate;
    }
}
