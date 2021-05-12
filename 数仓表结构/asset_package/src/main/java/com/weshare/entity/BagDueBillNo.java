package com.weshare.entity;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Created by mouzwang on 2020-12-01 11:44
 */
public class BagDueBillNo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String projectId;
    private String dueBillNo;
    private String bagId;
    private BigDecimal packageRemainPrincipal;
    private Integer packageRemainPeriods;
    private String relatedProjectId;
    private String relatedDate;

    public BagDueBillNo() {
    }

    public BagDueBillNo(String projectId, String dueBillNo, String bagId, BigDecimal packageRemainPrincipal, Integer packageRemainPeriods, String relatedProjectId, String relatedDate) {
        this.projectId = projectId;
        this.dueBillNo = dueBillNo;
        this.bagId = bagId;
        this.packageRemainPrincipal = packageRemainPrincipal;
        this.packageRemainPeriods = packageRemainPeriods;
        this.relatedProjectId = relatedProjectId;
        this.relatedDate = relatedDate;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getDueBillNo() {
        return dueBillNo;
    }

    public void setDueBillNo(String dueBillNo) {
        this.dueBillNo = dueBillNo;
    }

    public String getBagId() {
        return bagId;
    }

    public void setBagId(String bagId) {
        this.bagId = bagId;
    }

    public BigDecimal getPackageRemainPrincipal() {
        return packageRemainPrincipal;
    }

    public void setPackageRemainPrincipal(BigDecimal packageRemainPrincipal) {
        this.packageRemainPrincipal = packageRemainPrincipal;
    }

    public Integer getPackageRemainPeriods() {
        return packageRemainPeriods;
    }

    public void setPackageRemainPeriods(Integer packageRemainPeriods) {
        this.packageRemainPeriods = packageRemainPeriods;
    }

    public String getRelatedProjectId() {
        return relatedProjectId;
    }

    public void setRelatedProjectId(String relatedProjectId) {
        this.relatedProjectId = relatedProjectId;
    }

    public String getRelatedDate() {
        return relatedDate;
    }

    public void setRelatedDate(String relatedDate) {
        this.relatedDate = relatedDate;
    }

    @Override
    public String toString() {
        return "BagDueBillNo{" +
                "projectId='" + projectId + '\'' +
                ", dueBillNo='" + dueBillNo + '\'' +
                ", bagId='" + bagId + '\'' +
                ", packageRemainPrincipal=" + packageRemainPrincipal +
                ", packageRemainPeriods=" + packageRemainPeriods +
                ", relatedProjectId='" + relatedProjectId + '\'' +
                ", relatedDate='" + relatedDate + '\'' +
                '}';
    }
}
