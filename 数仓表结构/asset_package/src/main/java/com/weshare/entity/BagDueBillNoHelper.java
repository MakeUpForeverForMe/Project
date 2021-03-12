package com.weshare.entity;


import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Created by mouzwang on 2020-12-15 10:39
 */
public class BagDueBillNoHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    private String serialNumber;
    private BigDecimal packageRemainPrincipal;
    private Integer packageRemainPeriods;
    private String relatedProjectId;
    private String relatedDate;

    public BagDueBillNoHelper() {
    }

    public BagDueBillNoHelper(String serialNumber, BigDecimal packageRemainPrincipal, Integer packageRemainPeriods) {
        this.serialNumber = serialNumber;
        this.packageRemainPrincipal = packageRemainPrincipal;
        this.packageRemainPeriods = packageRemainPeriods;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber;
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
}
