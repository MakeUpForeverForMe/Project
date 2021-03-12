package com.weshare.entity;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * Created by mouzwang on 2020-12-01 11:44
 */
public class BagDueBillNo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String dueBillNo;
    private String bagId;
    private BigDecimal packageRemainPrincipal;
    private Integer packageRemainPeriods;
    private String relatedProjectId;
    private String relatedDate;

    public BagDueBillNo() {
    }

    public BagDueBillNo(String dueBillNo, String bagId, BigDecimal packageRemainPrincipal, Integer packageRemainPeriods) {
        this.dueBillNo = dueBillNo;
        this.bagId = bagId;
        this.packageRemainPrincipal = packageRemainPrincipal;
        this.packageRemainPeriods = packageRemainPeriods;
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
                "dueBillNo='" + dueBillNo + '\'' +
                ", bagId='" + bagId + '\'' +
                ", packageRemainPrincipal=" + packageRemainPrincipal +
                ", packageRemainPeriods=" + packageRemainPeriods +
                '}';
    }
}
