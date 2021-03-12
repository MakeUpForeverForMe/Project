package com.weshare.entity;

import java.io.Serializable;

/**
 * Created by mouzwang on 2021-02-03 16:09
 */
public class ProjectDueBillNoHelper implements Serializable {
    private static final long serialVersionUID = 1L;
    private String serialNumber;
    private String relatedProjectId;
    private String relatedDate;

    public ProjectDueBillNoHelper() {
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber;
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
