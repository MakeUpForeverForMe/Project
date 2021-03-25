package com.weshare.entity;


import java.io.Serializable;

/**
 * Created by mouzwang on 2020-12-01 15:09
 */
public class ProjectDueBillNo implements Serializable {
    private static final long serialVersionUID = 1L;
    private String importId;
    private String dueBillNo;
    private String projectId;
    private String relatedProjectId;
    private String relatedDate;

    public ProjectDueBillNo() {
    }

    public ProjectDueBillNo(String dueBillNo, String projectId) {
        this.dueBillNo = dueBillNo;
        this.projectId = projectId;
    }

    public String getImportId() {
        return importId;
    }

    public void setImportId(String importId) {
        this.importId = importId;
    }

    public String getDueBillNo() {
        return dueBillNo;
    }

    public void setDueBillNo(String dueBillNo) {
        this.dueBillNo = dueBillNo;
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
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
