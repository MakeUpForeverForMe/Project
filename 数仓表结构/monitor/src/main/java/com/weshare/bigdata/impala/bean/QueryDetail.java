package com.weshare.bigdata.impala.bean;

/**
 * Created by mouzwang on 2021-01-07 16:15
 */
public class QueryDetail {
    private String frontendHostName;
    private String queryState;
    private String queryType;
    private String queryId; // queryId
    private String statement;
    private Long durationTime;
    private String serviceName;

    public QueryDetail() {
    }

    public String getFrontendHostName() {
        return frontendHostName;
    }

    public void setFrontendHostName(String frontendHostName) {
        this.frontendHostName = frontendHostName;
    }

    public String getQueryState() {
        return queryState;
    }

    public void setQueryState(String queryState) {
        this.queryState = queryState;
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public Long getDurationTime() {
        return durationTime;
    }

    public void setDurationTime(Long durationTime) {
        this.durationTime = durationTime;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
