package com.weshare.interfaces.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weshare.entity.ProjectInfo;
import com.weshare.interfaces.AssetFiles;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-11-30 15:42
 */
public class ProjectInfoImpl implements AssetFiles, Serializable {
    private static final long serialVersionUID = 1L;

    public Dataset<ProjectInfo> process(Dataset<String> dataset) {
        Dataset<ProjectInfo> mapedDS = dataset.map(new MapFunction<String, ProjectInfo>() {
            @Override
            public ProjectInfo call(String value) throws Exception {
                ProjectInfo projectInfo = new ProjectInfo();
                JsonObject jsonObject = new JsonParser().parse(value).getAsJsonObject();
                projectInfo.setProjectId(jsonObject.get("project_id").getAsString());
                projectInfo.setProjectName(jsonObject.get("project_name").getAsString());
                projectInfo.setProjectStage(jsonObject.get("project_stage").getAsString());
                projectInfo.setAssetSide(jsonObject.get("asset_side").getAsString());
                projectInfo.setFundSide(jsonObject.get("fund_side").getAsString());
                projectInfo.setYear(jsonObject.get("year").getAsString());
                projectInfo.setTerm(jsonObject.get("term").getAsString());
                projectInfo.setRemarks(jsonObject.get("remarks") == null ? null :
                        jsonObject.get("remarks").getAsString());
                projectInfo.setProjectFullName(jsonObject.get("project_full_name").getAsString());
                projectInfo.setAssetType(jsonObject.get("asset_type").getAsString());
                projectInfo.setProjectType(jsonObject.get("project_type").getAsString());
                projectInfo.setMode(jsonObject.get("mode").getAsString());
                projectInfo.setProjectTime(jsonObject.get("project_time").getAsString());
                projectInfo.setProjectBeginDate(jsonObject.get("project_begin_date") == null ?
                        null : jsonObject.get("project_begin_date").getAsString());
                projectInfo.setProjectEndDate(jsonObject.get("project_end_date") == null ?
                        null : jsonObject.get("project_end_date").getAsString());
                projectInfo.setAssetPoolType(jsonObject.get("asset_pool_type").getAsString());
                projectInfo.setPublicOffer(jsonObject.get("public_offer") == null ? null : jsonObject.get("public_offer").getAsString());
                projectInfo.setDataSource(jsonObject.get("data_source").getAsString());
                projectInfo.setCreateUser(jsonObject.get("create_user") == null ? null : jsonObject.get("create_user").getAsString());
                projectInfo.setCreateTime(jsonObject.get("create_time").getAsString());
                projectInfo.setUpdateTime(jsonObject.get("update_time").getAsString());
                return projectInfo;
            }
        }, Encoders.bean(ProjectInfo.class));
        return mapedDS;
    }


    @Override
    public void insertData(Dataset<String> dataset, SparkSession sparkSession, String fileId) {
        Dataset<ProjectInfo> processDS = process(dataset);
        processDS.registerTempTable("temp_project_info");
        sparkSession.sql("insert overwrite table dim.project_info partition (project_id = '" + fileId + "') " +
                "select " +
                "projectName,projectStage,assetSide,fundSide,year,term,remarks,projectFullName," +
                "assetType,projectType,mode,projectTime,projectBeginDate,projectEndDate," +
                "assetPoolType,publicOffer,dataSource,createUser,createTime,updateTime " +
                "from temp_project_info ");
    }

    @Override
    public void updateData(Dataset<String> dataset, SparkSession sparkSession, String fileId) {
        Dataset<ProjectInfo> processDS = process(dataset);
        processDS.registerTempTable("temp_project_info");
        sparkSession.sql("insert overwrite table dim.project_info partition (project_id='" + fileId +
                "') " +
                "select " +
                "projectName,projectStage,assetSide,fundSide,year,term,remarks,projectFullName," +
                "assetType,projectType,mode,projectTime,projectBeginDate,projectEndDate," +
                "assetPoolType,publicOffer,dataSource,createUser,createTime,updateTime " +
                "from temp_project_info ");
    }

    @Override
    public void deleteData(Dataset<String> dataset, SparkSession sparkSession, String fileId, String importId) {
        sparkSession.sql("alter table dim.project_info drop partition (project_id = '" + fileId + "') ");
        sparkSession.sql("alter table dim.project_due_bill_no drop partition (project_id = '" + fileId + "') ");
    }
}
