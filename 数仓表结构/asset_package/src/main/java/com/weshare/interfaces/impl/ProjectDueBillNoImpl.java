package com.weshare.interfaces.impl;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.weshare.entity.ProjectDueBillNo;
import com.weshare.entity.ProjectDueBillNoHelper;
import com.weshare.interfaces.AssetFiles;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by mouzwang on 2020-11-30 15:49
 */
public class ProjectDueBillNoImpl implements AssetFiles, Serializable {
    private static final long serialVersionUID = 1L;

    public Dataset<ProjectDueBillNo> process(Dataset<String> dataset) {
        Dataset<ProjectDueBillNo> flatMapDS = dataset.flatMap(new FlatMapFunction<String, ProjectDueBillNo>() {
            ArrayList<ProjectDueBillNo> list = new ArrayList<ProjectDueBillNo>();

            @Override
            public Iterator<ProjectDueBillNo> call(String s) throws Exception {
                JsonObject jsonObject = new JsonParser().parse(s).getAsJsonObject();
                String projectId = jsonObject.get("project_id").getAsString();
                String importId = jsonObject.get("import_id").getAsString();
                JsonArray due_bill_nos = jsonObject.getAsJsonArray("due_bill_no");
                Gson gson = new Gson();
                for (JsonElement due_bill_no : due_bill_nos) {
                    ProjectDueBillNo projectDueBillNo = new ProjectDueBillNo();
                    projectDueBillNo.setImportId(importId);
                    projectDueBillNo.setProjectId(projectId);
                    ProjectDueBillNoHelper helper = gson.fromJson(due_bill_no,
                            new TypeToken<ProjectDueBillNoHelper>() {
                            }.getType());
                    projectDueBillNo.setDueBillNo(helper.getSerialNumber());
                    projectDueBillNo.setRelatedProjectId(helper.getRelatedProjectId());
                    projectDueBillNo.setRelatedDate(helper.getRelatedDate());
                    list.add(projectDueBillNo);
                }
                return list.iterator();
            }
        }, Encoders.bean(ProjectDueBillNo.class));
        return flatMapDS;
    }


    @Override
    public void insertData(Dataset<String> dataset, SparkSession sparkSession, String fileId) {
        Dataset<ProjectDueBillNo> processDS = process(dataset);
        processDS.registerTempTable("temp_dim_project_due_bill_no");
        sparkSession.sql("insert into table dim.project_due_bill_no partition(project_id='" + fileId + "',import_id) " +
                "select " +
                "temp.dueBillNo, " +
                "temp.relatedProjectId, " +
                "temp.relatedDate, " +
                "loan.product_id as partition_id, " +
                "temp.importId " +
                "from temp_dim_project_due_bill_no temp " +
                "left join " +
                "(select product_id,due_bill_no from ods.loan_info_abs group by product_id, " +
                "due_bill_no) loan " +
                "on temp.dueBillNo = loan.due_bill_no ");
    }

    @Override
    public void updateData(Dataset<String> dataset, SparkSession sparkSession, String fileId) {
        Dataset<ProjectDueBillNo> processDS = process(dataset);
        processDS.registerTempTable("temp_dim_project_due_bill_no");
        sparkSession.sql("insert overwrite table dim.project_due_bill_no partition(project_id='" + fileId + "',import_id) " +
                "select " +
                "temp.dueBillNo, " +
                "temp.relatedProjectId, " +
                "temp.relatedDate, " +
                "loan.product_id as partition_id " +
                "from temp_dim_project_due_bill_no temp " +
                "left join " +
                "(select product_id,due_bill_no from ods.loan_info_abs group by product_id, " +
                "due_bill_no) loan " +
                "on temp.dueBillNo = loan.due_bill_no ");
    }

    @Override
    public void deleteData(Dataset<String> dataset, SparkSession sparkSession, String fileId, String importId) {
        sparkSession.sql("alter table dim.project_due_bill_no drop partition (project_id = '" + fileId + "',import_id = '" + importId + "')");
    }
}
