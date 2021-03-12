package com.weshare.interfaces.impl;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weshare.entity.BagInfo;
import com.weshare.interfaces.AssetFiles;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by mouzwang on 2020-11-30 15:50
 */
public class BagInfoImpl implements AssetFiles, Serializable {
    private static final long serialVersionUID = 1L;

    public Dataset<BagInfo> process(Dataset<String> dataset) {
        Dataset<BagInfo> mapedDS = dataset.map(new MapFunction<String, BagInfo>() {
            @Override
            public BagInfo call(String value) throws Exception {
                BagInfo bagInfo = new BagInfo();
                JsonObject jsonObject = new JsonParser().parse(value).getAsJsonObject();
                bagInfo.setProjectId(jsonObject.get("project_id").getAsString());
                bagInfo.setBagId(jsonObject.get("bag_id").getAsString());
                bagInfo.setBagName(jsonObject.get("bag_name").getAsString());
                bagInfo.setBagStatus(jsonObject.get("bag_status").getAsString());
                bagInfo.setBagRemainPrincipal(jsonObject.get("bag_remain_principal").getAsBigDecimal());
                bagInfo.setBagDate(jsonObject.get("bag_date").getAsString());
                bagInfo.setInsertDate(jsonObject.get("insert_date").getAsString());
                return bagInfo;
            }
        }, Encoders.bean(BagInfo.class));
        return mapedDS;
    }


    @Override
    public void insertData(Dataset<String> dataset, SparkSession sparkSession, String fileId) {
        Dataset<BagInfo> processDS = process(dataset);
        processDS.registerTempTable("temp_bag_info");
        sparkSession.sql("insert into table dim.bag_info partition (bag_id = '" + fileId + "') " +
                "select projectId,bagName,bagStatus,bagRemainPrincipal,bagDate,insertDate from " +
                "temp_bag_info ");
    }

    @Override
    public void updateData(Dataset<String> dataset, SparkSession sparkSession, String fileId) {
        Dataset<BagInfo> processDS = process(dataset);
        processDS.registerTempTable("temp_bag_info");
        sparkSession.sql("insert overwrite table dim.bag_info partition (bag_id = '" + fileId + "') " +
                "select projectId,bagName,bagStatus,bagRemainPrincipal,bagDate,insertDate from " +
                "temp_bag_info ");
    }

    @Override
    public void deleteData(Dataset<String> dataset, SparkSession sparkSession, String fileId) {
        sparkSession.sql("alter table dim.bag_info drop partition (bag_id = '" + fileId + "')");
        sparkSession.sql("alter table dim.bag_due_bill_no drop partition (bag_id = '" + fileId + "')");
    }

}
