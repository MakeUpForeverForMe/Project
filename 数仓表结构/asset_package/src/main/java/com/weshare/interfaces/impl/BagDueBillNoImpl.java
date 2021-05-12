package com.weshare.interfaces.impl;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import com.weshare.entity.BagDueBillNo;
import com.weshare.entity.BagDueBillNoHelper;
import com.weshare.interfaces.AssetFiles;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by mouzwang on 2020-11-30 15:51
 */
public class BagDueBillNoImpl implements AssetFiles, Serializable {
    private static final long serialVersionUID = 1L;

    public Dataset<BagDueBillNo> process(Dataset<String> dataSet) {
        Dataset<BagDueBillNo> flatMapDS = dataSet.flatMap(new FlatMapFunction<String, BagDueBillNo>() {
            ArrayList<BagDueBillNo> list = new ArrayList<BagDueBillNo>();

            @Override
            public Iterator<BagDueBillNo> call(String s) {
                JsonObject jsonObject = new JsonParser().parse(s).getAsJsonObject();
                String bagId = jsonObject.get("bag_id").getAsString();
                JsonArray due_bill_nos = jsonObject.getAsJsonArray("due_bill_no");
                Gson gson = new Gson();
                for (JsonElement due_bill_no : due_bill_nos) {
                    BagDueBillNo bagDueBillNo = new BagDueBillNo();
                    bagDueBillNo.setBagId(bagId);
                    BagDueBillNoHelper helper = gson.fromJson(due_bill_no, new TypeToken<BagDueBillNoHelper>() {
                    }.getType());
                    bagDueBillNo.setDueBillNo(helper.getSerialNumber());
                    bagDueBillNo.setPackageRemainPrincipal(helper.getPackageRemainPrincipal());
                    bagDueBillNo.setPackageRemainPeriods(helper.getPackageRemainPeriods());
                    list.add(bagDueBillNo);
                }
                return list.iterator();
            }
        }, Encoders.bean(BagDueBillNo.class));
        return flatMapDS;
    }

    @Override
    public void insertData(Dataset<String> dataSet, SparkSession sparkSession, String fileId) {
        Dataset<BagDueBillNo> processDS = process(dataSet);
        processDS.registerTempTable("temp_dim_bag_due_bill_no");
        sparkSession.sql("insert overwrite table dim.bag_due_bill_no partition(bag_id = '" + fileId + "') " +
                "select projectId,dueBillNo,packageRemainPrincipal,packageRemainPeriods from temp_dim_bag_due_bill_no ");
    }

    @Override
    public void updateData(Dataset<String> dataSet, SparkSession sparkSession, String fileId) {
        Dataset<BagDueBillNo> processDS = process(dataSet);
        processDS.registerTempTable("temp_dim_bag_due_bill_no");
        sparkSession.sql("insert overwrite table dim.bag_due_bill_no partition(bag_id = '" + fileId + "') " +
                "select projectId,dueBillNo,packageRemainPrincipal,packageRemainPeriods from temp_dim_bag_due_bill_no ");
    }

    @Override
    public void deleteData(Dataset<String> dataset, SparkSession sparkSession, String fileId, String importId) {
        sparkSession.sql("alter table dim.bag_due_bill_no drop partition (bag_id = '" + fileId + "')");
    }

}
