package com.weshare.interfaces;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by mouzwang on 2020-11-30 15:41
 */
public interface AssetFiles {

    void insertData(Dataset<String> dataset,SparkSession sparkSession,String fileId);

    void updateData(Dataset<String> dataset,SparkSession sparkSession,String fileId);

    void deleteData(Dataset<String> dataset, SparkSession sparkSession,String fileId);
}
