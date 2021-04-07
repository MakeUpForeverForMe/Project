package com.weshare.core;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.weshare.interfaces.AssetFiles;
import com.weshare.interfaces.impl.BagDueBillNoImpl;
import com.weshare.interfaces.impl.BagInfoImpl;
import com.weshare.interfaces.impl.ProjectDueBillNoImpl;
import com.weshare.interfaces.impl.ProjectInfoImpl;
import com.weshare.util.COSFileUtils;
import com.weshare.util.FileCheckUtils;
import com.weshare.util.HadoopUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


/**
 * Created by mouzwang on 2020-11-30 15:08
 */
public class AssetFileToHive {
    private AssetFiles assetFiles;
    private static final Logger LOGGER = LoggerFactory.getLogger(AssetFileToHive.class);
    private static final Properties prop = new Properties();

    public static void main(String[] args) {
        if (args.length == 0) {
            LOGGER.warn("correct args was not received!");
            return;
        }
        LOGGER.info("========== AssetFileToHive begin!");
        try {
            prop.load(new FileInputStream("pro_cos.properties"));
        } catch (Exception e) {
            LOGGER.error("load properties failed : ",e);
        }
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        //get basic file name and bag_id or project_id
        String fileName = args[0];
        FileCheckUtils.checkFileName(fileName);
        String fileId = args[1];
        FileCheckUtils.checkFileId(fileId);
        LOGGER.info("========== check completed,file name : {} , file id : {}",fileName,fileId);
        String fileLocalPath =
                prop.getProperty("localFilePathDir") + fileName + "/" + fileName + "@" + fileId + ".json";
        LOGGER.info("local file path : {}",fileLocalPath);
        //get row type
        String json = null;
        try {
            json = IOUtils.toString(new FileInputStream(fileLocalPath), "UTF-8");
        } catch (IOException e) {
            LOGGER.error("failed to load local file: ",e);
        }
        if(json == null){
            LOGGER.error("cannot find local file!");
            return;
        }
        String rowType = getRowType(json);
        //对于债转的project_due_bill_no文件,需要提前解析出import_id44
        String importId = null;
        if("project_due_bill_no".equals(fileName)) importId = getImportId(json);
        LOGGER.info("========== row_type : " + rowType);
        SparkSession sparkSession =
                SparkSession.builder().appName("AssetFileToHive : " + fileName + "@" + fileId)
                    .config("hive.execution.engine","spark")
//                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();
        //put file into HDFS
        LOGGER.info("========== start to copy file to HDFS.");
        String bucketName = prop.getProperty("bucketName");
        String filePath = prop.getProperty("COSFilePathDir") + fileName + "@" + fileId + ".json";
        COSFileUtils.upLoadFilesToCOS(filePath,fileLocalPath,bucketName);
        LOGGER.info("========== copy file to COS completed.");
        //get file path on COS
        String COSFilePath = "cosn://" + bucketName + filePath;
        LOGGER.info("cosFilePath : " + COSFilePath);
        Dataset<String> textFileDS = sparkSession.read().textFile(COSFilePath);
        switch (fileName) {
            case "project_info":
                AssetFileToHive projectInfo = new AssetFileToHive(new ProjectInfoImpl());
                if ("insert".equals(rowType)) projectInfo.executeInsert(textFileDS,sparkSession,fileId);
                else if ("update".equals(rowType)) projectInfo.executeUpdate(textFileDS,sparkSession,fileId);
                else if ("delete".equals(rowType)) projectInfo.executeDelete(textFileDS,sparkSession,fileId,null);
                else LOGGER.warn("error row type value!");
                break;
            case "project_due_bill_no" :
                AssetFileToHive projectDueBillNo = new AssetFileToHive(new ProjectDueBillNoImpl());
                sparkSession.sql("set hive.exec.dynamic.partition=true");
                sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict");
                if ("insert".equals(rowType)) projectDueBillNo.executeInsert(textFileDS,sparkSession,fileId);
                else if ("update".equals(rowType)) projectDueBillNo.executeUpdate(textFileDS,sparkSession,fileId);
                else if ("delete".equals(rowType)) projectDueBillNo.executeDelete(textFileDS,sparkSession, fileId,importId);
                else LOGGER.warn("error row type value!");
                break;
            case "bag_info":
                AssetFileToHive bagInfo = new AssetFileToHive(new BagInfoImpl());
                if ("insert".equals(rowType)) bagInfo.executeInsert(textFileDS,sparkSession,fileId);
                else if ("update".equals(rowType)) bagInfo.executeUpdate(textFileDS,sparkSession,fileId);
                else if ("delete".equals(rowType)) bagInfo.executeDelete(textFileDS,sparkSession,fileId,null);
                else LOGGER.warn("error row type value!");
                break;
            case "bag_due_bill_no":
                AssetFileToHive bagDueBillNo = new AssetFileToHive(new BagDueBillNoImpl());
                if ("insert".equals(rowType)) bagDueBillNo.executeInsert(textFileDS,sparkSession,fileId);
                else if ("update".equals(rowType)) bagDueBillNo.executeUpdate(textFileDS,sparkSession,fileId);
                else if ("delete".equals(rowType)) bagDueBillNo.executeDelete(textFileDS,sparkSession, fileId,null);
                else LOGGER.warn("error row type value!");
                break;
        }
        sparkSession.stop();
    }

    public AssetFileToHive(AssetFiles assetFiles) {
        this.assetFiles = assetFiles;
    }

    public void executeInsert(Dataset<String> dataset,SparkSession sparkSession,String fileId) {
        assetFiles.insertData(dataset,sparkSession,fileId);
    }

    public void executeUpdate(Dataset<String> dataset,SparkSession sparkSession,String fileId) {
        assetFiles.updateData(dataset,sparkSession,fileId);
    }

    public void executeDelete(Dataset<String> dataset,SparkSession sparkSession,String fileId,
                              String importId) {
        assetFiles.deleteData(dataset,sparkSession,fileId,importId);
    }

    /**
     * get row type : insert / update / delete
     * @param context
     * @return
     */
    public static String getRowType(String context) {
        JsonObject jsonObject = new JsonParser().parse(context).getAsJsonObject();
        return jsonObject.get("row_type").getAsString();
    }

    public static String getImportId(String context) {
        JsonObject jsonObject = new JsonParser().parse(context).getAsJsonObject();
        return jsonObject.get("import_id").getAsString();
    }
}
