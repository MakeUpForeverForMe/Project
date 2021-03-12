package com.weshare.util;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.PutObjectResult;
import com.qcloud.cos.region.Region;

import java.io.File;

/**
 * Created by mouzwang on 2021-02-25 18:00
 */
public class COSFileUtils {
    private static String secretId = "AKIDY6e1i9sCrTosvjnbfT2KM7e077xgcMJ6";
    private static String secretKey = "JVUDhxwew32OXQ2NRFpKWSVM0OXR6VfY";
    private static COSCredentials cred = new BasicCOSCredentials(secretId, secretKey);
    private static Region region = new Region("ap-guangzhou");
    private static ClientConfig clientConfig = new ClientConfig(region);
    private static COSClient cosClient = new COSClient(cred, clientConfig);

    public static COSClient getCOSClientInstance() {
        return cosClient;
    }

    public static void upLoadFilesToCOS(String targetPath,String fileLocalPath,String bucketName)  {
        //指定本地文件
        File localFile = new File(fileLocalPath);
        //指定要上传到COS的对象键
        String key = targetPath.substring(1);
        PutObjectRequest request = new PutObjectRequest(bucketName, key, localFile);
        cosClient.putObject(request);
        cosClient.shutdown();
    }
}
