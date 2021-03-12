package com.weshare.bigdata.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * created by chao.guo on 2021/2/19
 * 数据库连接池
 **/

public class DruidDataSourceUtils {
    private static DruidDataSource test_dataSource=null; // cm的 数据库 存储的是 跑批任务信息
    private static DruidDataSource pro_dataSource=null;
    private  static DruidDataSource imapla_dataSource=null;






    public static  void initDataSource(String hdfs_filePath,String hdfs_maser,String type,int isHa) throws Exception {
        Configuration conf  = HdfsUtils.initConfiguration(hdfs_maser,isHa);

        FSDataInputStream in = FileSystem.get(conf).open(new Path(hdfs_filePath));
        Properties properties = new Properties();
        properties.load(in);
        switch (type){
            case  "cm_mysql":
                if(null==test_dataSource){
                    test_dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
                }
                break;
            case "pro_mysql":
                if(null==pro_dataSource){
                    pro_dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
                }
                break;
            case "impala": if(null==imapla_dataSource){
                imapla_dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
            }
                break;

        }

        in.close();
    }
    public static  Connection getConection(String type) throws SQLException {
        Connection connection=null;
        switch (type){
            case  "cm_mysql":
                connection=  test_dataSource.getConnection();
                break;
            case "pro_mysql":
                connection=   pro_dataSource.getConnection();
                break;
            case "impala":
                connection=   imapla_dataSource.getConnection();
                break;

        }
        return  connection;
    }
    public static void close(){
        if(pro_dataSource!=null && pro_dataSource.isKeepAlive()){
            pro_dataSource.close();
        }
        if(test_dataSource!=null && test_dataSource.isKeepAlive()){
            test_dataSource.close();
        }
        if(imapla_dataSource!=null && imapla_dataSource.isKeepAlive()){
            imapla_dataSource.close();
        }
    }
}
