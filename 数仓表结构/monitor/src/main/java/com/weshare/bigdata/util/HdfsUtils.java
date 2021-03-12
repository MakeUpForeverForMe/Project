package com.weshare.bigdata.util;

import org.apache.hadoop.conf.Configuration;

/**
 * created by chao.guo on 2021/2/24
 **/
public class HdfsUtils {
    public static Configuration initConfiguration(String hdfs ,int isHa){
        Configuration conf  = new Configuration();;
        switch (isHa) {
            case 1:{
                conf.set("fs.defaultFS","hdfs://HDFS14398");//hdfs://HDFS14398  nameservice 地址
                conf.set("dfs.nameservices", "HDFS14398"); // nameservice
                conf.set("dfs.ha.namenodes.HDFS14398", "nn1,nn2");
                conf.set("dfs.namenode.rpc-address.HDFS14398.nn1", "10.80.0.195:4007");
                conf.set("dfs.namenode.rpc-address.HDFS14398.nn2", "10.80.1.155:4007");
                conf.set("dfs.client.failover.proxy.provider.HDFS14398", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
                break;
            }
            case 0:{
                conf.set("fs.defaultFS", hdfs); //hdfs://10.83.0.47:8020
                conf.setBoolean("dfs.support.append", true);
                break;
            }
        }
        return  conf;

    }


}
