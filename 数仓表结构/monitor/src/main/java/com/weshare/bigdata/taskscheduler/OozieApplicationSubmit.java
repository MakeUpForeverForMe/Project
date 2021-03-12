package com.weshare.bigdata.taskscheduler;

import com.weshare.bigdata.util.HdfsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * created by chao.guo on 2021/2/20
 **/
public class OozieApplicationSubmit {
    private static final Logger LOGGER = LoggerFactory.getLogger(OozieApplicationSubmit.class);

    public static Properties properties =null;

    public OozieApplicationSubmit() {
    }

    public static  void initOozieProperties(String config_path, String hdfs_master,int isHa)  {
        Configuration conf = HdfsUtils.initConfiguration(hdfs_master, isHa);
       /* org.apache.hadoop.conf.Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfs_master); //hdfs://10.83.0.47:8020*/
        FSDataInputStream in = null;
        try {
            in = FileSystem.get(conf).open(new Path(config_path));
            properties = new Properties();
            properties.load(in);
            in.close();
            LOGGER.info("properties init [{}]",properties);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    public static WorkflowJob.Status subitOozieJob(String workflow_xml,int isHa) throws OozieClientException, InterruptedException {
        OozieClient oozieClient = new OozieClient(properties.getProperty("oozieUrl"));
        Properties conf = oozieClient.createConfiguration();
        conf.setProperty("oozie.wf.application.path",workflow_xml);
        if(isHa==1){
            conf.setProperty("dfs.nameservices", "HDFS14398"); // nameservice
            conf.setProperty("dfs.ha.namenodes.hadoop-ns1", "nn1,nn2");
            conf.setProperty("dfs.namenode.rpc-address.HDFS14398.nn1", "10.80.0.195:4007");
            conf.setProperty("dfs.namenode.rpc-address.HDFS14398.nn2", "10.80.1.155:4007");
            conf.setProperty("dfs.client.failover.proxy.provider.HDFS14398", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
            conf.setProperty("nameNode", "hdfs://HDFS14398");
        }else{
            conf.setProperty("nameNode", properties.getProperty("nameNode"));
        }
        conf.setProperty("jobTracker", properties.getProperty("jobTracker"));
        conf.setProperty("queueName", properties.getProperty("queueName"));
        conf.setProperty("oozie.use.system.libpath", "true");
        conf.setProperty("user.name", properties.getProperty("user_name"));
        //conf.setProperty("javaOpts","-Xmx1000m");
        conf.setProperty("hdfs.keytab.file", "C:/Program Files (x86)/Java/newhadoop_oozieweb_conf/oozieweb.keytab");
        conf.setProperty("hdfs.kerberos.principal", "oozieweb");
        conf.setProperty("oozie.libpath","/user/oozie/share/lib");
        String jobId = oozieClient.run(conf);
        LOGGER.info("Workflow job submitted");

        while (oozieClient.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
            LOGGER.info("Workflow job running ..."+"\t"+ oozieClient.getJobLog(jobId));
            Thread.sleep(10 * 1000);
        }
        LOGGER.info("Workflow job completed ...");
        LOGGER.info(oozieClient.getJobInfo(jobId).getUser()+"\t"+
                oozieClient.getJobInfo(jobId).getStatus()+"\t"+
                oozieClient.getJobInfo(jobId));
        return oozieClient.getJobInfo(jobId).getStatus();
    }
}
