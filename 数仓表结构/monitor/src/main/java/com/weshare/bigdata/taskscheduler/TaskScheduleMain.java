package com.weshare.bigdata.taskscheduler;

import com.weshare.bigdata.util.DruidDataSourceUtils;
import com.weshare.bigdata.util.SendEmailUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * created by chao.guo on 2021/2/19
 **/
public class TaskScheduleMain {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskScheduleMain.class);

    public static void main(String[] args) throws Exception {
        String type=args[0];
        String batch_date=args[1];
        LOGGER.info("type={},batch_date={}",type,batch_date);

        TaskSceduleService taskSceduleService = new TaskSceduleService();
        TaskRunningService taskRunningService = new TaskRunningService();
        Properties pro = initProperties(type);
        initDataSource(pro);// 初始化连接池信息 cm 的数据库 业务库的数据库 impala 的数据库
        //updateImpalaDate(batch_date); // 修改配置信息
        List<TaskMode> taskMode = taskSceduleService.getTaskMode(batch_date,pro);
        // 运行核心的抽数
        taskRunningService.runingJob(taskMode,"core",pro);
        // 执行bigdata 这边的抽数 实还和订单 订单历史表
        // 判断 类型定义为core的 是否全部跑完  跑完后 跑类型为bigdata 的 按顺序执行  bigdata执行完 执行 data_check
        //core (finish) ----> bigdata(finish) --->data_check(finish)
        boolean core_isFinish = taskRunningService.judgeJobIsAllFinish(taskMode,"core");
        if(core_isFinish){ // 核心运行完
            taskRunningService.runingJob(taskMode,"bigdata",pro);
        }
        boolean bigdata_isFinish = taskRunningService.judgeJobIsAllFinish(taskMode,"bigdata");

        if(core_isFinish&&bigdata_isFinish ){
            taskRunningService.runingJob(taskMode,"data_check",pro);
        }

        taskMode.forEach(it->{
            LOGGER.info("{}",it);
        });
    }

    private static Properties initProperties(String type) throws IOException {
        Properties pro =new Properties();
        switch (type) {
            case "EMR":
                InputStream inputStream = TaskScheduleMain.class.getClassLoader().getResourceAsStream("emr_properties.properties");
                pro.load(inputStream);
                inputStream.close();
                break;
            case "PRO":
                InputStream input =TaskScheduleMain.class.getClassLoader().getResourceAsStream("pro_properties.properties");
                pro.load(input);
                input.close();
                break;

        }
        return  pro;
    }

    /**
     * 初始化连接池信息
     *
     */
    public static void initDataSource(Properties pro) throws Exception {


        /*String hdfs_config="/user/admin/data_watch/cm_properties.properties"; //
        String  pro_hdfs_config="/user/admin/data_watch/pro_properties.properties";
        String oozie_config="/user/admin/data_watch/oozie_properties.properties";*/
       String hdfs_config= pro.getProperty("cm_config");
        String pro_hdfs_config= pro.getProperty("pro_mysql_config");
        String oozie_config= pro.getProperty("oozie_config");
        String hdfs_master =pro.getProperty("hdfs_master");
        int isHa=Integer.parseInt(pro.getProperty("isHa"));
        //String email_config="/user/admin/data_watch/email.properties";
        //String hdfs_master ="hdfs://node233:8020";
        if(null!=System.getenv() &&  null!=System.getenv().get("OS") && System.getenv().get("OS").startsWith("Windows") ) {
            hdfs_master = "hdfs://node5:8020";
            hdfs_config="/user/admin/data_watch/test_properties.properties"; //
        }
        DruidDataSourceUtils.initDataSource(hdfs_config,hdfs_master,"cm_mysql",isHa);
        DruidDataSourceUtils.initDataSource(pro_hdfs_config,hdfs_master,"pro_mysql",isHa);
        OozieApplicationSubmit.initOozieProperties(oozie_config,hdfs_master,isHa);
       // SendEmailUtil.initSession(email_config,hdfs_master);
    }

    /**
     * 修改impala 中的 时间配置表
     * 所有抽数脚本 统一读取配置时间表
     * @param batch_date
     * @throws SQLException
     */
    public static void updateImpalaDate(String batch_date) throws SQLException {
        Connection impala = DruidDataSourceUtils.getConection("impala");
        Statement statement = impala.createStatement();
        String update_sql = "upsert into  table ods.task_schedule_date values('"+batch_date+"')";
        int res = statement.executeUpdate(update_sql);
        LOGGER.info("impala-update-batch_date [{}],result [{}]",update_sql,res);
    }


}
