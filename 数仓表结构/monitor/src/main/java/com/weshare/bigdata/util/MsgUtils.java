package com.weshare.bigdata.util;

import com.weshare.bigdata.domain.RobootRerson;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * created by chao.guo on 2021/2/23
 **/
public class MsgUtils {

    // 企业微信消息通知
    //查询配置的机器人信息
    public static List<RobootRerson> getAllRobootRerson(Properties pro) throws Exception {
        ArrayList<RobootRerson> robootRersons = new ArrayList<>();
        String hdfs_config= pro.getProperty("cm_config");
        String hdfs_master =pro.getProperty("hdfs_master");
        int isHa=Integer.parseInt(pro.getProperty("isHa"));

        if(null!=System.getenv() &&  null!=System.getenv().get("OS") && System.getenv().get("OS").startsWith("Windows") ) {
            hdfs_master = "hdfs://node5:8020";
            hdfs_config="/user/admin/data_watch/test_properties.properties"; //
        }
        DruidDataSourceUtils.initDataSource(hdfs_config,hdfs_master,"cm_mysql",isHa);
        Connection cm_mysql = DruidDataSourceUtils.getConection("cm_mysql");
        ResultSet resultSet = cm_mysql.createStatement().executeQuery("select * from flink_config.robot_person_info where isEnable=1");
        while (resultSet.next()) {
            robootRersons.add( new RobootRerson(resultSet.getInt("id"),resultSet.getString("hookurl"),resultSet.getInt("isEnable"),resultSet.getString("user_phones")));
        }
            return  robootRersons;
    }

    /**
     * 初始化消息内容
     * @param context
     * @param type
     * @return
     */
    /**
     * 初始化消息内容
     * @param context
     * @param type
     * @return
     */
    public static String initMsgInfo(String context, String type,String user_phones){
        StringBuffer buffer = new StringBuffer();
        StringBuffer phoneBuffer = new StringBuffer();
        String warning="@all";
        Arrays.stream(user_phones.split(",")).forEach(it->{
            phoneBuffer.append("\"").append(it).append("\"").append(",");

        });
        if(StringUtils.endsWith(phoneBuffer.toString(),",")){
            warning=phoneBuffer.toString().substring(0,phoneBuffer.toString().lastIndexOf(","));
        }
        buffer.append("{").append("\n");
        switch (type) {
            case "markdown":{
                buffer.append("\"msgtype\":\"text\"").append(",").append("\n");
                buffer.append("\"text\":{").append("\n");
                buffer.append("\"content\":").append("\"").append(context).append("\"").append("\n");
                break;
            }
            case "text":
            {

                buffer.append("\"msgtype\":\"text\"").append(",").append("\n");
                buffer.append("\"text\":{").append("\n");
                buffer.append("\"content\":").append("\"").append(context).append("\"").append(",").append("\n");
                buffer.append("\"mentioned_list\":["+warning+"]").append(",").append("\n");
                buffer.append("\"mentioned_mobile_list\":["+warning+"]").append("\n");
                break;
            }
        }
        buffer.append("}").append("\n")
                .append("}");
        return buffer.toString();
    }
    public static String initMsgInfo(String context, String type,String user_phones,int isWarning){
        StringBuffer buffer = new StringBuffer();
        StringBuffer phoneBuffer = new StringBuffer();
        String warning="@all";
        Arrays.stream(user_phones.split(",")).forEach(it->{
            phoneBuffer.append("\"").append(it).append("\"").append(",");

        });
        if(StringUtils.endsWith(phoneBuffer.toString(),",")){
            warning=phoneBuffer.toString().substring(0,phoneBuffer.toString().lastIndexOf(","));
        }
        buffer.append("{").append("\n");
        if(isWarning==0){
            warning="";
        }
        switch (type) {
            case "markdown":{
                buffer.append("\"msgtype\":\"text\"").append(",").append("\n");
                buffer.append("\"text\":{").append("\n");
                buffer.append("\"content\":").append("\"").append(context).append("\"").append("\n");
                break;
            }
            case "text":
            {

                buffer.append("\"msgtype\":\"text\"").append(",").append("\n");
                buffer.append("\"text\":{").append("\n");
                buffer.append("\"content\":").append("\"").append(context).append("\"").append(",").append("\n");
                buffer.append("\"mentioned_list\":["+warning+"]").append(",").append("\n");
                buffer.append("\"mentioned_mobile_list\":["+warning+"]").append("\n");
                break;
            }
        }
        buffer.append("}").append("\n")
                .append("}");
        return buffer.toString();
    }
    /**
     * 发送消息给机器人
     */
    public static void sendMessage(String context, String type, Properties pro) throws Exception {
        List<RobootRerson> allRobootRerson = getAllRobootRerson(pro);
        allRobootRerson.forEach(it->{
            String msg = initMsgInfo(context, type,it.getUser_phones());
            System.out.println(msg);
            HTTPUtils.sendPost(it.getHookurl(),msg);
        });
    }

    public static void sendMessage(String context, String type, Properties pro,int isWarning) throws Exception {
        List<RobootRerson> allRobootRerson = getAllRobootRerson(pro);
        allRobootRerson.forEach(it->{
            String msg = initMsgInfo(context, type,it.getUser_phones(),isWarning);
            System.out.println(msg);
            HTTPUtils.sendPost(it.getHookurl(),msg);
        });
    }

}
