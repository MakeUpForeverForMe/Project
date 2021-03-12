package com.weshare.bigdata.taskscheduler;

import com.weshare.bigdata.util.DruidDataSourceUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * created by chao.guo on 2021/2/19
 **/
public class TaskSceduleService {
    /**
     * 从cm的测试库 查询出 今天还未进行抽数的任务
     * @param batch_date
     * @return
     * @throws SQLException
     */
    public List<TaskMode> getTaskMode(String batch_date, Properties pro) throws SQLException {
        ArrayList<TaskMode> taskModes = new ArrayList<>();
        String task_schedule_table=pro.getProperty("task_scheduler_table");
        String task_info_table=pro.getProperty("task_info_table");
        Connection cm_mysql = DruidDataSourceUtils.getConection("cm_mysql");
        Statement statement = cm_mysql.createStatement();
        ResultSet resultSet = statement.executeQuery(
                "select \n" +
                        "                scheduler.job_id,\n" +
                        "                scheduler.job_name,\n" +
                        "                scheduler.oozieAdress ,\n" +
                        "                scheduler.rn ,\n" +
                        "\t\t\t\tif(info.run_status is null,\"NONE\",info.run_status) as run_status ,\n" +
                        "scheduler.mode_type"+
                        "                from \n" +
                        "                "+task_schedule_table +" scheduler\n" +
                        "                left join (select * from "+task_info_table+" where batch_date='"+batch_date+"') info on scheduler.job_id=info.job_id");
        while (resultSet.next()) {
            String job_id = resultSet.getString("job_id");
            String job_name = resultSet.getString("job_name");
            String oozieAdress = resultSet.getString("oozieAdress");
            int rn = resultSet.getInt("rn");
            taskModes.add( new TaskMode(job_id,job_name,oozieAdress,resultSet.getString("run_status"),batch_date,rn,resultSet.getString("mode_type")));
        }
        return getFinishTaskMode(taskModes,batch_date);
    }

    /**
     * 查询业务库 已跑完的批量 并将状态从NONE---->READY 就绪
     * @param list
     * @param batch_date
     * @return
     * @throws SQLException
     */
    public List<TaskMode> getFinishTaskMode(List<TaskMode> list,String batch_date) throws SQLException {
        if(null!=list && list.size()>0){
            String job_ids = concatJobId(list);
            Connection pro_mysql = DruidDataSourceUtils.getConection("pro_mysql");
            Statement statement = pro_mysql.createStatement();
            if(null!=System.getenv() &&  null!=System.getenv().get("OS") && System.getenv().get("OS").startsWith("Windows") ) {
                statement.execute("use ecas_t_20");
            }else{
                statement.execute("use ecasdb");
            }

        //查询核心数据库已经跑完的批量
            ResultSet resultSet = statement.executeQuery("" +
                    "select  exe.job_execution_id,ins.JOB_NAME,exe.START_TIME,exe.END_TIME,exe.STATUS,DATE_FORMAT(param.DATE_VAL,'%Y-%m-%d') as batch_date \n" +
                    "from kb_job_execution exe,kb_job_instance ins,kb_job_execution_params param \n" +
                    "      where  ins.JOB_INSTANCE_ID = exe.JOB_INSTANCE_ID and param.JOB_EXECUTION_ID = exe.JOB_EXECUTION_ID and ins.job_name in (" + job_ids + ")  \n" +
                    "\t  and param.KEY_NAME = 'batch.date' and DATE_FORMAT(param.DATE_VAL,'%Y-%m-%d')='" + batch_date + "' and exe.STATUS='COMPLETED'" +
                    "" +
                    "");
            while (resultSet.next()) {  // 如果存在已经跑完的批量 则将 状态从NONE-->READY  准备运行
                String job_id = resultSet.getString("JOB_NAME");
                list.forEach(it->{
                    if(job_id.equals(it.job_id) && "NONE".equals(it.status)){
                        it.setStatus("READY");
                    }
                });

            }
            return list;
        }
    return  list;
    }

    /**
     * 处理jobid
     * @param list
     * @return
     */
    public String concatJobId(List<TaskMode> list){
        StringBuffer buffer = new StringBuffer("");
        String temp="";
        list.forEach(it->{
            buffer.append("'").append(it.job_id).append("'").append(",");
        });

       if(buffer.toString().endsWith(",")){
           temp= buffer.toString().substring(0,buffer.toString().lastIndexOf(","));

       }
    return temp;
    }


}
