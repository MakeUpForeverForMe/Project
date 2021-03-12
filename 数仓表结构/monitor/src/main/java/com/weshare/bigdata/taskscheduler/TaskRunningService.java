package com.weshare.bigdata.taskscheduler;


import com.weshare.bigdata.util.DruidDataSourceUtils;
import com.weshare.bigdata.util.MsgUtils;
import com.weshare.bigdata.util.SendEmailUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.oozie.client.WorkflowJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * created by chao.guo on 2021/2/19
 * / 1 根据批次时间修改 kudu 表
 * 2  读取已就绪的task的oozie 任务 提交任务
 *
 *
 **/
public class TaskRunningService {
    //调用oozie 客户端提交现已准备就绪的任务 然后进行等待
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRunningService.class);



    /**
     * 任务单个执行   每次只执行一个准备好的任务
     * @param taskModes
     * @throws Exception
     */
    public void runingJob(List<TaskMode> taskModes,String type,Properties pro) throws Exception {
        // 排序加过滤出 任务
        if(StringUtils.equals("core",type)){
            taskModes=taskModes.stream().filter(it -> StringUtils.equals(type, it.mode_type)).sorted(new Comparator<TaskMode>() {
                @Override
                public int compare(TaskMode o1, TaskMode o2) {
                    return o1.rn - o2.rn;
                }
            }).collect(Collectors.toList());
        }else{
            taskModes=taskModes.stream().filter(it -> StringUtils.equals(type, it.mode_type)).map(new Function<TaskMode, TaskMode>() {
                @Override
                public TaskMode apply(TaskMode taskMode) {
                    if (StringUtils.equals("NONE", taskMode.status)) { //未执行的
                        taskMode.setStatus("READY");
                    }
                    return taskMode;
                }
            }).sorted(new Comparator<TaskMode>() {
                @Override
                public int compare(TaskMode o1, TaskMode o2) {
                    return o1.rn - o2.rn;
                }
            }).collect(Collectors.toList());;
        }

        AtomicBoolean is_running = new AtomicBoolean(false);
        List<TaskMode> readyRunList =new LinkedList<>();
        taskModes.forEach(it->{
            if("RUN".equals(it.status)){ //如果有任务再执行则直接退出啥也不做
                is_running.set(true);
                LOGGER.info("任务正在运行:{}",it);
            }
            if("READY".equals(it.status)){ //就绪的抽数任务
                readyRunList.add(it);
            }
        });
        //针对list进行排序 保证1期最后一个执行 因数据校验再第一期 的跑批任务上
        if(!is_running.get() && readyRunList.size()>0){ // 没有正在运行的任务 且 有准备运行的任务
            TaskMode taskMode = readyRunList.get(0);
            taskMode.setStatus("RUN");
            LOGGER.info("开始执行任务:{},标记该任务为执行中{}",taskMode,"RUN");
            // insert 之前先删除今天 保证一天只有一条数据
            executeStatusSQL("delete",taskMode,pro);
            executeStatusSQL("insert",taskMode,pro);
            int isHa=Integer.parseInt(pro.getProperty("isHa"));
            WorkflowJob.Status status = OozieApplicationSubmit.subitOozieJob(taskMode.oozieAdress,isHa);
            if(status==WorkflowJob.Status.SUCCEEDED){
                LOGGER.info("任务执行结束:{},标记该任务为执行结束{}",taskMode,"FINISH");
                taskMode.setStatus("FINISH");
            }else{
                LOGGER.info("任务执行失败:{},标记该任务为执行失败{}",taskMode,"FAIL");
                taskMode.setStatus("FAIL");
            }
            executeStatusSQL("update",taskMode,pro);
            // 发送邮件告知任务执行结果
            String msg_title = pro.getProperty("msg_title");
            SendEmailUtil.sendEmail(msg_title+" oozie sqoop schedule task",msg_title+"-----Sqoop---- "+"\n"+taskMode.getHtml()+"\n"+"任务执行："+taskMode.status);
            if("FINISH".equals(taskMode.status)){
                MsgUtils.sendMessage(msg_title+":"+taskMode.getHtml()+"\n"+"任务执行："+taskMode.status,"markdown",pro);
            }else{
                MsgUtils.sendMessage(msg_title+":"+taskMode.getHtml()+"\n"+"任务执行："+taskMode.status,"text",pro);
            }
        }



    }


    /**
     * 修改任务执行日志表的记录信息
     * @param type
     * @param taskMode
     * @throws SQLException
     */
 void executeStatusSQL(String type,TaskMode taskMode,Properties pro) throws Exception {
     String task_info_table=pro.getProperty("task_info_table");
     String INSERT_SQL="insert into "+task_info_table+" (job_id,job_name,batch_date,run_status,start_time,end_time) values (?,?,?,?,?,?)";
     System.out.println(INSERT_SQL);
     String UPDATE_SQL="update "+task_info_table+"  set run_status=?,end_time=? where batch_date=? and job_id=?";
     System.out.println(UPDATE_SQL);
     String DELETE_SQL="delete from "+task_info_table+"  where batch_date=? and job_id=?";
     System.out.println(DELETE_SQL);
    Connection cm_mysql = DruidDataSourceUtils.getConection("cm_mysql");
     SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
     switch (type){
            case "insert":
                PreparedStatement preparedStatement = cm_mysql.prepareStatement(INSERT_SQL);
                preparedStatement.setString(1,taskMode.job_id);
                preparedStatement.setString(2,taskMode.job_name);
                preparedStatement.setDate(3,new Date(format.parse(taskMode.batch_date).getTime()));
                preparedStatement.setString(4,taskMode.status);
                preparedStatement.setTimestamp(5,new Timestamp(System.currentTimeMillis()));
                preparedStatement.setTimestamp(6,null);
                preparedStatement.execute();
                break;
        case "update":{
            PreparedStatement statement = cm_mysql.prepareStatement(UPDATE_SQL);
            statement.setString(1,taskMode.status);
            statement.setTimestamp(2,new Timestamp(System.currentTimeMillis()));
            statement.setDate(3,new Date(format.parse(taskMode.batch_date).getTime()));
            statement.setString(4,taskMode.job_id);
            statement.execute();
            break;
        }
        case "delete":{
            PreparedStatement statement = cm_mysql.prepareStatement(DELETE_SQL);
            statement.setDate(1,new Date(format.parse(taskMode.batch_date).getTime()));
            statement.setString(2,taskMode.job_id);
            statement.execute();
            break;
        }

    }


}
    /**
     * 判断 在同一类型下的所有任务是否全部执行完毕 执行完毕则返回true 否则返回false
     * @param taskMode
     * @param
     * @return
     */
    public boolean judgeJobIsAllFinish(List<TaskMode> taskMode,String type) {
        AtomicBoolean is_running = new AtomicBoolean(false);
        taskMode.forEach(it -> {
            if (!"FINISH".equals(it.status) && StringUtils.equals(type, it.mode_type)) { // 改类型下的所有任务是否执行完
                is_running.set(true);
                LOGGER.info("有任务未完成,跳过执行其他任务{},{}", taskMode, it.status);
            }

        });
        return !is_running.get(); //为true 代表该 类型的任务已全部执行完毕
    }
}
