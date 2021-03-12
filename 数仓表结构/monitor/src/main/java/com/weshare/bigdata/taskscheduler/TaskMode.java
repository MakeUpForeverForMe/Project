package com.weshare.bigdata.taskscheduler;

/**
 * created by chao.guo on 2021/2/19
 **/
public class TaskMode {
    public String job_id ;
    public String job_name ;
    public String oozieAdress ;
    public String status; // NONE READY RUN FINISH FAIL
    public String batch_date;
    public int rn;
    public String mode_type; // 模块类型core 核心   bigdata 数据中台 data_check

    public int getRn() {
        return rn;
    }

    public void setRn(int rn) {
        this.rn = rn;
    }

    public String getBatch_date() {
        return batch_date;
    }

    public void setBatch_date(String batch_date) {
        this.batch_date = batch_date;
    }

    public TaskMode() {
    }

    public String getMode_type() {
        return mode_type;
    }

    public void setMode_type(String mode_type) {
        this.mode_type = mode_type;
    }

    public TaskMode(String job_id, String job_name, String oozieAdress, String status, String batch_date, int rn,String mode_type) {
        this.job_id = job_id;
        this.job_name = job_name;
        this.oozieAdress = oozieAdress;
        this.status = status;
        this.batch_date=batch_date;
        this.rn=rn;
        this.mode_type=mode_type;
    }

    public String getJob_id() {
        return job_id;
    }

    public void setJob_id(String job_id) {
        this.job_id = job_id;
    }

    public String getJob_name() {
        return job_name;
    }

    public void setJob_name(String job_name) {
        this.job_name = job_name;
    }

    public String getOozieAdress() {
        return oozieAdress;
    }

    public void setOozieAdress(String oozieAdress) {
        this.oozieAdress = oozieAdress;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "TaskMode{" +
                "job_id='" + job_id + '\'' +
                ", job_name='" + job_name + '\'' +
                ", oozieAdress='" + oozieAdress + '\'' +
                ", status='" + status + '\'' +
                ", batch_date='" + batch_date + '\'' +
                ", rn=" + rn +
                '}';
    }

    public String getHtml(){

        return
                "job_name:"+job_name+"\n"
                        +"batch_date:"+batch_date+"\n"
                        +"status:"+status+"\n";

    }


}
