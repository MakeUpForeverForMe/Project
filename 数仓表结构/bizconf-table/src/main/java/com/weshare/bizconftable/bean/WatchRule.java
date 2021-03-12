package com.weshare.bizconftable.bean;

import java.io.Serializable;

/**
 * created by chao.guo on 2021/2/1
 **/
public class WatchRule implements Serializable {
    private  String task_id ;
    private String task_name;
    private  String task_param;
    private String dim_fileds;
    private String task_sql;
    private String task_sql_check;
    private String engine_type;
    private  String recivers_email;
    private String mode_name;

    public String getTask_id() {
        return task_id;
    }

    public void setTask_id(String task_id) {
        this.task_id = task_id;
    }

    public String getTask_name() {
        return task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public String getTask_param() {
        return task_param;
    }

    public void setTask_param(String task_param) {
        this.task_param = task_param;
    }

    public String getDim_fileds() {
        return dim_fileds;
    }

    public void setDim_fileds(String dim_fileds) {
        this.dim_fileds = dim_fileds;
    }

    public String getTask_sql() {
        return task_sql;
    }

    public void setTask_sql(String task_sql) {
        this.task_sql = task_sql;
    }

    public String getTask_sql_check() {
        return task_sql_check;
    }

    public void setTask_sql_check(String task_sql_check) {
        this.task_sql_check = task_sql_check;
    }

    public String getEngine_type() {
        return engine_type;
    }

    public void setEngine_type(String engine_type) {
        this.engine_type = engine_type;
    }

    public String getRecivers_email() {
        return recivers_email;
    }

    public void setRecivers_email(String recivers_email) {
        this.recivers_email = recivers_email;
    }

    public String getMode_name() {
        return mode_name;
    }

    public void setMode_name(String mode_name) {
        this.mode_name = mode_name;
    }
}
