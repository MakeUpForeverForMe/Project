package com.weshare.bigdata.domain;

/**
 * created by chao.guo on 2021/2/23
 **/
public class RobootRerson {
    private int id;
    private String hookurl;
    private int isEnable;
    private String user_phones;


    public RobootRerson() {
    }

    public RobootRerson(int id, String hookurl, int isEnable, String phones) {
        this.id = id;
        this.hookurl = hookurl;
        this.isEnable = isEnable;
        this.user_phones = phones;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getHookurl() {
        return hookurl;
    }

    public void setHookurl(String hookurl) {
        this.hookurl = hookurl;
    }

    public int getIsEnable() {
        return isEnable;
    }

    public void setIsEnable(int isEnable) {
        this.isEnable = isEnable;
    }

    public String getUser_phones() {
        return user_phones;
    }

    public void setUser_phones(String user_phones) {
        this.user_phones = user_phones;
    }
}
