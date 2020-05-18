package com.travel.common;

import java.io.Serializable;

/**
 * 行政区域
 */
public class District implements Serializable {
    private String citycode;
    private String name;
    //行政区域边界的一圈经纬度字符串
    private String polygon;//将边界的经纬度全部都放到这个字符串里面去，到时候通过一些图形的API获取一个形状

    public District(String citycode, String name, String polygon) {
        this.citycode = citycode;
        this.name = name;
        this.polygon = polygon;
    }

    public String getCitycode() {
        return citycode;
    }

    public void setCitycode(String citycode) {
        this.citycode = citycode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPolygon() {
        return polygon;
    }

    public void setPolygon(String polygon) {
        this.polygon = polygon;
    }
}
