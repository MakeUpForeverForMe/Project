package com.weshare.bizconftable.bean;



import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;

/**
 * Created by mouzwang on 2021-01-19 17:42
 */
public class SitBizConf implements Serializable {

    @Column
    private String col1Name;
    @Column
    private String col1Comment;
    @Column
    private String col1Val;
    @Column
    private String col2Name;
    @Column
    private String col2Comment;
    @Column
    private String col2Val;
    @Id
    private String colId;

    public String getColId() {
        return colId;
    }

    public void setColId(String colId) {
        this.colId = colId;
    }

    public String getCol1Name() {
        return col1Name;
    }

    public void setCol1Name(String col1Name) {
        this.col1Name = col1Name;
    }

    public String getCol1Comment() {
        return col1Comment;
    }

    public void setCol1Comment(String col1Comment) {
        this.col1Comment = col1Comment;
    }

    public String getCol1Val() {
        return col1Val;
    }

    public void setCol1Val(String col1Val) {
        this.col1Val = col1Val;
    }

    public String getCol2Name() {
        return col2Name;
    }

    public void setCol2Name(String col2Name) {
        this.col2Name = col2Name;
    }

    public String getCol2Comment() {
        return col2Comment;
    }

    public void setCol2Comment(String col2Comment) {
        this.col2Comment = col2Comment;
    }

    public String getCol2Val() {
        return col2Val;
    }

    public void setCol2Val(String col2Val) {
        this.col2Val = col2Val;
    }

    @Override
    public String toString() {
        return "BizConf{" +
                "colId='" + colId + '\'' +
                ", col1Name='" + col1Name + '\'' +
                ", col1Comment='" + col1Comment + '\'' +
                ", col1Val='" + col1Val + '\'' +
                ", col2Name='" + col2Name + '\'' +
                ", col2Comment='" + col2Comment + '\'' +
                ", col2Val='" + col2Val + '\'' +
                '}';
    }
}
