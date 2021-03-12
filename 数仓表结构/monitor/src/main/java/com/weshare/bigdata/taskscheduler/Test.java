package com.weshare.bigdata.taskscheduler;

/**
 * created by chao.guo on 2021/3/10
 **/
public class Test {
    public static void main(String[] args) {

        StringBuffer buffer = new StringBuffer();
        buffer.append("{").append("\n");
        buffer.append("\"msgtype\":\"text\"").append(",").append("\n");
        buffer.append("text:{").append("\n");
        buffer.append("\"content\":").append("\"").append("大鸡大利").append("\"").append("\n");
        buffer.append("}").append("\n")
                .append("}");
        System.out.println(buffer.toString());


    }
}
