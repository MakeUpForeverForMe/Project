package com.travel;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@MapperScan("com.travel.dao")
@SpringBootApplication
public class OrderMonitorApp {
    public static void main(String[] args) {
        SpringApplication.run(OrderMonitorApp.class, args);
    }
}
