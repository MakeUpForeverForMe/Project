package com.weshare.bizconftable;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import tk.mybatis.spring.annotation.MapperScan;

@SpringBootApplication
//@MapperScan(basePackages = "com.weshare.bizconftable.mapper")
public class BizconfTableApplication {

    public static void main(String[] args) {
        SpringApplication.run(BizconfTableApplication.class, args);
    }

}
