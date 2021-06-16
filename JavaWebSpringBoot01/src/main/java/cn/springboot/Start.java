package cn.springboot;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@MapperScan(value = "cn.mine.springboot.mapper")
@SpringBootApplication
public class Start {
//
//    private final RestTemplateBuilder builder;
//
//    public JavawebSpringboot01Application(RestTemplateBuilder builder) {
//        this.builder = builder;
//    }
//
//    @Bean
//    public RestTemplate restTemplate() {
//        return builder.build();
//    }

    public static void main(String[] args) {
        SpringApplication.run(Start.class, args);
    }
}
