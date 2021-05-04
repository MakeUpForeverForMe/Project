package cn.mine.springboot.config;

import cn.mine.springboot.filter.MyFilter;
import cn.mine.springboot.listener.MyListener;
import cn.mine.springboot.servlet.MyServlet;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

/**
 * @author 魏喜明 2021-05-04 19:04:18
 */
@Configuration
public class MyServerConfig {

    @Bean
    public ServletRegistrationBean myServer() {
        ServletRegistrationBean<MyServlet> myServletServletRegistrationBean = new ServletRegistrationBean<>(new MyServlet(), "/myServlet");
        myServletServletRegistrationBean.setLoadOnStartup(1);
        return myServletServletRegistrationBean;
    }

    @Bean
    public FilterRegistrationBean myFilter() {
        FilterRegistrationBean filterRegistrationBean = new FilterRegistrationBean();
        filterRegistrationBean.setFilter(new MyFilter());
        filterRegistrationBean.setUrlPatterns(Arrays.asList("/hello", "/myServlet"));
        return filterRegistrationBean;
    }

    @Bean
    public ServletListenerRegistrationBean myListener() {
        return new ServletListenerRegistrationBean<>(new MyListener());
    }
}
