package cn.mine.springboot.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * @author 魏喜明 2021-05-04 19:19:20
 */
public class MyListener implements ServletContextListener {
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        System.out.println("MyListener\tcontextInitialized\t" + sce);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        System.out.println("MyListener\tcontextDestroyed\t" + sce);
    }
}
