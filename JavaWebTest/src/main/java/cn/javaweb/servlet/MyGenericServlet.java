package cn.javaweb.servlet;

import javax.servlet.*;
import java.io.IOException;
import java.util.Enumeration;

/**
 * 自定义的 Servlet 接口实现类：让开发的任何 Servlet 都继承该类，简化开发
 * GenericServlet 核心功能的手动实现
 *
 * @author 魏喜明 2021-01-26 21:07:01
 */
public abstract class MyGenericServlet implements Servlet, ServletConfig {
    private ServletConfig servletConfig;

    /* 以下为 Servlet 接口的方法 */
    @Override
    public void init(ServletConfig servletConfig) {
        System.out.println("init(ServletConfig servletConfig)");
        this.servletConfig = servletConfig;
    }

    @Override
    public ServletConfig getServletConfig() {
        System.out.println("ServletConfig getServletConfig()");
        return this.servletConfig;
    }

    @Override
    public abstract void service(ServletRequest servletRequest, ServletResponse servletResponse) throws ServletException, IOException;

    @Override
    public String getServletInfo() {
        System.out.println("String getServletInfo()");
        return null;
    }

    @Override
    public void destroy() {
        System.out.println("void destroy()");
    }

    /* 以下为 ServletConfig 接口的方法 */
    @Override
    public String getServletName() {
        System.out.println("String getServletName()");
        return this.getServletConfig().getServletName();
    }

    @Override
    public ServletContext getServletContext() {
        System.out.println("ServletContext getServletContext()");
        return this.getServletConfig().getServletContext();
    }

    @Override
    public String getInitParameter(String s) {
        System.out.println("String getInitParameter(String s)");
        return this.getServletConfig().getInitParameter(s);
    }

    @Override
    public Enumeration<String> getInitParameterNames() {
        System.out.println("Enumeration<String> getInitParameterNames()");
        return this.getServletConfig().getInitParameterNames();
    }
}
