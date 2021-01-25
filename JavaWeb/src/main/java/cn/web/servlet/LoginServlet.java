package cn.web.servlet;

import javax.servlet.*;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;

/**
 * @author 魏喜明 2021-01-26 00:52:53
 */
public class LoginServlet implements Servlet {
    @Override
    public void init(ServletConfig servletConfig) {

    }

    @Override
    public ServletConfig getServletConfig() {
        return null;
    }

    @Override
    public void service(ServletRequest servletRequest, ServletResponse servletResponse) {
        System.out.println("请求来了！");
        String user = servletRequest.getParameter("user");
        String pass = servletRequest.getParameter("pass");
        System.out.println("String --> " + user + "\t" + pass);
        String[] interestings = servletRequest.getParameterValues("interesting");
        for (String interesting : interestings) {
            System.out.println("Strings --> " + interesting);
        }
        Enumeration<String> parameterNames = servletRequest.getParameterNames();
        while (parameterNames.hasMoreElements()) {
            System.out.println("Enum --> " + parameterNames.nextElement());
        }
        Map<String, String[]> parameterMap = servletRequest.getParameterMap();
        for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
            System.out.println("Map --> " + entry.getKey() + "\t" + Arrays.asList(entry.getValue()));
        }
        HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
        System.out.println(httpServletRequest.getRequestURI());
        System.out.println(httpServletRequest.getMethod());
        System.out.println(httpServletRequest.getQueryString());
    }

    @Override
    public String getServletInfo() {
        return null;
    }

    @Override
    public void destroy() {

    }
}
