package cn.javaweb.servlet;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;

/**
 * @author 魏喜明 2021-01-26 00:52:53
 */
public class LoginServlet2 extends MyGenericServlet {
    @Override
    public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws IOException {
        System.out.println("请求来了！");
        String user = servletRequest.getParameter("user");
        String pass = servletRequest.getParameter("pass");
        System.out.println("String --> " + user + "\t" + pass);

        String initUser = getInitParameter("user");
        String initPass = getInitParameter("pass");
        System.out.println("String Init --> " + initUser + "\t" + initPass);

        PrintWriter printWriter = servletResponse.getWriter();
        if (initUser.equals(user) && initPass.equals(pass)) printWriter.println("Hello " + user);
        else printWriter.println("Sorry " + user + ",you are not " + initUser);

        String[] interestings = servletRequest.getParameterValues("interesting");
        if (interestings != null) {
            for (String interesting : interestings) {
                System.out.println("Strings --> " + interesting);
            }
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

        PrintWriter writer = servletResponse.getWriter();
        writer.print("Hello World!!!");
    }
}
