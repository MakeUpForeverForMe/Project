package cn.javaweb.servlet;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Map;

/**
 * @author 魏喜明 2021-01-26 00:52:53
 */
public class LoginServlet extends MyHttpServlet {
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        System.out.println("Http 请求来了！");
        String user = request.getParameter("user");
        String pass = request.getParameter("pass");
        System.out.println("String --> " + user + "\t" + pass);

        String initUser = getInitParameter("user");
        String initPass = getInitParameter("pass");
        System.out.println("String Init --> " + initUser + "\t" + initPass);

        PrintWriter printWriter = response.getWriter();
        if (initUser.equals(user) && initPass.equals(pass)) printWriter.println("Hello " + user);
        else printWriter.println("Sorry " + user + ",you are not " + initUser);

        String[] interestings = request.getParameterValues("interesting");
        if (interestings != null) {
            for (String interesting : interestings) {
                System.out.println("Strings --> " + interesting);
            }
        }
        Enumeration<String> parameterNames = request.getParameterNames();
        while (parameterNames.hasMoreElements()) {
            System.out.println("Enum --> " + parameterNames.nextElement());
        }
        Map<String, String[]> parameterMap = request.getParameterMap();
        for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
            System.out.println("Map --> " + entry.getKey() + "\t" + Arrays.asList(entry.getValue()));
        }
        System.out.println(request.getRequestURI());
        System.out.println(request.getMethod());
        System.out.println(request.getQueryString());

        PrintWriter writer = response.getWriter();
        writer.print("Hello World!!!");
    }
}
