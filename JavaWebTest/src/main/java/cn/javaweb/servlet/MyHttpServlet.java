package cn.javaweb.servlet;

import com.sun.deploy.net.HttpResponse;

import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * 针对 Http 协议定义的一个 Servlet 类
 *
 * @author 魏喜明 2021-01-26 21:41:20
 */
public class MyHttpServlet extends MyGenericServlet {
    @Override
    public void service(ServletRequest servletRequest, ServletResponse servletResponse) throws IOException {
        System.out.println("void service(ServletRequest servletRequest, ServletResponse servletResponse)");
        if (servletRequest instanceof HttpServletRequest) {
            HttpServletRequest httpServletRequest = (HttpServletRequest) servletRequest;
            if (servletResponse instanceof HttpResponse) {
                HttpServletResponse httpServletResponse = (HttpServletResponse) servletResponse;
                httpService(httpServletRequest, httpServletResponse);
            }
        }
    }

    public void httpService(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException {
        String requestMethod = httpServletRequest.getMethod();
        switch (requestMethod.toLowerCase()) {
            case "get":
                doGet(httpServletRequest, httpServletResponse);
                break;
            case "post":
                doPost(httpServletRequest, httpServletResponse);
                break;
            default:
                System.out.println("Undefined!");
        }
    }

    public void doGet(HttpServletRequest request, HttpServletResponse response) {
        System.out.println(request + "\t" + response);
    }

    public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        System.out.println(request + "\t" + response);
    }
}
