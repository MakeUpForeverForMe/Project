package cn.javaweb.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author 魏喜明 2021-01-28 22:22:38
 */
public class RedirectServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        System.out.println("RedirectServlet's Servlet!");

        req.setAttribute("name", "abc RedirectServlet");
        resp.getWriter().println("RedirectServlet's " + req.getAttribute("name"));
        // 执行请求的重定向，直接调用 response 的 sendRedirect(path) 方法
        // path 为要重定向的地址
        String path = "testAttr";
        resp.sendRedirect(path);
    }
}
