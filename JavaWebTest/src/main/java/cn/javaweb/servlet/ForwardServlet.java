package cn.javaweb.servlet;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author 魏喜明 2021-01-28 22:11:33
 */
public class ForwardServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        System.out.println("ForwardServlet's doGet!");

        req.setAttribute("name", "abc ForwardServlet");
        resp.getWriter().println("ForwardServlet's " + req.getAttribute("name"));
        // 请求的转发
        // 1、调用 HttpServletRequest 的 getRequestDispatcher 方法获取转发器对象
        // 调用 getRequestDispatcher 需要传入转发地址
        String path = "testAttr";
        RequestDispatcher requestDispatcher = req.getRequestDispatcher("/" + path);
        // 2、调用 RequestDispatcher 的 forward(request,response) 进行请求的转发
        requestDispatcher.forward(req, resp);
    }
}
