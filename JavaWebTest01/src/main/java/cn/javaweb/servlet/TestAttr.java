package cn.javaweb.servlet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * @author 魏喜明 2021-01-28 00:12:00
 */
public class TestAttr extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        System.out.println("TestAttr's Servlet!");

        resp.getWriter().println("TestAttr's " + req.getAttribute("name"));

        PrintWriter writer = resp.getWriter();
        // 在 Servlet 中无法获取到 pageContext 对象
        // request
        Object attribute = req.getAttribute("requestAttr");
        writer.println("requestAttr: " + attribute);
        writer.println("<br><br>");
        // session
        Object sessionAttr = req.getSession().getAttribute("sessionAttr");
        writer.println("sessionAttr: " + sessionAttr);
        writer.println("<br><br>");
        // application
        Object applicationAttr = getServletContext().getAttribute("applicationAttr");
        writer.println("applicationAttr: " + applicationAttr);
        writer.println("<br><br>");
    }
}
