package cn.javaweb.mvc;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author 魏喜明 2021-01-31 22:36:45
 */
public class ListAllStudent extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        req.setAttribute("students", Arrays.asList("AA", "BB", "CC"));
        req.getRequestDispatcher("/student.jsp").forward(req, resp);
    }
}
