package cn.javaweb.servlet;

import cn.javaweb.dao.StudentDao;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * @author 魏喜明 2021-02-02 00:17:09
 */
public class DeleteStudent extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String flowId = req.getParameter("flowId");

        StudentDao studentDao = new StudentDao();
        studentDao.deleteByFlowId(Integer.parseInt(flowId));

        req.getRequestDispatcher("/success.jsp").forward(req, resp);
    }
}
