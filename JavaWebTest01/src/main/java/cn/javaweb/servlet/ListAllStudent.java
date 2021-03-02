package cn.javaweb.servlet;

import cn.javaweb.bean.Student;
import cn.javaweb.dao.StudentDao;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author 魏喜明 2021-01-31 22:36:45
 */
public class ListAllStudent extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        StudentDao studentDao = new StudentDao();
        List<Student> studentList = studentDao.getAll();

        req.setAttribute("students", studentList);
        req.getRequestDispatcher("/student.jsp").forward(req, resp);
    }
}
