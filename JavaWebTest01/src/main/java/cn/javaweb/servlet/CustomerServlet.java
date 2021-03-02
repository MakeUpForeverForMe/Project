package cn.javaweb.servlet;

import cn.javaweb.bean.Customer;
import cn.javaweb.dao.CustomerDAO;
import cn.javaweb.dao.CustomerDAOFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 * @author 魏喜明 2021-02-04 23:38:01
 */
public class CustomerServlet extends HttpServlet {
    private final CustomerDAO customerDAO = CustomerDAOFactory.getInstance().getCustomerDAO();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws UnsupportedEncodingException {
        System.out.println("CustomerServlet\tdoGet");
        System.out.println("CustomerServlet\tdoGet\t参数为：" + request.getServletPath());
        doPost(request, response);
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws UnsupportedEncodingException {
//        String method = request.getParameter("method");
//        switch (method) {
//            case "add":
//                add(request, response);
//                break;
//            case "query":
//                query(request, response);
//                break;
//            case "delete":
//                delete(request, response);
//                break;
//            default:
//                error(request, response);
//
//        }

        request.setCharacterEncoding("UTF-8");
        String servletPath = request.getServletPath();
        String methodName = servletPath.substring(1, servletPath.length() - 3);
        System.out.println("CustomerServlet\tdoPost\t参数为：" + methodName);
        try {
            Method method = this.getClass().getDeclaredMethod(methodName, HttpServletRequest.class, HttpServletResponse.class);
            method.invoke(this, request, response);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    private void edit(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        System.out.println("CustomerServlet\tedit");
        String forwardPath = "";
        int flowId;
        Customer customer = null;
        try {
            flowId = Integer.parseInt(request.getParameter("flowId"));
            customer = customerDAO.get(flowId);
            if (customer != null) {
                forwardPath = "/update.jsp";
                request.setAttribute("customer", customer);
            }
        } catch (NumberFormatException e) {
            System.out.println("请求的页面不存在！");
        }
        request.getRequestDispatcher(forwardPath).forward(request, response);
        System.out.println("CustomerServlet\tedit\t" + customer);
    }

    private void update(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        System.out.println("CustomerServlet\tupdate");
        String flowId = request.getParameter("flowId");
        String name = request.getParameter("name");
        String phone = request.getParameter("phone");
        String address = request.getParameter("address");
        String oldName = request.getParameter("oldName");

        int id = 0;
        try {
            id = Integer.parseInt(flowId);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        Customer customer;
        long countWithName = customerDAO.getCountWithName(name);
        if ((!name.equalsIgnoreCase(oldName)) && countWithName > 0) {
            String duplicate = "用户：'" + name + "' 已存在，请重新输入！";
            customer = new Customer(id, oldName, address, phone);
            request.setAttribute("customer", customer);
            request.setAttribute("duplicate", duplicate);
            request.getRequestDispatcher("/update.jsp").forward(request, response);
            System.out.println(duplicate + "\t" + customer);
        } else {
            customer = new Customer(id, name, address, phone);
            customerDAO.update(customer);
            response.sendRedirect("query.do");
            System.out.println("CustomerServlet\tupdate\t" + customer);
        }
    }

    private void add(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        System.out.println("CustomerServlet\tadd");

        String name = request.getParameter("name");
        long count = customerDAO.getCountWithName(name);
        String duplicate = "用户：'" + name + "' ";
        if (count > 0) {
            duplicate += "已存在，请重新输入！";
        } else {
            String address = request.getParameter("address");
            String phone = request.getParameter("phone");
            Customer customer = new Customer(name, address, phone);
            System.out.println(customer);
            customerDAO.save(customer);
            duplicate += "添加成功！";
        }
        request.setAttribute("duplicate", duplicate);
        request.getRequestDispatcher("/addCustomer.jsp").forward(request, response);
    }

    private void query(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        System.out.println("CustomerServlet\tquery");

        String name = request.getParameter("name");
        String address = request.getParameter("address");
        String phone = request.getParameter("phone");

        request.getParameterMap().forEach(
                (k, v) -> System.out.printf(
                        "CustomerServlet\tquery\tparameter：%-10s\tvalues%s\n",
                        k, Arrays.toString(v)
                )
        );

        Customer customer = new Customer(name, address, phone);

        // 调用 CustomerDAO 的 getAll 方法，得到 Customer 集合
        List<Customer> customers = customerDAO.getAllWithCriteriaCustomer(customer);
        // 把 Customer 集合放入到 request 中
        request.setAttribute("customers", customers);
        // 转发页面到 index.jsp 中（不能使用重定向，因为要在同一个页面中显示出来）
        request.getRequestDispatcher("/index.jsp").forward(request, response);
    }

    private void delete(HttpServletRequest request, HttpServletResponse response) throws IOException {
        System.out.println("CustomerServlet\tdelete");
        int flowId;
        try {
            flowId = Integer.parseInt(request.getParameter("flowId"));
            customerDAO.delete(flowId);
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
        response.sendRedirect("query.do");
    }

    private void error(HttpServletRequest request, HttpServletResponse response) {
        System.out.println("CustomerServlet\t调用的method方法错误！");
    }
}
