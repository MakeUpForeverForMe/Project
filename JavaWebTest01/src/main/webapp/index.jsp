<%@ page import="cn.javaweb.bean.Person" %>
<%@ page import="cn.javaweb.bean.Customer" %>
<%@ page import="java.util.List" %>
<%@ page contentType="text/html; charset=UTF-8" pageEncoding="utf-8" %>
<html>
<head>
    <title>测试Web</title>
    <script type="text/javascript" src="script/jquery-1.7.2.js"></script>
    <script type="text/javascript">
        $(function () {
            $(".delete").click(function () {
                const text = $(this).parent().parent().find("td:eq(1)").text();
                return confirm("确定要删除" + text + "的信息吗？！！");
            });
        });
    </script>
</head>
<body>
<h2>Hello World!</h2>
<%-- pageContext request session application 作用域从小到大 --%>
<%
    Person person = new Person();
    System.out.println(person.getInfo() + "哈哈！控制台输出。");
    request.setAttribute("requestAttr", "requestValue");
//    session.setAttribute("sessionAttr", "sessionValue");
    application.setAttribute("applicationAttr", "applicationValue");
%>
<%= person.getInfo() + " 哈哈！页面输出。"%>
<%--<br><a href="login.html">loginServlet</a>--%>
<%--<br><a href="testAttr">testAttr</a>--%>
<%--<br><a href="forwardServlet">forwardServlet</a>--%>
<%--<br><a href="redirectServlet">redirectServlet</a>--%>
<%--<br><a href="listAllStudent">List All Student</a>--%>
<%-- <br><a href="customerServer?method=add">Add</a> --%>
<%--<br><a href="add.do">Add</a>--%>
<%-- <br><a href="customerServer?method=query">Query</a> --%>
<%--<br><a href="query.do">Query</a>--%>
<%-- <br><a href="customerServer?method=delete">Delete</a> --%>
<%--<br><a href="delete.do">Delete</a>--%>
<%--<br><a href="update.do">Update</a>--%>
<%--<br><a href="edit.do">Edit</a>--%>
<form action="query.do" method="post">
    <table>
        <tr>
            <td>CustomerName:</td>
            <td><label><input name="name" type="text"></label></td>
        </tr>
        <tr>
            <td>CustomerAddress:</td>
            <td><label><input name="address" type="text"></label></td>
        </tr>
        <tr>
            <td>CustomerPhone:</td>
            <td><label><input name="phone" type="text"></label></td>
        </tr>
        <tr>
            <td><input name="submit" type="submit" value="Query"></td>
            <td><a href="addCustomer.jsp">Add New Customer</a></td>
        </tr>
    </table>
</form>
<br>
<%
    List<Customer> customers = (List<Customer>) request.getAttribute("customers");
    if (customers != null && customers.size() > 0) {
%>
<hr>
<br>
<table border="1" cellpadding="10" cellspacing="0">
    <tr>
        <th>ID</th>
        <th>CustomerName</th>
        <th>CustomerAddress</th>
        <th>CustomerPhone</th>
        <th>UPDATE\DELETE</th>
    </tr>
    <%
        for (Customer customer : customers) {
    %>
    <tr>
        <td><%= customer.getId() %>
        </td>
        <td><%= customer.getName() %>
        </td>
        <td><%= customer.getAddress() %>
        </td>
        <td><%= customer.getPhone() %>
        </td>
        <td>
            <a href="edit.do?flowId=<%= customer.getId() %>">UPDATE</a>
            <a href="delete.do?flowId=<%= customer.getId() %>" class="delete">DELETE</a>
        </td>
    </tr>
    <%
        }
    %>
</table>
<hr>
<%
    }
%>
</body>
</html>
