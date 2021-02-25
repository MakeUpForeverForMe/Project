<%@ page import="cn.javaweb.bean.Customer" %><%-- User: 魏喜明 2021-02-07 23:25:14 --%>
<%@ page
        contentType="text/html; charset=UTF-8"
        pageEncoding="UTF-8"
%>
<html>
<head>
    <title>Update</title>
</head>
<body>
<%
    Customer customer = (Customer) request.getAttribute("customer");
    String duplicate = (String) request.getAttribute("duplicate");
    if (duplicate != null) {
%>
<%= duplicate %>
<%
    }
%>
<br>
<%= "正在修改 " + customer %>
<form action="update.do" method="post">
    <input type="hidden" name="flowId" value="<%= customer.getId() %>">
    <input type="hidden" name="oldName" value="<%= customer.getName() %>">
    <table>
        <tr>
            <td>CustomerName:</td>
            <td><label><input name="name" type="text" value="<%= customer.getName() %>"></label></td>
        </tr>
        <tr>
            <td>CustomerPhone:</td>
            <td><label><input name="phone" type="text" value="<%= customer.getPhone() %>"></label></td>
        </tr>
        <tr>
            <td>CustomerAddress:</td>
            <td><label><input name="address" type="text" value="<%= customer.getAddress() %>"></label></td>
        </tr>
        <tr>
            <td><input name="submit" type="submit" value="Submit"></td>
        </tr>
    </table>
</form>
</body>
</html>
