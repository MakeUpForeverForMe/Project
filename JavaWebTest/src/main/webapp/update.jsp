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
<%= "正在修改 " + request.getAttribute("customer") %>
<form action="query.do" method="post">
    <table>
        <tr>
            <td>CustomerName:</td>
            <td><label><input name="name" type="text"></label></td>
        </tr>
        <tr>
            <td>CustomerPhone:</td>
            <td><label><input name="phone" type="text"></label></td>
        </tr>
        <tr>
            <td>CustomerAddress:</td>
            <td><label><input name="address" type="text"></label></td>
        </tr>
        <tr>
            <td><input name="submit" type="submit" value="Query"></td>
        </tr>
    </table>
</form>
</body>
</html>
