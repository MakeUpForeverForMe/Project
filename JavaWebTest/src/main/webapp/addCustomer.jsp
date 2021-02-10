<%-- User: 魏喜明 2021-02-05 22:39:57 --%>
<%@ page
        contentType="text/html; charset=UTF-8"
        pageEncoding="UTF-8"
%>
<html>
<head>
    <title>Add Customer</title>
</head>
<body>
<%
    String duplicate = (String) request.getAttribute("duplicate");
    if (duplicate != null) {
%>
<%= duplicate %>
<%
    }
%>
<form action="add.do" method="post">
    <table>
        <tr>
            <td>CustomerName:</td>
            <td><label><input name="name" type="text"
                              value="<%= request.getParameter("name") == null ? "" : request.getParameter("name") %>"></label>
            </td>
        </tr>
        <tr>
            <td>CustomerPhone:</td>
            <td><label><input name="phone" type="text"
                              value="<%= request.getParameter("phone") == null ? "" : request.getParameter("phone") %>"></label>
            </td>
        </tr>
        <tr>
            <td>CustomerAddress:</td>
            <td><label><input name="address" type="text"
                              value="<%= request.getParameter("address") == null ? "" : request.getParameter("address") %>"></label>
            </td>
        </tr>
        <tr>
            <td><input name="submit" type="submit" value="Query"></td>
        </tr>
    </table>
</form>
</body>
</html>
