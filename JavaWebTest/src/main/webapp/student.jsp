<%@ page import="java.util.List" %><%-- User: 魏喜明 2021-01-31 22:49:02 --%>
<%@ page
        contentType="text/html; charset=UTF-8"
        pageEncoding="UTF-8"
%>
<html>
<head>
    <title>Student</title>
</head>
<body>
<%= request.getAttribute("students") %>
<br>
<%
    List<String> students = (List<String>) request.getAttribute("students");
    for (String student : students) {
%>
<%= student %><br>
<%
    }
%>
</body>
</html>
