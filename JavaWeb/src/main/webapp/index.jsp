<%@ page import="cn.web.bean.Person" %>
<%@ page contentType="text/html; UTF-8" pageEncoding="utf-8" %>
<html>
<title>测试Web</title>
<body>
<h2>Hello World!</h2>
<%
    Person person = new Person();
    System.out.println(person.getInfo());
%>
</body>
</html>
