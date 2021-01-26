<%@ page import="cn.javaweb.bean.Person" %>
<%@ page contentType="text/html; UTF-8" pageEncoding="utf-8" %>
<html>
<title>测试Web</title>
<body>
<h2>Hello World!</h2>
<%-- pageContext request session application 作用域从小到大 --%>
<%
    Person person = new Person();
    System.out.println(person.getInfo());
%>
</body>
</html>
