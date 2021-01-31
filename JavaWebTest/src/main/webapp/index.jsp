<%@ page import="cn.javaweb.bean.Person" %>
<%@ page contentType="text/html; charset=UTF-8" pageEncoding="utf-8" %>
<html>
<title>测试Web</title>
<body>
<h2>Hello World!</h2>
<%-- pageContext request session application 作用域从小到大 --%>
<%
    Person person = new Person();
    System.out.println(person.getInfo() + "哈哈！");
    request.setAttribute("requestAttr", "requestValue");
    session.setAttribute("sessionAttr", "sessionValue");
    application.setAttribute("applicationAttr", "applicationValue");
%>
<%= person.getInfo() + " 哈哈！"%>
<br>
<a href="login.html">loginServlet</a>
<br>
<a href="testAttr">testAttr</a>
<br>
<a href="forwardServlet">forwardServlet</a>
<br>
<a href="redirectServlet">redirectServlet</a>
<br>
<a href="listAllStudent">List All Student</a>
</body>
</html>
