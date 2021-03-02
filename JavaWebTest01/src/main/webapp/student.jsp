<%-- User: 魏喜明 2021-01-31 22:49:02 --%>
<%@ page import="java.util.List" %>
<%@ page import="cn.javaweb.bean.Student" %>
<%@ page import="java.util.ArrayList" %>
<%@ page
        contentType="text/html; charset=UTF-8"
        pageEncoding="UTF-8"
%>
<html>
<head>
    <title>Student</title>
</head>
<body>
<%
    Object students = request.getAttribute("students");
    List<Student> studentList = new ArrayList<>();
    if (students instanceof List<?>) {
        for (Object student : ((List<?>) students)) {
            studentList.add(((Student) student));
        }
    }
%>
<table border="1" cellpadding="10" cellspacing="0">
    <tr>
        <th>FlowId</th>
        <th>Type</th>
        <th>IDCard</th>
        <th>ExamCard</th>
        <th>StudentName</th>
        <th>Location</th>
        <th>Grade</th>
        <th>Delete</th>
    </tr>
    <%
        for (Student student : studentList) {
    %>
    <tr>
        <td><%= student.getFlowId() %>
        </td>
        <td><%= student.getType() %>
        </td>
        <td><%= student.getIdCard() %>
        </td>
        <td><%= student.getExamCard() %>
        </td>
        <td><%= student.getStudentName() %>
        </td>
        <td><%= student.getLocation() %>
        </td>
        <td><%= student.getGrade() %>
        </td>
        <td><a href="deleteStudent?flowId=<%= student.getFlowId() %>">Delete</a></td>
    </tr>
    <%
        }
    %>
</table>
</body>
</html>
