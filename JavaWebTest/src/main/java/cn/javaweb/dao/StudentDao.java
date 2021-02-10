package cn.javaweb.dao;

import cn.javaweb.bean.Student;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 魏喜明 2021-02-01 01:27:39
 */
public class StudentDao {
    public void deleteByFlowId(int flowId) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            String driverClass = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://192.168.18.100:3306/java_web";
            String user = "root";
            String pass = "000000";

            Class.forName(driverClass);
            connection = DriverManager.getConnection(url, user, pass);

            String sql = "delete from examstudent where flow_id = ?";
            preparedStatement = connection.prepareStatement(sql);

            preparedStatement.setInt(1, flowId);
            preparedStatement.executeUpdate();
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (preparedStatement != null) preparedStatement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            try {
                if (connection != null) connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public List<Student> getAll() {
        ArrayList<Student> students = new ArrayList<>();

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            String driverClass = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://192.168.18.100:3306/java_web";
            String user = "root";
            String pass = "000000";

            Class.forName(driverClass);
            connection = DriverManager.getConnection(url, user, pass);

            String sql = "select " +
                    "flow_id, type, id_card, exam_card, student_name, location, grade " +
                    "from examstudent";

            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                int flowId = resultSet.getInt(1);
                int type = resultSet.getInt(2);
                String idCard = resultSet.getString(3);
                String examCard = resultSet.getString(4);
                String studentName = resultSet.getString(5);
                String location = resultSet.getString(6);
                int grade = resultSet.getInt(7);

                Student student = new Student(flowId, type, idCard, examCard, studentName, location, grade);
                students.add(student);
            }
        } catch (SQLException | ClassNotFoundException throwables) {
            throwables.printStackTrace();
        } finally {
            try {
                if (resultSet != null) resultSet.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            try {
                if (preparedStatement != null) preparedStatement.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            try {
                if (connection != null) connection.close();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
        return students;
    }
}
