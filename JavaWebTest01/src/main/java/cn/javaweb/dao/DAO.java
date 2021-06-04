package cn.javaweb.dao;

import cn.javaweb.utils.JdbcUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * 封装了基本的 CRUD 的方法，以供子类继承使用
 *
 * @param <T> 当前 DAO 处理的实体类的类型是什么
 * @author 魏喜明 2021-02-03 00:14:49
 */
public class DAO<T> {
    private final QueryRunner queryRunner = new QueryRunner();
    private Class<T> clazz;

    public DAO() {
        Type superclass = getClass().getGenericSuperclass();
        if (superclass instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) superclass;
            Type[] typeArguments = parameterizedType.getActualTypeArguments();
            if (typeArguments != null && typeArguments.length > 0) {
                Type type = typeArguments[0];
                if (type instanceof Class<?>) {
                    clazz = (Class<T>) type;
                }
            }
        }
    }

    /**
     * 返回 T 对应的对象的集合
     *
     * @param sql  SQL 语句
     * @param args 填充 SQL 语句的站位符
     * @return 返回 T 对应的对象的集合
     */
    public List<T> getList(String sql, Object... args) {
        Connection connection = null;
        try {
            connection = JdbcUtils.getConnection();
            List<T> query = queryRunner.query(connection, sql, new BeanListHandler<>(clazz), args);
            System.out.println("DAO<" + clazz.getSimpleName() + ">[");
            query.forEach(item -> System.out.println("  " + item));
            System.out.println(']');
            return query;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            JdbcUtils.releaseConnection(connection);
        }
        return null;
    }

    /**
     * 返回 T 对应的实体类对象
     *
     * @param sql  SQL 语句
     * @param args 填充 SQL 语句的站位符
     * @return 返回 T 对应的实体类对象
     */
    public T get(String sql, Object... args) {
        Connection connection = null;
        try {
            connection = JdbcUtils.getConnection();
            return queryRunner.query(connection, sql, new BeanHandler<>(clazz), args);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            JdbcUtils.releaseConnection(connection);
        }
        return null;
    }

    /**
     * 返回某一个字段的值。例如：返回某一条记录的值，或返回数据表中的记录数等
     *
     * @param sql  SQL 语句
     * @param args 填充 SQL 语句的站位符
     * @param <E>  泛型
     * @return 返回某一个字段的值
     */
    public <E> E getValue(String sql, Object... args) {
        Connection connection = null;
        try {
            connection = JdbcUtils.getConnection();
            queryRunner.query(connection, sql, new ScalarHandler(), args);
            return (E) queryRunner.query(connection, sql, new ScalarHandler(), args);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            JdbcUtils.releaseConnection(connection);
        }
        return null;
    }

    /**
     * 该方法封装了 INSERT、DELETE、UPDATE 操作
     *
     * @param sql  SQL 语句
     * @param args 填充 SQL 语句的站位符
     */
    public void update(String sql, Object... args) {
        Connection connection = null;
        try {
            connection = JdbcUtils.getConnection();
            queryRunner.update(connection, sql, args);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            JdbcUtils.releaseConnection(connection);
        }
    }
}
