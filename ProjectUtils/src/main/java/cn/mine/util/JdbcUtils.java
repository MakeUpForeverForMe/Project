package cn.mine.util;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author 魏喜明 2021-02-03 00:32:07
 */
public class JdbcUtils {
    private static final DataSource dataSource;
    private static Connection connection;

    static {
        // 数据源只能被创建一次
        dataSource = new ComboPooledDataSource("mvc");
    }

    /**
     * 返回数据源的连接对象
     *
     * @return 返回数据源的连接对象
     */
    public static Connection getConnection() {
        try {
            connection = dataSource.getConnection();
        } catch (SQLException throwable) {
            throwable.printStackTrace();
        }
        return connection;
    }

    /**
     * 释放 Connection 连接
     *
     * @param connection Connection 对象
     */
    public static void releaseConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException throwable) {
                throwable.printStackTrace();
            }
        }
    }
}
