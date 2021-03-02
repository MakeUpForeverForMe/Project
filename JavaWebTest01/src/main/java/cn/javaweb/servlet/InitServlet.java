package cn.javaweb.servlet;

import cn.javaweb.dao.CustomerDAOFactory;

import javax.servlet.http.HttpServlet;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class InitServlet extends HttpServlet {

    @Override
    public void init() {
        System.out.println("InitServlet\tinit");

        CustomerDAOFactory.getInstance().setType("jdbc");

        InputStream resourceAsStream = getServletContext().getResourceAsStream("/WEB-INF/classes/switch.properties");
        Properties properties = new Properties();
        try {
            properties.load(resourceAsStream);
            String type = properties.getProperty("type");
            System.out.println("InitServlet\tinit\t" + type);
            CustomerDAOFactory.getInstance().setType(type);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
