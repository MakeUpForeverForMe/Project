package cn.javaweb.dao;

import cn.javaweb.dao.impl.CustomerDAOJdbcImpl;
import cn.javaweb.dao.impl.CustomerDAOXMLImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 魏喜明 2021-02-10 23:28:30
 */
public class CustomerDAOFactory {
    private static CustomerDAOFactory instance;
    private final Map<String, CustomerDAO> daoMap = new HashMap<>();
    private String type = "jdbc";

    private CustomerDAOFactory() {
        daoMap.put("jdbc", new CustomerDAOJdbcImpl());
        daoMap.put("xml", new CustomerDAOXMLImpl());
    }

    public static CustomerDAOFactory getInstance() {
        if (instance == null) {
            instance = new CustomerDAOFactory();
        }
        return instance;
    }

    public void setType(String type) {
        this.type = type;
    }

    public CustomerDAO getCustomerDAO() {
        CustomerDAO customerDAO = daoMap.get(type);
        System.out.println("CustomerDAOFactory\tgetCustomerDAO\t" + customerDAO);
        return customerDAO;
    }
}
