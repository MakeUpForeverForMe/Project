package cn.javaweb;

import cn.javaweb.bean.Customer;
import cn.javaweb.dao.CustomerDAO;
import cn.javaweb.dao.impl.CustomerDAOJdbcImpl;
import cn.javaweb.utils.JdbcUtils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author 魏喜明 2021-02-04 01:09:28
 */
public class JavaWebTest {

    @Test
    public void JdbcUtilsGetConnection() throws SQLException {
        Connection connection = JdbcUtils.getConnection();
        System.out.println(connection);
    }

    private final CustomerDAO customerDAO = new CustomerDAOJdbcImpl();

    @Test
    public void CustomerDAOJdbcImplGetAll() {
        List<Customer> all = customerDAO.getAll();
        all.forEach(System.out::println);
    }

    @Test
    public void CustomerDAOJdbcImplGet() {
        System.out.println(customerDAO.get(1));
    }

    @Test
    public void CustomerDAOJdbcImplGetCountWithName() {
        long jerry = customerDAO.getCountWithName("Jerry");
        System.out.println(jerry);
    }

    @Test
    public void CustomerDAOJdbcImplSave() {
        Customer customer = new Customer();
        customer.setName("Mike");
        customer.setAddress("BeiJing");
        customer.setPhone("18012341234");
        customerDAO.save(customer);

        customer.setName("Jerry");
        customer.setAddress("ShangHai");
        customer.setPhone("18112341234");
        customerDAO.save(customer);
    }

    @Test
    public void CustomerDAOJdbcImplDelete() {
        customerDAO.delete(1);
    }

    @Test
    public void testGetAllWithCriteriaCustomer() {
        Customer customer = new Customer(null, null, null);
        List<Customer> allWithCriteriaCustomer = customerDAO.getAllWithCriteriaCustomer(customer);
        System.out.println("testGetAllWithCriteriaCustomer" + allWithCriteriaCustomer);
    }

    @Test
    public void testCustomerServletAdd() {
        Customer customer = new Customer("Nike", null, null);
        long count = customerDAO.getCountWithName(customer.getName());
        System.out.println("testCustomerServletAdd\t" + count);
    }
}
