package cn.javaweb.dao.impl;

import cn.javaweb.bean.Customer;
import cn.javaweb.dao.CustomerDAO;
import cn.javaweb.dao.DAO;

import java.util.List;

/**
 * @author 魏喜明 2021-02-03 23:26:39
 */
public class CustomerDAOJdbcImpl extends DAO<Customer> implements CustomerDAO {

    @Override
    public List<Customer> getAllWithCriteriaCustomer(Customer customer) {
        String sql = "select id,name,address,phone from customers where " +
                "name like ? and address like ? and phone like ?";
        return getList(sql, customer.getCriteriaName(), customer.getCriteriaAddress(), customer.getCriteriaPhone());
    }

    @Override
    public List<Customer> getAll() {
        String sql = "select id,name,address,phone from customers";
        return getList(sql);
    }

    @Override
    public Customer get(int id) {
        String sql = "select id,name,address,phone from customers where id = ?";
        return get(sql, id);
    }

    @Override
    public long getCountWithName(String name) {
        String sql = "select count(id) from customers where name = ?";
        return getValue(sql, name);
    }

    @Override
    public void save(Customer customer) {
        String sql = "insert into customers(name,address,phone) values(?,?,?)";
        update(sql, customer.getName(), customer.getAddress(), customer.getPhone());
    }

    @Override
    public void update(Customer customer) {
        String sql = "update customers set name = ?,address = ?,phone = ? where id = ?";
        update(sql, customer.getName(), customer.getAddress(), customer.getPhone(), customer.getId());
    }

    @Override
    public void delete(int id) {
        String sql = "delete from customers where id = ?";
        update(sql, id);
    }
}
