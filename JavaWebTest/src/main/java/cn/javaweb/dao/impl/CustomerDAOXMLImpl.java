package cn.javaweb.dao.impl;

import cn.javaweb.bean.Customer;
import cn.javaweb.dao.CustomerDAO;

import java.util.List;

public class CustomerDAOXMLImpl implements CustomerDAO {
    @Override
    public List<Customer> getAllWithCriteriaCustomer(Customer customer) {
        System.out.println("List<Customer> getAllWithCriteriaCustomer(Customer customer)");
        return null;
    }

    @Override
    public List<Customer> getAll() {
        System.out.println("List<Customer> getAll()");
        return null;
    }

    @Override
    public Customer get(int id) {
        System.out.println("Customer get(int id)");
        return null;
    }

    @Override
    public long getCountWithName(String name) {
        System.out.println("long getCountWithName(String name)");
        return 0;
    }

    @Override
    public void save(Customer customer) {
        System.out.println("void save(Customer customer)");
    }

    @Override
    public void update(Customer customer) {
        System.out.println("void update(Customer customer)");
    }

    @Override
    public void delete(int id) {
        System.out.println("void delete(int id)");
    }
}
