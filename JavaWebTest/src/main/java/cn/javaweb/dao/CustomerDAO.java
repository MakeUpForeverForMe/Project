package cn.javaweb.dao;

import cn.javaweb.bean.Customer;

import java.util.List;

/**
 * @author 魏喜明 2021-02-03 00:36:02
 */
public interface CustomerDAO {
    List<Customer> getAllWithCriteriaCustomer(Customer customer);

    /**
     * 获取所有数据
     *
     * @return 获取所有数据
     */
    List<Customer> getAll();

    /**
     * 根据 ID 获取相应记录
     *
     * @param id 传入记录的 ID
     * @return 返回 ID 对应的记录
     */
    Customer get(int id);

    /**
     * 返回和 Name 相等的记录数
     *
     * @param name 传入的名字
     * @return 返回和 Name 相等的记录数
     */
    long getCountWithName(String name);

    /**
     * 增加新的 Customer 对象
     *
     * @param customer 传入 Customer 对象
     */
    void save(Customer customer);

    /**
     * 更新数据
     *
     * @param customer 传入 Customer 对象
     */
    void update(Customer customer);

    /**
     * 根据传入的 ID 删除对应记录
     *
     * @param id 传入 ID
     */
    void delete(int id);
}
