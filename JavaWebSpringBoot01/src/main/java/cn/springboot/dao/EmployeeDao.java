package cn.springboot.dao;

import cn.springboot.entities.Department;
import cn.springboot.entities.Employee;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


@Repository
public class EmployeeDao {

    private static final Map<Integer, Employee> employees;

    private final DepartmentDao departmentDao;

    public EmployeeDao(DepartmentDao departmentDao) {
        this.departmentDao = departmentDao;
    }

    static {
        employees = new HashMap<>();

        employees.put(1001, new Employee(1001, "E-AA", "aa@163.com", 1, new Department(101, "D-AA"), new Date()));
        employees.put(1002, new Employee(1002, "E-BB", "bb@163.com", 1, new Department(102, "D-BB"), new Date()));
        employees.put(1003, new Employee(1003, "E-CC", "cc@163.com", 0, new Department(103, "D-CC"), new Date()));
        employees.put(1004, new Employee(1004, "E-DD", "dd@163.com", 0, new Department(104, "D-DD"), new Date()));
        employees.put(1005, new Employee(1005, "E-EE", "ee@163.com", 1, new Department(105, "D-EE"), new Date()));
    }

    private static Integer initId = 1006;

    public void save(Employee employee) {
        if (employee.getId() == null) {
            employee.setId(initId++);
        }

        employee.setDepartment(departmentDao.getDepartment(employee.getDepartment().getId()));
        employees.put(employee.getId(), employee);
    }

    public Collection<Employee> getAll() {
        return employees.values();
    }

    public Employee get(Integer id) {
        return employees.get(id);
    }

    public void delete(Integer id) {
        employees.remove(id);
    }
}
