package cn.mine.springboot.controller;

import cn.mine.springboot.dao.DepartmentDao;
import cn.mine.springboot.dao.EmployeeDao;
import cn.mine.springboot.entities.Department;
import cn.mine.springboot.entities.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;

/**
 * @author 魏喜明 2021-05-02 17:04:24
 */
@Controller
public class EmployeeController {
    private final EmployeeDao employeeDao;

    private final DepartmentDao departmentDao;

    public EmployeeController(EmployeeDao employeeDao, DepartmentDao departmentDao) {
        this.employeeDao = employeeDao;
        this.departmentDao = departmentDao;
    }

    @GetMapping("/emps")
    public String list(Map<String, Object> map) {
        Collection<Employee> employees = employeeDao.getAll();
        map.put("emps", employees);
        return "emp/list";
    }

    @GetMapping("/emp")
    public String toAddPage(Map<String, Object> map) {
        Collection<Department> departments = departmentDao.getDepartments();
        map.put("depts", departments);
        return "emp/add";
    }

    @PostMapping("/emp")
    public String addEmp(Employee employee) {
        employeeDao.save(employee);
        return "redirect:/emps";
    }

    @GetMapping("/emp/{id}")
    public String toEditPage(@PathVariable("id") Integer id, Map<String, Object> map) {
        Employee employee = employeeDao.get(id);
        map.put("emp", employee);
        Collection<Department> departments = departmentDao.getDepartments();
        map.put("depts", departments);
        return "emp/add";
    }

    @PutMapping("/emp")
    public String updateEmp(Employee employee) {
        employeeDao.save(employee);
        return "redirect:/emps";
    }

    @DeleteMapping("/emp/{id}")
    public String deleteEmp(@PathVariable("id") Integer id) {
        employeeDao.delete(id);
        return "redirect:/emps";
    }
}
