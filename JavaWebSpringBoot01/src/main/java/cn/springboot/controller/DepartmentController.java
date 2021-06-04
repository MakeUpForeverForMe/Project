package cn.springboot.controller;

import cn.springboot.bean.Department;
import cn.springboot.bean.Employee;
import cn.springboot.mapper.DepartmentMapper;
import cn.springboot.mapper.EmployeeMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author 魏喜明 2021-05-05 15:05:10
 */
@Controller
public class DepartmentController {
    private final DepartmentMapper departmentMapper;
    private final EmployeeMapper employeeMapper;

    public DepartmentController(DepartmentMapper departmentMapper, EmployeeMapper employeeMapper) {
        this.departmentMapper = departmentMapper;
        this.employeeMapper = employeeMapper;
    }

    @GetMapping("/dept/{id}")
    public Department getDept(@PathVariable("id") Integer id) {
        return departmentMapper.getDeptById(id);
    }

    @GetMapping("/dept")
    public Department insertDept(Department department) {
        departmentMapper.insertDept(department);
        return department;
    }

    @GetMapping("/emp2/{id}")
    public Employee getEmp(@PathVariable("id") Integer id) {
        return employeeMapper.getEmpById(id);
    }

    @GetMapping("/emp2")
    public Employee insertEmp(Integer id) {
        return employeeMapper.getEmpById(id);
    }
}
