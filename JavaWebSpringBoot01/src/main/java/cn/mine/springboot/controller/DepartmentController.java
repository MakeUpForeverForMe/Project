package cn.mine.springboot.controller;

import cn.mine.springboot.bean.Department;
import cn.mine.springboot.bean.Employee;
import cn.mine.springboot.mapper.DepartmentMapper;
import cn.mine.springboot.mapper.EmployeeMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author 魏喜明 2021-05-05 15:05:10
 */
@RestController
public class DepartmentController {
    private final DepartmentMapper departmentMapper;
    private final EmployeeMapper employeeMapper;

    public DepartmentController(
            DepartmentMapper departmentMapper,
            EmployeeMapper employeeMapper
    ) {
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
