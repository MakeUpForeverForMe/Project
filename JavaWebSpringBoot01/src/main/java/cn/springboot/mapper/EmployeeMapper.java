package cn.springboot.mapper;

import cn.springboot.bean.Employee;

/**
 * @author 魏喜明 2021-05-05 14:57:30
 */
public interface EmployeeMapper {
    Employee getEmpById(Integer id);

    int insertEmp(Employee employee);
}
