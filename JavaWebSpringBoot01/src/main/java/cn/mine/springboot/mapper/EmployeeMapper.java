package cn.mine.springboot.mapper;

import cn.mine.springboot.bean.Department;
import cn.mine.springboot.bean.Employee;
import org.apache.ibatis.annotations.*;

/**
 * @author 魏喜明 2021-05-05 14:57:30
 */
public interface EmployeeMapper {
    Employee getEmpById(Integer id);

    int insertEmp(Employee employee);
}
