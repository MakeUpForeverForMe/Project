package cn.springboot.mapper;

import cn.springboot.bean.Department;
import org.apache.ibatis.annotations.*;

/**
 * @author 魏喜明 2021-05-05 14:57:30
 */
public interface DepartmentMapper {
    @Select("select * from department where id = #{id}")
    Department getDeptById(Integer id);

    @Delete("delete from department where id = #{id}")
    int deleteDeptById(Integer id);

    @Options(useGeneratedKeys = true)
    @Insert("insert into department(department_name) values(#{departmentName})")
    int insertDept(Department department);

    @Update("update department set department_name = #{departmentName} where id = #{id})")
    int updateDept(Department department);
}
