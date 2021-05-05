package cn.mine.springboot.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author 魏喜明 2021-05-05 14:54:05
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Department {
    private Integer id;
    private String departmentName;
}
