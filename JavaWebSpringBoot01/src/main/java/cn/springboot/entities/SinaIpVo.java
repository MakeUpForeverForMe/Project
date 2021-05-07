package cn.springboot.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author 魏喜明 2021-05-06 07:18:06
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SinaIpVo implements Serializable {
    private Integer ret;
    private String province;
    private String city;
}