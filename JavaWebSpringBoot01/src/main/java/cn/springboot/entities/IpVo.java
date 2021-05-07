package cn.springboot.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author 魏喜明 2021-05-06 07:17:08
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IpVo implements Serializable {
    private Integer code;
    private Address address;
}