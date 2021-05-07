package cn.springboot.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author 魏喜明 2021-05-06 07:19:49
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Address implements Serializable {
    private String ip;
    private String region;
    private String city;
}