package cn.springboot.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

/**
 * @author 魏喜明 2021-05-05 16:25:43
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
public class User {
    @Id
    @GeneratedValue
    private Integer id;
    @Column(name = "last_name")
    private String lastName;
    private String email;
}
