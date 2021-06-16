package cn.mine;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author ximing.wei 2021-06-15 16:45:53
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer {
    private String id;
    private String cTime;
    private String uTime;
    private String birthDate;
    private Integer sex;
    private String city;
    private Integer expectation;
    private String province;
    private Integer sourceChannel;

    @Override
    public String toString() {
        return "Customer{" +
                "id='" + id + '\'' +
                ", cTime='" + cTime + '\'' +
                ", uTime='" + uTime + '\'' +
                ", birthDate='" + birthDate + '\'' +
                ", sex=" + sex +
                ", city='" + city + '\'' +
                ", expectation=" + expectation +
                ", province='" + province + '\'' +
                ", sourceChannel=" + sourceChannel +
                '}';
    }
}
