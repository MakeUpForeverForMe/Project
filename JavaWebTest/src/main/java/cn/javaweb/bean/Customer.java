package cn.javaweb.bean;

/**
 * @author 魏喜明 2021-02-03 00:38:14
 */
public class Customer {
    private int id;
    private String name;
    private String address;
    private String phone;

    public Customer() {
    }

    public Customer(String name, String address, String phone) {
        this.name = name;
        this.address = address;
        this.phone = phone;
    }

    public Customer(int id, String name, String address, String phone) {
        this.id = id;
        this.name = name;
        this.address = address;
        this.phone = phone;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public String getCriteriaName() {
        return name == null ? "%%" : "%" + name + "%";
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getAddress() {
        return address;
    }

    public String getCriteriaAddress() {
        return address == null ? "%%" : "%" + address + "%";
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getPhone() {
        return phone;
    }

    public String getCriteriaPhone() {
        return phone == null ? "%%" : "%" + phone + "%";
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id=" + this.getId() +
                ",name='" + this.getName() + '\'' +
                ",address='" + this.getAddress() + '\'' +
                ",phone='" + this.getPhone() + '\'' +
                '}';
    }
}
