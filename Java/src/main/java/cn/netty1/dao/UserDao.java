package cn.netty1.dao;

/**
 * @author 魏喜明
 */
public interface UserDao {
    boolean verifyExistFriend(int friendId);

    boolean verifyPassword(String oldPassword, int id);

    void modifyPassword(String newPassword, int id);

    boolean getInformation(int id, String password);
}
