package cn.netty1.dao;

/**
 * @author 魏喜明
 */
public class UserDaoImpl implements UserDao {
    @Override
    public boolean verifyExistFriend(int friendId) {
        return false;
    }

    @Override
    public boolean verifyPassword(String oldPassword, int id) {
        return false;
    }

    @Override
    public void modifyPassword(String newPassword, int id) {

    }

    @Override
    public boolean getInformation(int id, String password) {
        return false;
    }
}
