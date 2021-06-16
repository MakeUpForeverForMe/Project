package cn.mine.netty1.dao;

import java.util.ArrayList;

/**
 * @author 魏喜明
 */
public class FriendDaoImpl implements FriendDao {
    @Override
    public boolean isExist(String friendName, int userId) {
        return false;
    }

    @Override
    public void delFriend(int userId, String friendName) {

    }

    @Override
    public void addFriends(int userId, String localName, String friendNickname) {

    }

    @Override
    public ArrayList<String> getFriends(int uid) {
        return null;
    }
}
