package cn.mine.netty1.dao;

import java.util.ArrayList;

/**
 * @author 魏喜明
 */
public interface FriendDao {
    boolean isExist(String friendName, int userId);

    void delFriend(int userId, String friendName);

    void addFriends(int userId, String localName, String friendNickname);

    ArrayList<String> getFriends(int uid);
}
