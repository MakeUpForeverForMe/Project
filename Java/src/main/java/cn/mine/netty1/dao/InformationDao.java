package cn.mine.netty1.dao;

import cn.mine.netty1.bean.Information;

/**
 * @author 魏喜明
 */
public interface InformationDao {
    Information getInformation(int friendId);

    Information nicknameGetId(String nickname);

    void storeNickname(String nickname, int id);

    void storeSignature(String signature, int id);
}
