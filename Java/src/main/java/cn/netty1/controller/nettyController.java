package cn.netty1.controller;

import cn.netty1.bean.Information;
import cn.netty1.constant.EnMsgType;
import cn.netty1.dao.*;
import cn.netty1.utils.CacheUtil;
import cn.netty1.utils.JsonUtils;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author 魏喜明
 */
public class nettyController {

    private static UserDao userDao = new UserDaoImpl();
    private static InformationDao informationDao = new InformationDaoImpl();
    private static FriendDao friendDao = new FriendDaoImpl();


    public static String processing(String message, Channel channel) {

        //解析客户端发送的消息
        ObjectNode jsonNodes = JsonUtils.getObjectNode(message);
        assert jsonNodes != null;
        String msgtype = jsonNodes.get("msgtype").asText();
        if (EnMsgType.EN_MSG_LOGIN.toString().equals(msgtype)) {
            //登录操作
            return loginOperation(jsonNodes, channel);
        } else if (EnMsgType.EN_MSG_MODIFY_SIGNATURE.toString().equals(msgtype)) {
            //修改签名
            return modifySignature(jsonNodes);
        } else if (EnMsgType.EN_MSG_MODIFY_NICKNAME.toString().equals(msgtype)) {
            //修改昵称
            return modifyNickname(jsonNodes);
        } else if (EnMsgType.EN_MSG_GETINFORMATION.toString().equals(msgtype)) {
            //获取登录信息
            return getInformation(jsonNodes);
        } else if (EnMsgType.EN_MSG_VERIFY_PASSWORD.toString().equals(msgtype)) {
            //进行修改密码
            return verifyPasswd(jsonNodes);
        } else if (EnMsgType.EN_MSG_CHAT.toString().equals(msgtype)) {
            //单聊模式
            return SingleChat(jsonNodes);
        } else if (EnMsgType.EN_MSG_GET_ID.toString().equals(msgtype)) {
            //获取id
            return getId(jsonNodes);
        } else if (EnMsgType.EN_MSG_GET_FRIEND.toString().equals(msgtype)) {
            //获取好友列表
            return getFriend(jsonNodes);
        } else if (EnMsgType.EN_MSG_ADD_FRIEND.toString().equals(msgtype)) {
            //添加好友
            return addFriends(jsonNodes);
        } else if (EnMsgType.EN_MSG_DEL_FRIEND.toString().equals(msgtype)) {
            //删除好友
            return delFriend(jsonNodes);
        } else if (EnMsgType.EN_MSG_ACTIVE_STATE.toString().equals(msgtype)) {
            //判断好友的在线状态
            return friendIsActive(jsonNodes);
        }
        return "";
    }


    //判断好友在线状态
    private static String friendIsActive(ObjectNode jsonNodes) {
        int friendId = jsonNodes.get("friendId").asInt();


        ObjectNode objectNode = JsonUtils.getObjectNode();
        assert objectNode != null;
        objectNode.put("msgtype", EnMsgType.EN_MSG_ACK.toString());
        objectNode.put("srctype", EnMsgType.EN_MSG_ACTIVE_STATE.toString());


        //客户端保证用户独立存在且是好友
        Channel channel = CacheUtil.get(friendId);
        //判断用户是否在线
        if (channel == null) {
            //用户不在线
            objectNode.put("code", 200);
        } else {
            //用户在线
            objectNode.put("code", 300);
        }
        return objectNode.toString();
    }


    //添加好友
    private static String delFriend(ObjectNode jsonNodes) {
        int friendId = jsonNodes.get("friendId").asInt();
        int userId = jsonNodes.get("id").asInt();
        String localName = jsonNodes.get("localName").asText();


        //封装发回客户端的JSON
        ObjectNode objectNode = JsonUtils.getObjectNode();
        assert objectNode != null;
        objectNode.put("msgtype", EnMsgType.EN_MSG_ACK.toString());
        objectNode.put("srctype", EnMsgType.EN_MSG_DEL_FRIEND.toString());
        objectNode.put("code", 200);


        //验证是否存在当前好友
        Information information = informationDao.getInformation(friendId);
        String friendName = information.getNickname();
        //查询自己是否有该好友
        boolean exist = friendDao.isExist(friendName, userId);
        if (exist) {
            //存在当前好友进行删除操作
            friendDao.delFriend(userId, friendName);
            friendDao.delFriend(friendId, localName);
            objectNode.put("code", 300);
        }
        return objectNode.toString();
    }


    //添加好友
    private static String addFriends(ObjectNode jsonNodes) {
        int friendId = jsonNodes.get("friendId").asInt();
        int userId = jsonNodes.get("id").asInt();
        String localName = jsonNodes.get("localName").asText();


        //验证是否有ID
        boolean exists = userDao.verifyExistFriend(friendId);
        ObjectNode objectNode = JsonUtils.getObjectNode();
        assert objectNode != null;
        objectNode.put("msgtype", EnMsgType.EN_MSG_ACK.toString());
        objectNode.put("srctype", EnMsgType.EN_MSG_ADD_FRIEND.toString());
        objectNode.put("code", 200);


        if (exists) {
            //表示存在此id
            objectNode.put("code", 300);
            //获取好友昵称
            Information information = informationDao.getInformation(friendId);
            String friendNickname = information.getNickname();
            //进行添加好友的操作   两个对应的信息都应该添加
            friendDao.addFriends(userId, localName, friendNickname);
            friendDao.addFriends(friendId, friendNickname, localName);
        }
        return objectNode.toString();
    }


    //获取好友列表
    private static String getFriend(ObjectNode jsonNodes) {


        int uid = jsonNodes.get("uid").asInt();


        //返回ArrayLis集合
        ArrayList<String> friends = friendDao.getFriends(uid);
        //封装JSON
        ObjectNode objectNode = JsonUtils.getObjectNode();
        assert objectNode != null;
        objectNode.put("msgtype", EnMsgType.EN_MSG_ACK.toString());
        objectNode.put("srctype", EnMsgType.EN_MSG_GET_FRIEND.toString());


        //写回friend集合
        Iterator<String> iterator = friends.iterator();
        int i = 0;
        while (iterator.hasNext()) {
            objectNode.put("res" + i, iterator.next());
            i++;
        }
        //记录好友个数
        objectNode.put("count", i);


        return objectNode.toString();
    }


    //获取id
    private static String getId(ObjectNode jsonNodes) {
        String nickname = jsonNodes.get("nickname").asText();
        Information information = informationDao.nicknameGetId(nickname);
        //联系人的id
        int uid = information.getUid();
        //封装JSON
        ObjectNode objectNode = JsonUtils.getObjectNode();
        assert objectNode != null;
        objectNode.put("msgtype", EnMsgType.EN_MSG_ACK.toString());
        objectNode.put("srctype", EnMsgType.EN_MSG_GET_ID.toString());
        objectNode.put("uid", uid);
        return objectNode.toString();
    }


    //单聊模式
    private static String SingleChat(ObjectNode jsonNodes) {

        int id = jsonNodes.get("id").asInt();

        //根据id在friend表获取登录用户名

        //封装JSON数据服务端转发数据
        ObjectNode objectNode = JsonUtils.getObjectNode();
        assert objectNode != null;
        objectNode.put("msgtype", EnMsgType.EN_MSG_ACK.toString());
        objectNode.put("srctype", EnMsgType.EN_MSG_CHAT.toString());


        //客户端保证用户独立存在且是好友
        Channel channel = CacheUtil.get(id);
        //判断用户是否在线
        if (channel == null) {
            //用户不在线
            objectNode.put("code", 200);
        } else {
            //用户在线
            objectNode.put("code", 300);
            //消息转发
            channel.writeAndFlush(jsonNodes.toString());
        }
        return objectNode.toString();
    }


    //修改密码
    private static String verifyPasswd(ObjectNode jsonNodes) {
        int id = jsonNodes.get("id").asInt();
        String oldPassword = jsonNodes.get("oldPassword").asText();
        String newPassword = jsonNodes.get("newPassword").asText();


        boolean exits = userDao.verifyPassword(oldPassword, id);


        ObjectNode objectNode = JsonUtils.getObjectNode();
        assert objectNode != null;
        objectNode.put("msgtype", EnMsgType.EN_MSG_VERIFY_PASSWORD.toString());
        objectNode.put("code", 200);
        if (exits) {
            //验证成功
            userDao.modifyPassword(newPassword, id);
            objectNode.put("code", 300);
        }
        return objectNode.toString();
    }


    //获取信息
    private static String getInformation(ObjectNode jsonNodes) {
        int id = jsonNodes.get("id").asInt();

        Information information = informationDao.getInformation(id);

        //封装JSON发回客户端
        ObjectNode objectNode = JsonUtils.getObjectNode();
        assert objectNode != null;
        objectNode.put("msgtype", EnMsgType.EN_MSG_ACK.toString());
        objectNode.put("srctype", EnMsgType.EN_MSG_GETINFORMATION.toString());
        objectNode.put("Nickname", information.getNickname());
        objectNode.put("Signature", information.getSignature());


        return objectNode.toString();
    }


    //修改昵称
    private static String modifyNickname(ObjectNode jsonNodes) {
        int id = jsonNodes.get("id").asInt();
        String nickname = jsonNodes.get("nickname").asText();
        //进行存储
        informationDao.storeNickname(nickname, id);
        return "";
    }


    //修改签名
    private static String modifySignature(ObjectNode jsonNodes) {
        int id = jsonNodes.get("id").asInt();
        String signature = jsonNodes.get("signature").asText();
        //进行存储
        informationDao.storeSignature(signature, id);
        return "";
    }


    //登录操作
    private static String loginOperation(ObjectNode objectNode, Channel channel) {


        int id = objectNode.get("id").asInt();
        String password = objectNode.get("password").asText();
        //进行数据库查询
        boolean exits = userDao.getInformation(id, password);


        ObjectNode jsonNodes = JsonUtils.getObjectNode();
        assert jsonNodes != null;
        jsonNodes.put("msgtype", EnMsgType.EN_MSG_ACK.toString());
        jsonNodes.put("srctype", EnMsgType.EN_MSG_LOGIN.toString());
        jsonNodes.put("code", 300);
        //返回状态码
        if (exits) {
            jsonNodes.put("code", 200);

            //添加用户的在线信息
            CacheUtil.put(id, channel);
        }
        return jsonNodes.toString();
    }
}
