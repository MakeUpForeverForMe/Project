package com.weshare.utils;


import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

/**
 * Aes工具类
 */
public class AesPlus {
    private static final Logger logger = Logger.getLogger(AesPlus.class);

    public static final String PASSWORD_TENCENT = "tencentabs123456";// 秘钥
    public static final String PASSWORD_WESHARE = "weshare666";// 秘钥

    private static final String KEY_ALGORITHM = "AES";
    private static final String CIPHER_MODE = KEY_ALGORITHM + "/ECB/PKCS5Padding";
    private static final String CHARACTER = "UTF-8";//默认的加密算法 编码

    /**
     * AES 加密操作
     *
     * @param content  传入明文
     * @param password 传入秘钥
     * @return String  返回密文
     */
    public String encrypt(String content, String password) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_MODE);          // 创建密码器
            byte[] byteContent = content.getBytes(CHARACTER);
            cipher.init(Cipher.ENCRYPT_MODE, getSecretKey(password)); // 初始化为加密模式的密码器
            byte[] result = cipher.doFinal(byteContent);              // 加密
            return Base64.encodeBase64String(result);                 // 通过Base64转码返回
        } catch (Exception e) {
            logger.error("AES 加密异常：", e);
        }
        return null;
    }

    /**
     * AES 解密操作
     *
     * @param content  传入的密文
     * @param password 传入的秘钥
     * @return String  返回明文
     */
    public String decrypt(String content, String password) {
        String string = null;
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_MODE);              // 实例化
            cipher.init(Cipher.DECRYPT_MODE, getSecretKey(password));     // 使用密钥初始化，设置为解密模式
            byte[] result = cipher.doFinal(Base64.decodeBase64(content)); // 执行操作
            string = new String(result, CHARACTER);
        } catch (Exception e) {
            logger.error("AES 解密异常：", e);
        }
        return string;
    }

    /**
     * 生成加密秘钥
     *
     * @param password 输入加密秘钥
     * @return SecretKeySpec 返回加密对象
     */
    private static SecretKeySpec getSecretKey(final String password) {
        SecretKeySpec secretKeySpec = null;
        try {
            KeyGenerator kg = KeyGenerator.getInstance(KEY_ALGORITHM);
            SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
            random.setSeed(password.getBytes());
            kg.init(128, random); // AES 要求密钥长度为 128
            SecretKey secretKey = kg.generateKey(); // 生成密钥
            secretKeySpec = new SecretKeySpec(secretKey.getEncoded(), KEY_ALGORITHM); // 转换为AES专用密钥
        } catch (NoSuchAlgorithmException e) {
            logger.error("AES 生成加密秘钥异常", e);
        }
        return secretKeySpec;
    }
}
